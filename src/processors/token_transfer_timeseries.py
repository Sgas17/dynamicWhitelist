import os
import sqlalchemy as sa
from sqlalchemy import create_engine, text, Column, String, Integer, BigInteger, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

Base = declarative_base()

class TokenRawTransfers(Base):
    """TimescaleDB table for 5-minute raw token transfer data"""
    __tablename__ = 'token_raw_transfers'
    
    # TimescaleDB hypertable requires a time column
    timestamp = Column(DateTime, primary_key=True)
    token_address = Column(String(42), primary_key=True)
    transfer_count = Column(Integer, nullable=False)
    unique_senders = Column(Integer, nullable=False)
    unique_receivers = Column(Integer, nullable=False)
    total_volume = Column(sa.Numeric(precision=78, scale=0), nullable=False, default=0)

class TokenHourlyTransfers(Base):
    """TimescaleDB table for hourly aggregated token transfer data"""
    __tablename__ = 'token_hourly_transfers'
    
    # TimescaleDB hypertable requires a time column
    hour_timestamp = Column(DateTime, primary_key=True)
    token_address = Column(String(42), primary_key=True)
    transfer_count = Column(Integer, nullable=False)
    unique_senders = Column(Integer, nullable=False)
    unique_receivers = Column(Integer, nullable=False)
    total_volume = Column(sa.Numeric(precision=78, scale=0), nullable=False, default=0)
    
    # Computed fields for averages - nullable so we can handle new vs existing tokens properly
    avg_transfers_24h = Column(Float, nullable=True)
    avg_transfers_7d = Column(Float, nullable=True)
    avg_transfers_30d = Column(Float, nullable=True)

def get_timescale_engine():
    """Get TimescaleDB engine"""
    db_uri = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(db_uri)

def migrate_table_schema(engine):
    """Safely migrate table schema without losing data"""
    with engine.connect() as conn:
        try:
            # Check if total_volume needs migration
            column_type = conn.execute(text("""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_name = 'token_hourly_transfers' 
                AND column_name = 'total_volume';
            """)).scalar()
            
            if column_type == 'bigint':
                logger.info("Migrating total_volume from BIGINT to NUMERIC...")
                conn.execute(text("""
                    ALTER TABLE token_hourly_transfers 
                    ALTER COLUMN total_volume TYPE NUMERIC(78,0) 
                    USING total_volume::NUMERIC(78,0);
                """))
                logger.info("Migration completed successfully")
            
            
            
            # Check if average columns need to be made nullable
            avg_columns = ['avg_transfers_24h', 'avg_transfers_7d', 'avg_transfers_30d']
            for column in avg_columns:
                is_nullable = conn.execute(text("""
                    SELECT is_nullable 
                    FROM information_schema.columns 
                    WHERE table_name = 'token_hourly_transfers' 
                    AND column_name = :column;
                """), {'column': column}).scalar()
                
                if is_nullable == 'NO':
                    logger.info(f"Making {column} nullable...")
                    conn.execute(text(f"""
                        ALTER TABLE token_hourly_transfers 
                        ALTER COLUMN {column} DROP NOT NULL;
                    """))
                    logger.info(f"{column} migration completed successfully")
            
            logger.info("Schema is up to date")
                
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise

def setup_timescale_tables(engine):
    """Create TimescaleDB hypertables for time-series data with optimized settings"""
    with engine.connect() as conn:
        # Check if tables exist
        hourly_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'token_hourly_transfers'
            );
        """)).scalar()
        
        raw_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'token_raw_transfers'
            );
        """)).scalar()
        
        # Create tables if they don't exist
        if not hourly_exists or not raw_exists:
            logger.info("Creating new tables with correct schema...")
            Base.metadata.create_all(engine)
        
        if hourly_exists:
            # Always run migration check for schema updates
            logger.info("Checking for schema migrations...")
            migrate_table_schema(engine)
        
        # Setup raw transfers hypertable (5-minute chunks, short retention)
        try:
            if not raw_exists:
                # Create hypertable with 5-minute chunk interval for raw data
                conn.execute(text("""
                    SELECT create_hypertable('token_raw_transfers', 'timestamp', 
                                           chunk_time_interval => INTERVAL '5 minutes',
                                           if_not_exists => TRUE);
                """))
                logger.info("Raw transfers hypertable created with 5-minute chunks")
                
                # Set up compression policy (compress chunks older than 1 hour)
                conn.execute(text("""
                    SELECT add_compression_policy('token_raw_transfers', INTERVAL '1 hour');
                """))
                logger.info("Raw transfers compression policy added (compress after 1 hour)")
                
                # Set up retention policy (keep raw data for 7 days)
                conn.execute(text("""
                    SELECT add_retention_policy('token_raw_transfers', INTERVAL '7 days');
                """))
                logger.info("Raw transfers retention policy added (keep for 7 days)")
        except Exception as e:
            logger.info(f"Raw transfers hypertable setup: {e}")
            if "already exists" not in str(e):
                raise
        
        # Setup hourly transfers hypertable (1-hour chunks, longer retention)
        try:
            # Create hypertable with 1-hour chunk interval for hourly data
            conn.execute(text("""
                SELECT create_hypertable('token_hourly_transfers', 'hour_timestamp', 
                                       chunk_time_interval => INTERVAL '1 hour',
                                       migrate_data => TRUE,
                                       if_not_exists => TRUE);
            """))
            logger.info("Hourly transfers hypertable created with 1-hour chunks")
            
            # Enable compression if not already enabled
            conn.execute(text("""
                ALTER TABLE token_hourly_transfers SET (timescaledb.compress, timescaledb.compress_segmentby = 'token_address');
            """))
            logger.info("Hourly transfers compression enabled")
            
            # Set up compression policy (compress chunks older than 1 day)
            conn.execute(text("""
                SELECT add_compression_policy('token_hourly_transfers', INTERVAL '1 day');
            """))
            logger.info("Hourly transfers compression policy added (compress after 1 day)")
            
            # Set up retention policy (keep hourly data for 90 days)
            conn.execute(text("""
                SELECT add_retention_policy('token_hourly_transfers', INTERVAL '90 days');
            """))
            logger.info("Hourly transfers retention policy added (keep for 90 days)")
            
        except Exception as e:
            logger.info(f"Hourly transfers hypertable setup: {e}")
            # If policies already exist, that's fine
            if "already exists" not in str(e):
                raise

def store_raw_transfers(transfers_data: List[Dict], timestamp: datetime):
    """Store raw 5-minute transfer data in TimescaleDB"""
    engine = get_timescale_engine()
    
    if not transfers_data:
        logger.info("No raw transfer data to store")
        return
    
    try:
        # Prepare data for bulk insert
        values = []
        for transfer in transfers_data:
            values.append((
                timestamp,
                transfer['token_address'],
                transfer['transfer_count'],
                transfer.get('unique_senders', 0),
                transfer.get('unique_receivers', 0),
                transfer.get('total_volume', 0)
            ))
        
        # Use bulk insert for raw data (no upsert needed for 5-minute intervals)
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            execute_values(
                cursor,
                """
                INSERT INTO token_raw_transfers 
                (timestamp, token_address, transfer_count, unique_senders, unique_receivers, total_volume)
                VALUES %s
                """,
                values,
                template=None,
                page_size=1000
            )
            conn.commit()
            cursor.close()
        finally:
            conn.close()
        
        logger.info(f"Bulk inserted {len(transfers_data)} raw transfer records for {timestamp}")
        
    except Exception as e:
        logger.error(f"Error storing raw transfers: {e}")
        raise

def aggregate_raw_to_hourly(current_hour: datetime):
    """Aggregate raw 5-minute data into hourly data"""
    engine = get_timescale_engine()
    
    # Calculate the start and end of the hour
    hour_start = current_hour.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    
    try:
        with engine.connect() as conn:
            # Aggregate raw data for the hour
            result = conn.execute(text("""
                SELECT 
                    token_address,
                    SUM(transfer_count) as transfer_count,
                    SUM(unique_senders) as unique_senders,
                    SUM(unique_receivers) as unique_receivers,
                    SUM(total_volume) as total_volume
                FROM token_raw_transfers
                WHERE timestamp >= :hour_start AND timestamp < :hour_end
                GROUP BY token_address
            """), {
                'hour_start': hour_start,
                'hour_end': hour_end
            })
            
            aggregated_data = []
            for row in result:
                aggregated_data.append({
                    'token_address': row.token_address,
                    'transfer_count': row.transfer_count,
                    'unique_senders': row.unique_senders,
                    'unique_receivers': row.unique_receivers,
                    'total_volume': row.total_volume
                })
            
            if aggregated_data:
                # Store aggregated data in hourly table
                store_hourly_transfers(aggregated_data, hour_start)
                
                # Update averages
                update_token_averages(hour_start)
                
                logger.info(f"Aggregated {len(aggregated_data)} tokens from raw data to hourly for {hour_start}")
            else:
                logger.info(f"No raw data found to aggregate for {hour_start}")
                
    except Exception as e:
        logger.error(f"Error aggregating raw to hourly: {e}")
        raise

def store_hourly_transfers(transfers_data: List[Dict], hour_timestamp: datetime):
    """Store hourly transfer data in TimescaleDB using fast bulk upsert"""
    engine = get_timescale_engine()
    
    if not transfers_data:
        logger.info("No transfer data to store")
        return
    
    try:
        # Prepare data for bulk insert - only core data, averages will be set by update_token_averages
        values = []
        for transfer in transfers_data:
            values.append((
                hour_timestamp,
                transfer['token_address'],
                transfer['transfer_count'],
                transfer.get('unique_senders', 0),
                transfer.get('unique_receivers', 0),
                transfer.get('total_volume', 0)
            ))
        
        # Use bulk upsert with ON CONFLICT for maximum performance
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            execute_values(
                cursor,
                """
                INSERT INTO token_hourly_transfers 
                (hour_timestamp, token_address, transfer_count, unique_senders, unique_receivers, total_volume)
                VALUES %s
                ON CONFLICT (hour_timestamp, token_address) DO UPDATE SET
                    transfer_count = EXCLUDED.transfer_count,
                    unique_senders = EXCLUDED.unique_senders,
                    unique_receivers = EXCLUDED.unique_receivers,
                    total_volume = EXCLUDED.total_volume
                """,
                values,
                template=None,
                page_size=1000
            )
            conn.commit()
            cursor.close()
        finally:
            conn.close()
        
        logger.info(f"Bulk upserted {len(transfers_data)} token transfer records for {hour_timestamp}")
        
    except Exception as e:
        logger.error(f"Error storing hourly transfers: {e}")
        raise

def calculate_token_averages(token_address: str, current_hour: datetime) -> Dict[str, float]:
    """Calculate rolling averages for a token"""
    engine = get_timescale_engine()
    
    with engine.connect() as conn:
        # Calculate 24h average
        avg_24h = conn.execute(text("""
            SELECT AVG(transfer_count) as avg_24h
            FROM token_hourly_transfers 
            WHERE token_address = :token_address 
            AND hour_timestamp >= :cutoff_24h
        """), {
            'token_address': token_address,
            'cutoff_24h': current_hour - timedelta(hours=24)
        }).scalar() or 0.0
        
        # Calculate 7d average
        avg_7d = conn.execute(text("""
            SELECT AVG(transfer_count) as avg_7d
            FROM token_hourly_transfers 
            WHERE token_address = :token_address 
            AND hour_timestamp >= :cutoff_7d
        """), {
            'token_address': token_address,
            'cutoff_7d': current_hour - timedelta(days=7)
        }).scalar() or 0.0
        
        # Calculate 30d average
        avg_30d = conn.execute(text("""
            SELECT AVG(transfer_count) as avg_30d
            FROM token_hourly_transfers 
            WHERE token_address = :token_address 
            AND hour_timestamp >= :cutoff_30d
        """), {
            'token_address': token_address,
            'cutoff_30d': current_hour - timedelta(days=30)
        }).scalar() or 0.0
        
        return {
            'avg_24h': float(avg_24h),
            'avg_7d': float(avg_7d),
            'avg_30d': float(avg_30d)
        }

def get_top_tokens_by_average(limit: int = 100, period: str = '24h') -> List[Dict]:
    """Get top tokens by average transfer count over specified period"""
    engine = get_timescale_engine()
    
    # Map period to column name
    period_map = {
        '24h': 'avg_transfers_24h',
        '7d': 'avg_transfers_7d', 
        '30d': 'avg_transfers_30d'
    }
    
    avg_column = period_map.get(period, 'avg_transfers_24h')
    
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                token_address,
                transfer_count,
                {avg_column} as avg_transfers,
                hour_timestamp
            FROM token_hourly_transfers 
            WHERE hour_timestamp = (
                SELECT MAX(hour_timestamp) FROM token_hourly_transfers
            )
            ORDER BY {avg_column} DESC
            LIMIT :limit
        """), {'limit': limit})
        
        return [
            {
                'token_address': row.token_address,
                'transfer_count': row.transfer_count,
                'avg_transfers': float(row.avg_transfers),
                'hour_timestamp': row.hour_timestamp
            }
            for row in result
        ]

def update_token_averages(current_hour: datetime):
    """Efficiently update average columns for all tokens in a single batch."""
    engine = get_timescale_engine()
    cutoff_24h = current_hour - timedelta(hours=24)
    cutoff_7d = current_hour - timedelta(days=7)
    cutoff_30d = current_hour - timedelta(days=30)

    with engine.begin() as conn:
        # First, set averages for new tokens (no historical data) to their current transfer_count
        conn.execute(text("""
            UPDATE token_hourly_transfers 
            SET 
                avg_transfers_24h = transfer_count,
                avg_transfers_7d = transfer_count,
                avg_transfers_30d = transfer_count
            WHERE hour_timestamp = :current_hour 
            AND avg_transfers_24h IS NULL
        """), {'current_hour': current_hour})
        
        # Then calculate proper averages for tokens with historical data
        conn.execute(text("""
            WITH avg_data AS (
                SELECT
                    token_address,
                    AVG(transfer_count) FILTER (WHERE hour_timestamp >= :cutoff_24h) AS avg_24h,
                    AVG(transfer_count) FILTER (WHERE hour_timestamp >= :cutoff_7d) AS avg_7d,
                    AVG(transfer_count) FILTER (WHERE hour_timestamp >= :cutoff_30d) AS avg_30d
                FROM token_hourly_transfers
                WHERE hour_timestamp >= :cutoff_30d
                GROUP BY token_address
                HAVING COUNT(*) > 1  -- Only tokens with multiple data points
            )
            UPDATE token_hourly_transfers AS t
            SET
                avg_transfers_24h = a.avg_24h,
                avg_transfers_7d = a.avg_7d,
                avg_transfers_30d = a.avg_30d
            FROM avg_data AS a
            WHERE t.token_address = a.token_address
              AND t.hour_timestamp = :current_hour
        """), {
            'cutoff_24h': cutoff_24h,
            'cutoff_7d': cutoff_7d,
            'cutoff_30d': cutoff_30d,
            'current_hour': current_hour
        })
        logger.info(f"Updated averages for all tokens in batch for {current_hour}")

def cleanup_old_data(retention_days: int = 90):
    """Clean up old data beyond retention period"""
    engine = get_timescale_engine()
    
    cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            DELETE FROM token_hourly_transfers 
            WHERE hour_timestamp < :cutoff_date
        """), {'cutoff_date': cutoff_date})
        
        logger.info(f"Cleaned up {result.rowcount} old records") 