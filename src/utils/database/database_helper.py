import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Column, String, Integer, BigInteger, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
from typing import List, Dict, Optional
import json

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_NAME = os.getenv("DATABASE_NAME")
CHAIN_ID = os.getenv("CHAIN_ID")
UNISWAP_V3_FACTORY_ADDRESSES = ["0x1F98431c8aD98523631AE4a59f267346ea31F984".lower(), "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac".lower(),"0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865".lower()  ]
UNISWAP_V4_FACTORY_ADDRESSES = ["0x000000000004444c5dc75cb358380d2e3de08a90"]

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

Base = declarative_base()

class UniswapV3Pool(Base):
    """Database table for Uniswap V3 pool data"""
    __tablename__ = f"network_{os.getenv("CHAIN_ID")}_dex_pools_cryo"
    
    address = Column(String(42), primary_key=True)
    fee = Column(Integer, nullable=True)
    tick_spacing = Column(Integer, nullable=True)
    asset0 = Column(String(42), nullable=False)
    asset1 = Column(String(42), nullable=False)
    type = Column(String(50), nullable=False)
    creation_block = Column(BigInteger, nullable=False)
    factory = Column(String(42), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

def get_database_engine():
    """Get PostgreSQL database engine"""
    db_uri = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(db_uri)

def setup_uniswap_pools_table(engine):
    """Create the network_{CHAIN_ID}_dex_pools_cryo table if it doesn't exist"""
    chain_id = os.getenv("CHAIN_ID", "1")
    table_name = f"network_{chain_id}_dex_pools_cryo"
    
    with engine.connect() as conn:
        # Check if table exists
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            );
        """), {'table_name': table_name}).scalar()
        
        if table_exists:
            logger.info(f"Table {table_name} exists and is ready for use")
        else:
            logger.info(f"Creating table {table_name}...")
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                address TEXT PRIMARY KEY,
                factory TEXT,
                asset0 TEXT,
                asset1 TEXT,
                asset2 TEXT,
                asset3 TEXT,
                creation_block INTEGER,
                fee INTEGER,
                additional_data JSON,
                priority INTEGER,
                tick_spacing INTEGER
            );
            """
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info(f"Table {table_name} created successfully")
        
        return True

def safe_hex_or_str(val):
    """Safely convert bytes to hex or any value to string, handling None"""
    if val is None:
        return None
    if isinstance(val, bytes):
        return val.hex()
    return str(val)



def store_pools_to_database(pool_data: List[Dict]):
    """Store Uniswap  pool data to the correct network_{CHAIN_ID}_dex_pools_cryo table using bulk insert"""
    if not pool_data:
        logger.info("No pool data to store")
        return
    
    engine = get_database_engine()
    chain_id = os.getenv("CHAIN_ID", "1")
    table_name = f"network_{chain_id}_dex_pools_cryo"
    
    # Check if table exists first
    if not setup_uniswap_pools_table(engine):
        logger.error(f"Cannot store data - table {table_name} does not exist")
        return
    
    # Prepare data for bulk insert
    values = []

    for pool in pool_data:
        if isinstance(pool, dict) and 'address' in pool:
            values.append((
                pool['address'],
                safe_hex_or_str(pool.get('factory')),
                pool.get('asset0'),
                pool.get('asset1'),
                None,  # asset2 (not used in Uniswap V3)
                None,  # asset3 (not used in Uniswap V3)
                pool.get('creation_block'),
                pool.get('fee'),
                json.dumps(pool.get('additional_data', None)),
                None,  # priority (not used in Uniswap V3)
                pool.get('tick_spacing'),
            ))
    
    if not values:
        logger.info("No valid pool data to insert")
        return
    
    # Deduplicate values by address (first element of each tuple) to prevent PostgreSQL conflicts
    seen_addresses = set()
    deduplicated_values = []
    duplicates_removed = 0
    
    for value_tuple in values:
        address = value_tuple[0]  # address is the first element
        if address not in seen_addresses:
            seen_addresses.add(address)
            deduplicated_values.append(value_tuple)
        else:
            duplicates_removed += 1
    
    if duplicates_removed > 0:
        logger.warning(f"Removed {duplicates_removed} duplicate pool addresses from database insert")
    
    values = deduplicated_values
    
    # Use bulk insert with ON CONFLICT for upsert behavior
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        from psycopg2.extras import execute_values
        
        execute_values(
            cursor,
            f"""
            INSERT INTO {table_name} 
            (address, factory, asset0, asset1, asset2, asset3, creation_block, fee, additional_data, priority, tick_spacing)
            VALUES %s
            ON CONFLICT (address) DO UPDATE SET
                factory = EXCLUDED.factory,
                asset0 = EXCLUDED.asset0,
                asset1 = EXCLUDED.asset1,
                asset2 = EXCLUDED.asset2,
                asset3 = EXCLUDED.asset3,
                creation_block = EXCLUDED.creation_block,
                fee = EXCLUDED.fee,
                additional_data = EXCLUDED.additional_data,
                priority = EXCLUDED.priority,
                tick_spacing = EXCLUDED.tick_spacing
            """,
            values,
            template=None,
            page_size=1000
        )
        conn.commit()
        cursor.close()
        logger.info(f"Successfully stored {len(values)} pools to {table_name}")
    finally:
        conn.close()

def check_uniswap_database_results(LP_TYPE: str):
    """Check the results in the database"""
    engine = get_database_engine()
    chain_id = os.getenv("CHAIN_ID", "1")
    table_name = f"network_{chain_id}_dex_pools_cryo"

    
    print(f"=== {LP_TYPE} Database Results ===")
    
    # Check total records
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) as total_pools FROM {table_name}"))
        total_pools = result.scalar()
        print(f"Total pools in database: {total_pools}")
    
    # Check sample data
    print("\n=== Sample Pool Data ===")
    
    with engine.connect() as conn:

        V3_sql_query = f"""
            SELECT address, fee, asset0, asset1, creation_block, factory, 'V3' as version
            FROM {table_name} 
            WHERE factory IN ({','.join([f"'{addr}'" for addr in UNISWAP_V3_FACTORY_ADDRESSES])})
            ORDER BY creation_block DESC
            LIMIT 5;"""
            
        V4_sql_query = f"""SELECT address, fee, asset0, asset1, creation_block, factory, additional_data, 'V4' as version
            FROM {table_name} 
            WHERE factory IN ({','.join([f"'{addr}'" for addr in UNISWAP_V4_FACTORY_ADDRESSES])})
            ORDER BY creation_block DESC
            LIMIT 5;
        """

        if LP_TYPE == "UniswapV3":
            result = conn.execute(text(V3_sql_query))
        elif LP_TYPE == "UniswapV4":
            result = conn.execute(text(V4_sql_query))
        else:
            print(f"Unknown protocol: {LP_TYPE}")
            return

        for row in result:
            print(f"  Pool: {row.address}")
            print(f"    Fee: {row.fee}, Assets: {row.asset0} / {row.asset1}")
            print(f"    Block: {row.creation_block}, Factory: {row.factory}")
    
    print("\n=== Database check completed ===")



def get_pools_by_token(token_address: str) -> List[Dict]:
    """Get pools that contain a specific token"""
    engine = get_database_engine()
    chain_id = os.getenv("CHAIN_ID", "1")
    table_name = f"network_{chain_id}_dex_pools_cryo"
    
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT address, fee, asset0, asset1, creation_block, factory
            FROM {table_name} 
            WHERE asset0 = :token_address OR asset1 = :token_address
            ORDER BY creation_block DESC
        """), {'token_address': token_address})
        
        pools = []
        for row in result:
            pools.append({
                'address': row.address,
                'fee': row.fee,
                'asset0': row.asset0,
                'asset1': row.asset1,
                'creation_block': row.creation_block,
                'factory': row.factory
            })
        
        return pools


def inspect_existing_table_schema():
    """Inspect the existing table schema to understand its structure"""
    engine = get_database_engine()
    chain_id = os.getenv("CHAIN_ID", "1")  #
    table_name = f"network_{chain_id}__dex_pools"
    
    print(f"=== Inspecting table: {table_name} ===")
    
    with engine.connect() as conn:
        # Check if table exists
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            );
        """), {'table_name': table_name}).scalar()
        
        if not table_exists:
            print(f"Table {table_name} does not exist")
            return None
        
        # Get table schema
        result = conn.execute(text("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable, 
                character_maximum_length,
                column_default,
                ordinal_position
            FROM information_schema.columns 
            WHERE table_name = :table_name
            ORDER BY ordinal_position
        """), {'table_name': table_name})
        
        columns = []
        print(f"\nTable schema for {table_name}:")
        for row in result:
            nullable = "NULL" if row.is_nullable == "YES" else "NOT NULL"
            length_info = f"({row.character_maximum_length})" if row.character_maximum_length else ""
            default_info = f" DEFAULT {row.column_default}" if row.column_default else ""
            print(f"  {row.column_name}: {row.data_type}{length_info} {nullable}{default_info}")
            columns.append({
                'name': row.column_name,
                'type': row.data_type,
                'nullable': row.is_nullable == "YES",
                'length': row.character_maximum_length,
                'default': row.column_default
            })
        
        # Get sample data
        print(f"\nSample data from {table_name}:")
        sample_result = conn.execute(text(f"""
            SELECT * FROM {table_name} LIMIT 3
        """))
        
        for row in sample_result:
            print(f"  {dict(row)}")
        
        return columns

def remove_pools_from_database(pool_data: List[Dict]):
    """Remove pools from the database"""
    engine = get_database_engine()
    chain_id = os.getenv("CHAIN_ID", "1")
    table_name = f"network_{chain_id}_dex_pools_cryo"
    
    with engine.connect() as conn:
        for pool in pool_data:
            conn.execute(text(f"DELETE FROM {table_name} WHERE address = :address"), {'address': pool['address']})
    
    print(f"Successfully removed {len(pool_data)} pools from {table_name}")
    

if __name__ == "__main__":
    check_uniswap_database_results("UniswapV3")
