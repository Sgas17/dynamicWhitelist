"""
Production-ready TimescaleDB setup with proper error handling and transaction management.
"""

from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def setup_compression_policies_production(engine):
    """
    Setup compression policies with proper transaction isolation.
    Each policy is executed in its own transaction to prevent rollback cascades.
    """
    
    policies = [
        {
            "name": "raw_transfers_compression",
            "sql": """
                ALTER TABLE token_raw_transfers SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'token_address',
                    timescaledb.compress_orderby = 'timestamp DESC'
                )
            """,
            "success_msg": "Raw transfers compression enabled"
        },
        {
            "name": "hourly_transfers_compression", 
            "sql": """
                ALTER TABLE token_hourly_transfers SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'token_address',
                    timescaledb.compress_orderby = 'hour_timestamp DESC'
                )
            """,
            "success_msg": "Hourly transfers compression enabled"
        },
        {
            "name": "raw_compression_policy",
            "sql": """
                SELECT add_compression_policy('token_raw_transfers', INTERVAL '2 hours')
            """,
            "success_msg": "Raw transfers compression policy added (compress after 2 hours)"
        },
        {
            "name": "hourly_compression_policy", 
            "sql": """
                SELECT add_compression_policy('token_hourly_transfers', INTERVAL '1 day')
            """,
            "success_msg": "Hourly transfers compression policy added (compress after 1 day)"
        },
        {
            "name": "raw_retention_policy",
            "sql": """
                SELECT add_retention_policy('token_raw_transfers', INTERVAL '5 days')
            """,
            "success_msg": "Raw transfers retention policy added (keep for 5 days)"
        },
        {
            "name": "hourly_retention_policy",
            "sql": """
                SELECT add_retention_policy('token_hourly_transfers', INTERVAL '90 days') 
            """,
            "success_msg": "Hourly transfers retention policy added (keep for 90 days)"
        }
    ]
    
    results = {"successful": [], "failed": [], "skipped": []}
    
    for policy in policies:
        # Each policy gets its own connection and transaction
        try:
            with engine.begin() as conn:  # Auto-commit transaction
                conn.execute(text(policy["sql"]))
                logger.info(policy["success_msg"])
                results["successful"].append(policy["name"])
                
        except Exception as e:
            error_msg = str(e).lower()
            
            # Check if policy already exists (expected in production restarts)
            if any(phrase in error_msg for phrase in [
                "already set", "already enabled", "already exists", 
                "policy already exists", "compression already enabled"
            ]):
                logger.info(f"{policy['name']} already configured")
                results["skipped"].append(policy["name"])
            else:
                logger.error(f"{policy['name']} failed: {e}")
                results["failed"].append({"name": policy["name"], "error": str(e)})
    
    return results

def setup_hypertables_with_validation(engine):
    """
    Setup hypertables with proper validation and error handling.
    """
    
    hypertables = [
        {
            "name": "token_raw_transfers",
            "time_column": "timestamp",
            "chunk_interval": "5 minutes",
            "sql": """
                SELECT create_hypertable('token_raw_transfers', 'timestamp', 
                                       chunk_time_interval => INTERVAL '5 minutes',
                                       if_not_exists => TRUE)
            """
        },
        {
            "name": "token_hourly_transfers", 
            "time_column": "hour_timestamp",
            "chunk_interval": "1 hour",
            "sql": """
                SELECT create_hypertable('token_hourly_transfers', 'hour_timestamp',
                                       chunk_time_interval => INTERVAL '1 hour', 
                                       if_not_exists => TRUE)
            """
        }
    ]
    
    results = {"successful": [], "failed": []}
    
    for table in hypertables:
        try:
            with engine.begin() as conn:
                result = conn.execute(text(table["sql"]))
                logger.info(f"{table['name']} hypertable created with {table['chunk_interval']} chunks")
                results["successful"].append(table["name"])
                
        except Exception as e:
            if "already a hypertable" in str(e).lower():
                logger.info(f"{table['name']} already configured as hypertable")
                results["successful"].append(table["name"])
            else:
                logger.error(f"{table['name']} hypertable creation failed: {e}")
                results["failed"].append({"name": table["name"], "error": str(e)})
    
    return results

def validate_timescale_configuration(engine):
    """
    Validate that TimescaleDB configuration is working properly.
    """
    
    checks = []
    
    try:
        with engine.connect() as conn:
            # Check TimescaleDB extension
            result = conn.execute(text("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'"))
            if result.fetchone():
                checks.append({"check": "TimescaleDB extension", "status": "✅ Installed"})
            else:
                checks.append({"check": "TimescaleDB extension", "status": "❌ Missing"})
            
            # Check hypertables
            hypertable_result = conn.execute(text("""
                SELECT hypertable_name, num_chunks 
                FROM timescaledb_information.hypertables 
                WHERE hypertable_name IN ('token_raw_transfers', 'token_hourly_transfers')
            """))
            
            hypertables_found = list(hypertable_result)
            if len(hypertables_found) >= 2:
                checks.append({"check": "Hypertables", "status": f"✅ {len(hypertables_found)} configured"})
                for ht in hypertables_found:
                    checks.append({"check": f"  - {ht.hypertable_name}", "status": f"{ht.num_chunks} chunks"})
            else:
                checks.append({"check": "Hypertables", "status": f"⚠️ Only {len(hypertables_found)} found"})
            
            # Check compression policies
            try:
                compression_result = conn.execute(text("""
                    SELECT application_name, schedule_interval, proc_name
                    FROM timescaledb_information.jobs 
                    WHERE proc_name = 'policy_compression'
                """))
                compression_jobs = list(compression_result)
                checks.append({"check": "Compression jobs", "status": f"✅ {len(compression_jobs)} active"})
            except Exception:
                checks.append({"check": "Compression jobs", "status": "⚠️ Cannot verify"})
            
            # Check retention policies
            try:
                retention_result = conn.execute(text("""
                    SELECT application_name, schedule_interval, proc_name
                    FROM timescaledb_information.jobs 
                    WHERE proc_name = 'policy_retention'
                """))
                retention_jobs = list(retention_result)
                checks.append({"check": "Retention jobs", "status": f"✅ {len(retention_jobs)} active"})
            except Exception:
                checks.append({"check": "Retention jobs", "status": "⚠️ Cannot verify"})
                
    except Exception as e:
        checks.append({"check": "Database connection", "status": f"❌ Failed: {e}"})
    
    return checks

# Production deployment function
def deploy_timescale_production(engine):
    """
    Complete production deployment of TimescaleDB configuration.
    """
    
    logger.info("🚀 Starting production TimescaleDB deployment...")
    
    # Step 1: Setup hypertables
    logger.info("📊 Setting up hypertables...")
    hypertable_results = setup_hypertables_with_validation(engine)
    
    # Step 2: Setup compression and retention policies  
    logger.info("🗜️ Configuring compression and retention policies...")
    policy_results = setup_compression_policies_production(engine)
    
    # Step 3: Validate configuration
    logger.info("✅ Validating configuration...")
    validation_results = validate_timescale_configuration(engine)
    
    # Step 4: Report results
    logger.info("📋 Deployment Summary:")
    logger.info(f"  Hypertables: {len(hypertable_results['successful'])} successful, {len(hypertable_results['failed'])} failed")
    logger.info(f"  Policies: {len(policy_results['successful'])} successful, {len(policy_results['failed'])} failed, {len(policy_results['skipped'])} skipped")
    
    logger.info("🔍 System Status:")
    for check in validation_results:
        logger.info(f"  {check['check']}: {check['status']}")
    
    # Return comprehensive results
    return {
        "hypertables": hypertable_results,
        "policies": policy_results, 
        "validation": validation_results,
        "overall_success": len(hypertable_results["failed"]) == 0 and len(policy_results["failed"]) == 0
    }