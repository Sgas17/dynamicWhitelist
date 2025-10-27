"""
TimescaleDB Policy Manager for production environments.
Handles compression, retention, and continuous aggregation policies with proper error handling.
"""

from typing import Dict, List, Optional, Any
from sqlalchemy import text, Engine
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TimescalePolicyManager:
    """
    Manages TimescaleDB policies with production-grade error handling and monitoring.
    """
    
    def __init__(self, engine: Engine):
        self.engine = engine
        
    def check_policy_exists(self, table_name: str, policy_type: str) -> bool:
        """
        Check if a specific policy already exists for a table.
        
        Args:
            table_name: Name of the hypertable
            policy_type: Type of policy ('compression', 'retention', 'refresh')
            
        Returns:
            True if policy exists, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM timescaledb_information.jobs j
                    JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
                    WHERE j.application_name LIKE :table_pattern
                    AND j.proc_name LIKE :policy_pattern
                """), {
                    "table_pattern": f"%{table_name}%",
                    "policy_pattern": f"%{policy_type}%"
                })
                
                return result.scalar() > 0
                
        except Exception as e:
            logger.warning(f"Could not check policy existence for {table_name}: {e}")
            return False
    
    def setup_compression_policy(
        self, 
        table_name: str, 
        compress_after: str = "2 hours",
        segment_by: Optional[List[str]] = None,
        order_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Setup compression policy for a hypertable with proper error handling.
        
        Args:
            table_name: Name of the hypertable
            compress_after: Interval after which to compress (e.g., "2 hours", "1 day")
            segment_by: Columns to segment by during compression
            order_by: Column to order by during compression
            
        Returns:
            Result dictionary with success status and details
        """
        
        result = {"success": False, "action": "none", "message": ""}
        
        try:
            # Step 1: Enable compression on the table
            with self.engine.begin() as conn:
                # Build compression settings
                settings = ["timescaledb.compress"]
                
                if segment_by:
                    segment_columns = "', '".join(segment_by)
                    settings.append(f"timescaledb.compress_segmentby = '{segment_columns}'")
                    
                if order_by:
                    settings.append(f"timescaledb.compress_orderby = '{order_by}'")
                
                settings_sql = ", ".join(settings)
                
                # Enable compression
                conn.execute(text(f"""
                    ALTER TABLE {table_name} SET ({settings_sql})
                """))
                
                logger.info(f"Compression enabled for {table_name}")
                
            # Step 2: Add compression policy
            if not self.check_policy_exists(table_name, "compression"):
                with self.engine.begin() as conn:
                    conn.execute(text(f"""
                        SELECT add_compression_policy('{table_name}', INTERVAL '{compress_after}')
                    """))
                    
                    result.update({
                        "success": True,
                        "action": "created",
                        "message": f"Compression policy created (compress after {compress_after})"
                    })
                    logger.info(f"Compression policy added for {table_name} (compress after {compress_after})")
            else:
                result.update({
                    "success": True,
                    "action": "existed",
                    "message": "Compression policy already exists"
                })
                logger.info(f"Compression policy already exists for {table_name}")
                
        except Exception as e:
            error_msg = str(e).lower()
            if any(phrase in error_msg for phrase in ["already enabled", "already set"]):
                result.update({
                    "success": True,
                    "action": "existed", 
                    "message": "Compression already configured"
                })
                logger.info(f"Compression already configured for {table_name}")
            else:
                result.update({
                    "success": False,
                    "action": "failed",
                    "message": f"Failed to setup compression: {e}"
                })
                logger.error(f"Compression setup failed for {table_name}: {e}")
        
        return result
    
    def setup_retention_policy(
        self,
        table_name: str,
        retain_for: str = "30 days"
    ) -> Dict[str, Any]:
        """
        Setup retention policy for a hypertable.
        
        Args:
            table_name: Name of the hypertable
            retain_for: How long to retain data (e.g., "30 days", "1 year")
            
        Returns:
            Result dictionary with success status and details
        """
        
        result = {"success": False, "action": "none", "message": ""}
        
        try:
            if not self.check_policy_exists(table_name, "retention"):
                with self.engine.begin() as conn:
                    conn.execute(text(f"""
                        SELECT add_retention_policy('{table_name}', INTERVAL '{retain_for}')
                    """))
                    
                    result.update({
                        "success": True,
                        "action": "created",
                        "message": f"Retention policy created (retain for {retain_for})"
                    })
                    logger.info(f"Retention policy added for {table_name} (retain for {retain_for})")
            else:
                result.update({
                    "success": True,
                    "action": "existed",
                    "message": "Retention policy already exists"
                })
                logger.info(f"Retention policy already exists for {table_name}")
                
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg:
                result.update({
                    "success": True,
                    "action": "existed",
                    "message": "Retention policy already exists"
                })
                logger.info(f"Retention policy already exists for {table_name}")
            else:
                result.update({
                    "success": False,
                    "action": "failed", 
                    "message": f"Failed to setup retention: {e}"
                })
                logger.error(f"Retention setup failed for {table_name}: {e}")
        
        return result
    
    def get_policy_status(self) -> Dict[str, List[Dict]]:
        """
        Get status of all TimescaleDB policies.
        
        Returns:
            Dictionary with policy status information
        """
        
        status = {
            "compression_policies": [],
            "retention_policies": [],
            "refresh_policies": []
        }
        
        try:
            with self.engine.connect() as conn:
                # Get all policies
                result = conn.execute(text("""
                    SELECT 
                        j.application_name,
                        j.proc_name,
                        j.schedule_interval,
                        j.max_runtime,
                        j.max_retries,
                        js.last_run_started_at,
                        js.last_successful_finish,
                        js.consecutive_failures,
                        js.total_runs,
                        js.total_successes
                    FROM timescaledb_information.jobs j
                    LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
                    WHERE j.proc_name IN ('policy_compression', 'policy_retention', 'policy_refresh_continuous_aggregate')
                    ORDER BY j.application_name
                """))
                
                for row in result:
                    policy_info = {
                        "table": row.application_name,
                        "schedule_interval": str(row.schedule_interval) if row.schedule_interval else None,
                        "last_run": row.last_run_started_at,
                        "last_success": row.last_successful_finish,
                        "failures": row.consecutive_failures or 0,
                        "total_runs": row.total_runs or 0,
                        "success_rate": (row.total_successes or 0) / max(row.total_runs or 1, 1) * 100
                    }
                    
                    if row.proc_name == "policy_compression":
                        status["compression_policies"].append(policy_info)
                    elif row.proc_name == "policy_retention":
                        status["retention_policies"].append(policy_info)
                    elif row.proc_name == "policy_refresh_continuous_aggregate":
                        status["refresh_policies"].append(policy_info)
                        
        except Exception as e:
            logger.error(f"Failed to get policy status: {e}")
            
        return status
    
    def setup_production_policies(self) -> Dict[str, Any]:
        """
        Setup all production policies for transfer data tables.
        
        Returns:
            Comprehensive results of policy setup
        """
        
        logger.info("🔧 Setting up production TimescaleDB policies...")
        
        results = {
            "raw_transfers": {},
            "hourly_transfers": {},
            "overall_success": True
        }
        
        # Raw transfers table policies
        logger.info("📊 Configuring raw transfers policies...")
        
        # Compression: compress after 2 hours, segment by token_address, order by timestamp DESC
        raw_compression = self.setup_compression_policy(
            table_name="token_raw_transfers",
            compress_after="2 hours", 
            segment_by=["token_address"],
            order_by="timestamp DESC"
        )
        results["raw_transfers"]["compression"] = raw_compression
        
        # Retention: keep raw data for 5 days
        raw_retention = self.setup_retention_policy(
            table_name="token_raw_transfers",
            retain_for="5 days"
        )
        results["raw_transfers"]["retention"] = raw_retention
        
        # Hourly transfers table policies
        logger.info("📊 Configuring hourly transfers policies...")
        
        # Compression: compress after 1 day, segment by token_address, order by hour_timestamp DESC  
        hourly_compression = self.setup_compression_policy(
            table_name="token_hourly_transfers",
            compress_after="1 day",
            segment_by=["token_address"], 
            order_by="hour_timestamp DESC"
        )
        results["hourly_transfers"]["compression"] = hourly_compression
        
        # Retention: keep hourly data for 90 days
        hourly_retention = self.setup_retention_policy(
            table_name="token_hourly_transfers", 
            retain_for="90 days"
        )
        results["hourly_transfers"]["retention"] = hourly_retention
        
        # Check overall success
        all_policies = [raw_compression, raw_retention, hourly_compression, hourly_retention]
        results["overall_success"] = all(policy["success"] for policy in all_policies)
        
        # Log summary
        successful_policies = sum(1 for policy in all_policies if policy["success"])
        logger.info(f"✅ Policy setup complete: {successful_policies}/4 policies successful")
        
        return results

# Convenience function for integration
def setup_timescale_policies_production(engine: Engine) -> Dict[str, Any]:
    """
    Convenience function to setup all TimescaleDB policies in production.
    
    Args:
        engine: SQLAlchemy engine connected to TimescaleDB
        
    Returns:
        Results of policy setup
    """
    
    policy_manager = TimescalePolicyManager(engine)
    return policy_manager.setup_production_policies()