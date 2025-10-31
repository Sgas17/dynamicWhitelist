"""
TimescaleDB monitoring and health check utilities for production environments.
"""

from typing import Dict, List, Any, Optional
from sqlalchemy import text, Engine
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TimescaleMonitor:
    """
    Monitor TimescaleDB performance, policies, and health metrics.
    """
    
    def __init__(self, engine: Engine):
        self.engine = engine
        
    def get_compression_stats(self) -> Dict[str, Any]:
        """
        Get compression statistics for all hypertables.
        
        Returns:
            Dictionary with compression metrics and ratios
        """
        
        stats = {
            "tables": [],
            "total_compressed_size": 0,
            "total_uncompressed_size": 0,
            "overall_ratio": 1.0,
            "error": None
        }
        
        try:
            with self.engine.connect() as conn:
                # Get compression stats - fixed query to avoid column ambiguity
                result = conn.execute(text("""
                    SELECT 
                        h.hypertable_name as table_name,
                        h.compression_enabled,
                        COALESCE(cs.total_chunks, 0) as total_chunks,
                        COALESCE(cs.number_compressed_chunks, 0) as compressed_chunks,
                        COALESCE(cs.uncompressed_heap_size, 0) as uncompressed_size,
                        COALESCE(cs.compressed_heap_size, 0) as compressed_size,
                        CASE 
                            WHEN cs.compressed_heap_size > 0 
                            THEN cs.uncompressed_heap_size::float / cs.compressed_heap_size 
                            ELSE 1.0 
                        END as compression_ratio
                    FROM timescaledb_information.hypertables h
                    LEFT JOIN timescaledb_information.compression_settings cs 
                        ON h.hypertable_schema = cs.hypertable_schema 
                        AND h.hypertable_name = cs.hypertable_name
                    WHERE h.hypertable_name IN ('token_raw_transfers', 'token_hourly_transfers')
                    ORDER BY h.hypertable_name
                """))
                
                total_uncompressed = 0
                total_compressed = 0
                
                for row in result:
                    table_stats = {
                        "table": row.table_name,
                        "compression_enabled": row.compression_enabled,
                        "total_chunks": row.total_chunks,
                        "compressed_chunks": row.compressed_chunks,
                        "uncompressed_size_bytes": row.uncompressed_size,
                        "compressed_size_bytes": row.compressed_size,
                        "compression_ratio": float(row.compression_ratio),
                        "compression_percentage": (1 - 1/float(row.compression_ratio)) * 100 if row.compression_ratio > 1 else 0
                    }
                    
                    stats["tables"].append(table_stats)
                    total_uncompressed += row.uncompressed_size
                    total_compressed += row.compressed_size
                
                stats["total_uncompressed_size"] = total_uncompressed
                stats["total_compressed_size"] = total_compressed
                stats["overall_ratio"] = total_uncompressed / max(total_compressed, 1)
                
        except Exception as e:
            logger.error(f"Failed to get compression stats: {e}")
            stats["error"] = str(e)
            
        return stats
    
    def get_policy_health(self) -> Dict[str, Any]:
        """
        Check health and performance of TimescaleDB policies.
        
        Returns:
            Policy health metrics and failure analysis
        """
        
        health = {
            "policies": [],
            "overall_health": "unknown",
            "failed_policies": [],
            "warnings": [],
            "error": None
        }
        
        try:
            with self.engine.connect() as conn:
                # Get policy execution stats
                result = conn.execute(text("""
                    SELECT 
                        j.job_id,
                        j.application_name,
                        j.proc_name,
                        j.schedule_interval,
                        j.max_runtime,
                        j.max_retries,
                        js.last_run_started_at,
                        js.last_successful_finish,
                        js.last_run_duration,
                        js.consecutive_failures,
                        js.total_runs,
                        js.total_successes,
                        js.total_failures
                    FROM timescaledb_information.jobs j
                    LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
                    WHERE j.proc_name IN ('policy_compression', 'policy_retention')
                    ORDER BY j.application_name, j.proc_name
                """))
                
                failed_count = 0
                warning_count = 0
                
                for row in result:
                    total_runs = row.total_runs or 0
                    total_successes = row.total_successes or 0
                    consecutive_failures = row.consecutive_failures or 0
                    
                    success_rate = (total_successes / max(total_runs, 1)) * 100
                    
                    # Determine policy health
                    policy_health = "healthy"
                    issues = []
                    
                    if consecutive_failures > 0:
                        policy_health = "failing"
                        issues.append(f"{consecutive_failures} consecutive failures")
                        failed_count += 1
                        
                    elif success_rate < 90 and total_runs > 5:
                        policy_health = "degraded"  
                        issues.append(f"Low success rate: {success_rate:.1f}%")
                        warning_count += 1
                        
                    elif row.last_successful_finish and row.last_successful_finish < datetime.now() - timedelta(hours=25):
                        policy_health = "stale"
                        issues.append("No recent successful execution")
                        warning_count += 1
                    
                    policy_info = {
                        "job_id": row.job_id,
                        "table": row.application_name,
                        "type": row.proc_name.replace("policy_", ""),
                        "health": policy_health,
                        "issues": issues,
                        "schedule_interval": str(row.schedule_interval) if row.schedule_interval else None,
                        "last_success": row.last_successful_finish,
                        "consecutive_failures": consecutive_failures,
                        "success_rate": success_rate,
                        "total_runs": total_runs,
                        "avg_duration": str(row.last_run_duration) if row.last_run_duration else None
                    }
                    
                    health["policies"].append(policy_info)
                    
                    if policy_health == "failing":
                        health["failed_policies"].append(policy_info)
                    elif policy_health in ["degraded", "stale"]:
                        health["warnings"].append(policy_info)
                
                # Overall health assessment
                total_policies = len(health["policies"])
                if failed_count > 0:
                    health["overall_health"] = "critical"
                elif warning_count > total_policies * 0.5:  # More than 50% have warnings
                    health["overall_health"] = "degraded"
                elif total_policies > 0:
                    health["overall_health"] = "healthy"
                else:
                    health["overall_health"] = "no_policies"
                    
        except Exception as e:
            logger.error(f"Failed to get policy health: {e}")
            health["error"] = str(e)
            health["overall_health"] = "error"
            
        return health
    
    def get_chunk_statistics(self) -> Dict[str, Any]:
        """
        Get chunk statistics for hypertables.
        
        Returns:
            Chunk distribution and size metrics
        """
        
        stats = {
            "tables": [],
            "total_chunks": 0,
            "total_size_bytes": 0,
            "error": None
        }
        
        try:
            with self.engine.connect() as conn:
                # Get chunk statistics
                result = conn.execute(text("""
                    SELECT 
                        hypertable_name,
                        chunk_name,
                        total_bytes,
                        index_bytes,
                        toast_bytes,
                        table_bytes,
                        compression_status,
                        is_compressed
                    FROM timescaledb_information.chunks
                    WHERE hypertable_name IN ('token_raw_transfers', 'token_hourly_transfers')
                    ORDER BY hypertable_name, chunk_name
                """))
                
                tables = {}
                total_size = 0
                total_chunks = 0
                
                for row in result:
                    table = row.hypertable_name
                    if table not in tables:
                        tables[table] = {
                            "table": table,
                            "chunk_count": 0,
                            "total_size_bytes": 0,
                            "compressed_chunks": 0,
                            "uncompressed_chunks": 0,
                            "avg_chunk_size": 0
                        }
                    
                    tables[table]["chunk_count"] += 1
                    tables[table]["total_size_bytes"] += row.total_bytes or 0
                    
                    if row.is_compressed:
                        tables[table]["compressed_chunks"] += 1
                    else:
                        tables[table]["uncompressed_chunks"] += 1
                    
                    total_size += row.total_bytes or 0
                    total_chunks += 1
                
                # Calculate averages
                for table_stats in tables.values():
                    if table_stats["chunk_count"] > 0:
                        table_stats["avg_chunk_size"] = table_stats["total_size_bytes"] // table_stats["chunk_count"]
                    table_stats["compression_ratio"] = table_stats["compressed_chunks"] / max(table_stats["chunk_count"], 1)
                    stats["tables"].append(table_stats)
                
                stats["total_chunks"] = total_chunks
                stats["total_size_bytes"] = total_size
                
        except Exception as e:
            logger.error(f"Failed to get chunk statistics: {e}")
            stats["error"] = str(e)
            
        return stats
    
    def generate_health_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive health report for TimescaleDB setup.
        
        Returns:
            Complete health assessment with recommendations
        """
        
        logger.info("🔍 Generating TimescaleDB health report...")
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "unknown",
            "compression": self.get_compression_stats(),
            "policies": self.get_policy_health(), 
            "chunks": self.get_chunk_statistics(),
            "recommendations": [],
            "critical_issues": [],
            "warnings": []
        }
        
        # Analyze compression performance
        if not report["compression"]["error"]:
            for table in report["compression"]["tables"]:
                if not table["compression_enabled"]:
                    report["critical_issues"].append(f"Compression not enabled for {table['table']}")
                elif table["compression_ratio"] < 2.0:
                    report["warnings"].append(f"Low compression ratio ({table['compression_ratio']:.1f}x) for {table['table']}")
                elif table["compression_percentage"] > 50:
                    report["recommendations"].append(f"Good compression on {table['table']} ({table['compression_percentage']:.0f}% size reduction)")
        
        # Analyze policy health
        policy_health = report["policies"]["overall_health"]
        if policy_health == "critical":
            report["critical_issues"].append("One or more policies are failing")
        elif policy_health == "degraded":
            report["warnings"].append("Some policies are performing poorly")
        
        # Analyze chunk distribution
        if not report["chunks"]["error"]:
            for table in report["chunks"]["tables"]:
                if table["chunk_count"] > 1000:
                    report["warnings"].append(f"High chunk count for {table['table']} ({table['chunk_count']} chunks)")
                elif table["chunk_count"] == 0:
                    report["warnings"].append(f"No chunks found for {table['table']} - check if data exists")
        
        # Determine overall status
        if len(report["critical_issues"]) > 0:
            report["overall_status"] = "critical"
        elif len(report["warnings"]) > 0:
            report["overall_status"] = "warning"
        else:
            report["overall_status"] = "healthy"
            
        # Add general recommendations
        if report["overall_status"] == "healthy":
            report["recommendations"].append("TimescaleDB configuration is optimal")
        else:
            report["recommendations"].append("Review critical issues and warnings for optimization opportunities")
            
        return report

def format_bytes(bytes_value: int) -> str:
    """Format bytes into human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"

def print_health_report(report: Dict[str, Any]) -> None:
    """Print formatted health report to logs."""
    
    logger.info("📊 TimescaleDB Health Report")
    logger.info("=" * 50)
    logger.info(f"Overall Status: {report['overall_status'].upper()}")
    logger.info(f"Report Time: {report['timestamp']}")
    
    # Critical issues
    if report["critical_issues"]:
        logger.error("🚨 Critical Issues:")
        for issue in report["critical_issues"]:
            logger.error(f"  - {issue}")
    
    # Warnings  
    if report["warnings"]:
        logger.warning("⚠️  Warnings:")
        for warning in report["warnings"]:
            logger.warning(f"  - {warning}")
    
    # Compression stats
    if not report["compression"]["error"]:
        logger.info("🗜️  Compression Statistics:")
        for table in report["compression"]["tables"]:
            logger.info(f"  {table['table']}:")
            logger.info(f"    - Enabled: {table['compression_enabled']}")
            logger.info(f"    - Ratio: {table['compression_ratio']:.1f}x")
            logger.info(f"    - Size reduction: {table['compression_percentage']:.0f}%")
    
    # Policy health
    if not report["policies"]["error"]:
        logger.info("🔧 Policy Health:")
        for policy in report["policies"]["policies"]:
            status_emoji = {"healthy": "✅", "degraded": "⚠️", "failing": "❌", "stale": "🕐"}.get(policy["health"], "❓")
            logger.info(f"  {status_emoji} {policy['table']} {policy['type']}: {policy['health']}")
            if policy["issues"]:
                for issue in policy["issues"]:
                    logger.info(f"      - {issue}")
    
    # Recommendations
    if report["recommendations"]:
        logger.info("💡 Recommendations:")
        for rec in report["recommendations"]:
            logger.info(f"  - {rec}")