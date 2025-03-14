"""
System health monitoring for Alpha Trade Exchange.

This module provides monitoring capabilities for:
1. Database performance metrics
2. Connection pool health
3. Background task status
4. System resource usage
"""

import time
import psutil
import asyncio
import logging
import bittensor as bt
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dataclasses import dataclass

from .alerts import AlertManager, AlertConfig, AlertType, AlertSeverity, Alert
from alphatrade.validator.database.manager import DatabaseManager

@dataclass
class DatabaseMetrics:
    """Database performance metrics."""
    active_connections: int
    idle_connections: int
    avg_query_time: float
    total_queries: int
    failed_queries: int
    last_snapshot_time: Optional[datetime]
    snapshot_task_running: bool

@dataclass
class SystemMetrics:
    """System metrics data class."""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    disk_usage_percent: float
    open_file_descriptors: int
    active_connections: int
    total_queries: int
    failed_queries: int
    
    def __await__(self):
        """Make the class awaitable."""
        yield self

class HealthMonitor:
    """Monitor system health and performance metrics."""
    
    def __init__(self, db_manager, alert_config: Optional[AlertConfig] = None):
        """Initialize health monitor.
        
        Args:
            db_manager: Database manager instance to monitor
            alert_config (AlertConfig, optional): Alert configuration
        """
        self.db = db_manager
        self._metrics_history: List[Dict] = []
        self._start_time = datetime.utcnow()
        self._monitoring = False
        self._monitor_task = None
        
        # Initialize alert manager
        self.alert_manager = AlertManager(alert_config)
        
        # Add default console logger for alerts
        self.alert_manager.add_handler(
            AlertType.DB_CONNECTIONS,
            lambda alert: print(f"[{alert.severity.name}] {alert.message}")
        )
        self.alert_manager.add_handler(
            AlertType.SYSTEM_RESOURCES,
            lambda alert: print(f"[{alert.severity.name}] {alert.message}")
        )
    
    async def get_db_metrics(self) -> DatabaseMetrics:
        """Get current database metrics.
        
        Returns:
            DatabaseMetrics: Current database performance metrics
        """
        async with self.db.get_connection() as conn:
            # Get connection pool stats
            active = len(self.db._connection_pool)
            total = self.db._db_semaphore._value
            
            # Get query stats from metrics history
            recent_metrics = self._metrics_history[-100:]  # Last 100 samples
            if recent_metrics:
                avg_time = sum(m['query_time'] for m in recent_metrics) / len(recent_metrics)
                total_queries = sum(m['queries'] for m in recent_metrics)
                failed = sum(m['failed'] for m in recent_metrics)
            else:
                avg_time = 0.0
                total_queries = 0
                failed = 0
            
            # Get snapshot task status
            has_task = hasattr(self.db, '_snapshot_task')
            task_running = has_task and not self.db._snapshot_task.done()
            
            # Get last snapshot time
            cursor = await conn.execute(
                "SELECT MAX(timestamp) FROM performance_snapshots"
            )
            row = await cursor.fetchone()
            last_snapshot = datetime.fromisoformat(row[0]) if row[0] else None
            
            metrics = DatabaseMetrics(
                active_connections=active,
                idle_connections=total - active,
                avg_query_time=avg_time,
                total_queries=total_queries,
                failed_queries=failed,
                last_snapshot_time=last_snapshot,
                snapshot_task_running=task_running
            )
            
            # Check for alerts
            if alert := self.alert_manager.check_database_metrics(metrics):
                await self.alert_manager.process_alert(alert)
            
            if alert := self.alert_manager.check_snapshot_timing(last_snapshot):
                await self.alert_manager.process_alert(alert)
            
            return metrics
    
    async def get_system_metrics(self) -> SystemMetrics:
        """Get current system metrics.
        
        Returns:
            SystemMetrics: Current system metrics
        """
        try:
            # Get CPU and memory usage
            cpu_percent = psutil.Process().cpu_percent()
            memory_percent = psutil.Process().memory_percent()
            
            # Get disk usage
            disk = psutil.disk_usage('/')
            disk_usage_percent = disk.percent
            
            # Get file descriptors
            open_fds = psutil.Process().num_fds()
            
            # Get database metrics - safely access attributes
            active_connections = 0  # Default value
            total_queries = 0
            failed_queries = 0
            
            # Safely get active connections
            if hasattr(self.db, '_connection_pool'):
                active_connections = len(self.db._connection_pool)
            elif hasattr(self.db, 'connection') and hasattr(self.db.connection, '_active_connections'):
                active_connections = self.db.connection._active_connections
                
            # Safely get total queries
            if hasattr(self.db, '_db_semaphore') and hasattr(self.db._db_semaphore, '_value'):
                total_queries = self.db._db_semaphore._value
            elif hasattr(self.db, '_total_queries'):
                total_queries = self.db._total_queries
                
            # Safely get failed queries
            if hasattr(self.db, 'connection') and hasattr(self.db.connection, '_failed_queries'):
                failed_queries = self.db.connection._failed_queries
            
            metrics = SystemMetrics(
                timestamp=datetime.now(timezone.utc),
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                disk_usage_percent=disk_usage_percent,
                open_file_descriptors=open_fds,
                active_connections=active_connections,
                total_queries=total_queries,
                failed_queries=failed_queries
            )
            
            # Check for alerts
            if alert := self.alert_manager.check_system_metrics(metrics):
                await self.alert_manager.process_alert(alert)
            
            return metrics
            
        except Exception as e:
            bt.logging.error(f"Failed to get system metrics: {str(e)}")
            return SystemMetrics(
                timestamp=datetime.now(timezone.utc),
                cpu_percent=0.0,
                memory_percent=0.0,
                disk_usage_percent=0.0,
                open_file_descriptors=0,
                active_connections=0,
                total_queries=0,
                failed_queries=0
            )
    
    async def check_health(self) -> Dict:
        """Perform comprehensive health check.
        
        Returns:
            Dict: Health check results including:
                - status: 'healthy' or 'unhealthy'
                - database: Database health metrics
                - system: System resource metrics
                - uptime: System uptime in seconds
                - alerts: Current active alerts
        """
        try:
            db_metrics = await self.get_db_metrics()
            sys_metrics = await self.get_system_metrics()
            
            # Get active alerts
            active_alerts = self.alert_manager.get_active_alerts()
            
            # Consider system unhealthy if any ERROR or CRITICAL alerts
            unhealthy = any(
                alert.severity in (AlertSeverity.ERROR, AlertSeverity.CRITICAL)
                for alert in active_alerts
            )
            
            return {
                'status': 'unhealthy' if unhealthy else 'healthy',
                'database': {
                    'connections': {
                        'active': db_metrics.active_connections,
                        'idle': db_metrics.idle_connections
                    },
                    'queries': {
                        'avg_time': db_metrics.avg_query_time,
                        'total': db_metrics.total_queries,
                        'failed': db_metrics.failed_queries
                    },
                    'snapshots': {
                        'last_snapshot': db_metrics.last_snapshot_time.isoformat() 
                            if db_metrics.last_snapshot_time else None,
                        'task_running': db_metrics.snapshot_task_running
                    }
                },
                'system': {
                    'cpu_percent': sys_metrics.cpu_percent,
                    'memory_percent': sys_metrics.memory_percent,
                    'disk_usage_percent': sys_metrics.disk_usage_percent,
                    'open_fds': sys_metrics.open_file_descriptors
                },
                'uptime': (datetime.utcnow() - self._start_time).total_seconds(),
                'alerts': [
                    {
                        'type': alert.type.value,
                        'severity': alert.severity.value,
                        'message': alert.message,
                        'timestamp': alert.timestamp.isoformat()
                    }
                    for alert in active_alerts
                ]
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'alerts': [{
                    'type': AlertType.SYSTEM_RESOURCES.value,
                    'severity': AlertSeverity.CRITICAL.value,
                    'message': f"Health check failed: {str(e)}",
                    'timestamp': datetime.utcnow().isoformat()
                }]
            }
    
    async def start_monitoring(self, interval: int = 60):
        """Start periodic health monitoring.
        
        Args:
            interval (int): Seconds between health checks
        """
        async def monitor_loop():
            while self._monitoring:
                try:
                    # Record metrics
                    start = time.time()
                    health = await self.check_health()
                    duration = time.time() - start
                    
                    self._metrics_history.append({
                        'timestamp': datetime.utcnow(),
                        'query_time': duration,
                        'queries': 1,
                        'failed': 0 if health['status'] == 'healthy' else 1
                    })
                    
                    # Keep last 24 hours of metrics
                    cutoff = datetime.utcnow().timestamp() - 86400
                    self._metrics_history = [
                        m for m in self._metrics_history
                        if m['timestamp'].timestamp() > cutoff
                    ]
                    
                    # Cleanup old alerts
                    self.alert_manager.cleanup_old_alerts()
                    
                except Exception as e:
                    print(f"Error in health monitor: {str(e)}")
                    
                await asyncio.sleep(interval)
        
        self._monitoring = True
        if not self._monitor_task:
            self._monitor_task = asyncio.create_task(monitor_loop())
    
    async def stop_monitoring(self):
        """Stop health monitoring."""
        self._monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None 