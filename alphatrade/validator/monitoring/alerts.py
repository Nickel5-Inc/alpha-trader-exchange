"""
Alert system for monitoring critical metrics.

This module provides configurable alerts for:
1. Database performance issues
2. System resource thresholds
3. Operation failures
4. Data integrity problems
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Any
import logging
import asyncio
import bittensor as bt

# Remove logging configuration
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class AlertType(Enum):
    """Types of alerts that can be triggered."""
    DB_CONNECTIONS = "db_connections"
    QUERY_PERFORMANCE = "query_performance"
    SYSTEM_RESOURCES = "system_resources"
    OPERATION_FAILURES = "operation_failures"
    DATA_INTEGRITY = "data_integrity"
    SNAPSHOT_DELAY = "snapshot_delay"

@dataclass
class AlertConfig:
    """Configuration for alert thresholds."""
    # Database thresholds
    max_active_connections: int = 80  # Percentage of pool
    max_query_time: float = 1.0  # seconds
    max_failed_queries: int = 50  # per hour
    min_idle_connections: int = 2
    
    # System resource thresholds
    max_cpu_percent: float = 85.0
    max_memory_percent: float = 85.0
    max_disk_percent: float = 85.0
    max_open_files: int = 1000
    
    # Operation thresholds
    max_failed_operations: int = 10  # per hour
    max_retry_attempts: int = 3
    
    # Timing thresholds
    max_snapshot_delay: int = 3600  # seconds
    max_processing_delay: int = 300  # seconds
    
    # Recovery thresholds
    max_recovery_attempts: int = 3
    recovery_cooldown: int = 300  # seconds

@dataclass
class Alert:
    """Alert instance with metadata."""
    type: AlertType
    severity: AlertSeverity
    message: str
    timestamp: datetime
    value: float
    threshold: float
    context: Optional[Dict] = None

class AlertManager:
    """Manages system alerts and notifications."""
    
    def __init__(self, config: Optional[AlertConfig] = None):
        """Initialize alert manager.
        
        Args:
            config (AlertConfig, optional): Alert configuration
        """
        self.config = config or AlertConfig()
        self._active_alerts: Dict[AlertType, Alert] = {}
        self._alert_history: List[Alert] = []
        self._handlers: Dict[AlertType, List[Callable]] = {}
        self._alert_counts: Dict[AlertType, int] = {}
        self._last_cleanup = datetime.utcnow()
    
    def add_handler(self, alert_type: AlertType, handler: Callable[[Alert], Any]):
        """Add an alert handler.
        
        Args:
            alert_type (AlertType): Type of alert to handle
            handler (Callable): Handler function
        """
        if alert_type not in self._handlers:
            self._handlers[alert_type] = []
        self._handlers[alert_type].append(handler)
    
    async def process_alert(self, alert: Alert):
        """Process an alert through registered handlers.
        
        Args:
            alert (Alert): Alert to process
        """
        # Update alert counts
        self._alert_counts[alert.type] = self._alert_counts.get(alert.type, 0) + 1
        
        # Store in history
        self._alert_history.append(alert)
        
        # Set as active alert
        self._active_alerts[alert.type] = alert
        
        # Call handlers
        handlers = self._handlers.get(alert.type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(alert)
                else:
                    handler(alert)
            except Exception as e:
                logger.error(f"Alert handler failed: {str(e)}")
    
    def check_database_metrics(self, metrics) -> Optional[Alert]:
        """Check database metrics against thresholds.
        
        Args:
            metrics: Current database metrics
            
        Returns:
            Optional[Alert]: Alert if threshold exceeded
        """
        # Check connection pool
        pool_usage = (metrics.active_connections / 
                     (metrics.active_connections + metrics.idle_connections)) * 100
        if pool_usage >= self.config.max_active_connections:
            return Alert(
                type=AlertType.DB_CONNECTIONS,
                severity=AlertSeverity.ERROR,
                message=f"Connection pool usage at {pool_usage:.1f}%",
                timestamp=datetime.utcnow(),
                value=pool_usage,
                threshold=self.config.max_active_connections
            )
        
        # Check query performance
        if metrics.avg_query_time >= self.config.max_query_time:
            return Alert(
                type=AlertType.QUERY_PERFORMANCE,
                severity=AlertSeverity.WARNING,
                message=f"High average query time: {metrics.avg_query_time:.3f}s",
                timestamp=datetime.utcnow(),
                value=metrics.avg_query_time,
                threshold=self.config.max_query_time
            )
        
        # Check failed queries
        if metrics.failed_queries >= self.config.max_failed_queries:
            return Alert(
                type=AlertType.OPERATION_FAILURES,
                severity=AlertSeverity.ERROR,
                message=f"High number of failed queries: {metrics.failed_queries}",
                timestamp=datetime.utcnow(),
                value=metrics.failed_queries,
                threshold=self.config.max_failed_queries
            )
        
        return None
    
    def check_system_metrics(self, metrics) -> Optional[Alert]:
        """Check system metrics against thresholds.
        
        Args:
            metrics: Current system metrics
            
        Returns:
            Optional[Alert]: Alert if threshold exceeded
        """
        # Check CPU usage
        if metrics.cpu_percent >= self.config.max_cpu_percent:
            return Alert(
                type=AlertType.SYSTEM_RESOURCES,
                severity=AlertSeverity.WARNING,
                message=f"High CPU usage: {metrics.cpu_percent:.1f}%",
                timestamp=datetime.utcnow(),
                value=metrics.cpu_percent,
                threshold=self.config.max_cpu_percent
            )
        
        # Check memory usage
        if metrics.memory_percent >= self.config.max_memory_percent:
            return Alert(
                type=AlertType.SYSTEM_RESOURCES,
                severity=AlertSeverity.WARNING,
                message=f"High memory usage: {metrics.memory_percent:.1f}%",
                timestamp=datetime.utcnow(),
                value=metrics.memory_percent,
                threshold=self.config.max_memory_percent
            )
        
        # Check disk usage
        if metrics.disk_usage_percent >= self.config.max_disk_percent:
            return Alert(
                type=AlertType.SYSTEM_RESOURCES,
                severity=AlertSeverity.ERROR,
                message=f"High disk usage: {metrics.disk_usage_percent:.1f}%",
                timestamp=datetime.utcnow(),
                value=metrics.disk_usage_percent,
                threshold=self.config.max_disk_percent
            )
        
        return None
    
    def check_snapshot_timing(self, last_snapshot: Optional[datetime]) -> Optional[Alert]:
        """Check if performance snapshots are delayed.
        
        Args:
            last_snapshot (datetime): Timestamp of last snapshot
            
        Returns:
            Optional[Alert]: Alert if snapshot is delayed
        """
        if not last_snapshot:
            return Alert(
                type=AlertType.SNAPSHOT_DELAY,
                severity=AlertSeverity.WARNING,
                message="No performance snapshots taken yet",
                timestamp=datetime.utcnow(),
                value=0,
                threshold=self.config.max_snapshot_delay
            )
        
        delay = (datetime.utcnow() - last_snapshot).total_seconds()
        if delay >= self.config.max_snapshot_delay:
            return Alert(
                type=AlertType.SNAPSHOT_DELAY,
                severity=AlertSeverity.WARNING,
                message=f"Performance snapshot delayed by {delay:.0f}s",
                timestamp=datetime.utcnow(),
                value=delay,
                threshold=self.config.max_snapshot_delay
            )
        
        return None
    
    def cleanup_old_alerts(self):
        """Clean up old alerts from history."""
        now = datetime.utcnow()
        
        # Only clean up once per hour
        if (now - self._last_cleanup).total_seconds() < 3600:
            return
            
        # Keep last 24 hours of alerts
        cutoff = now - timedelta(hours=24)
        self._alert_history = [
            alert for alert in self._alert_history
            if alert.timestamp > cutoff
        ]
        
        # Reset alert counts
        self._alert_counts = {}
        self._last_cleanup = now
    
    def get_active_alerts(self) -> List[Alert]:
        """Get list of currently active alerts.
        
        Returns:
            List[Alert]: Active alerts
        """
        return list(self._active_alerts.values())
    
    def get_alert_history(
        self,
        alert_type: Optional[AlertType] = None,
        severity: Optional[AlertSeverity] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Alert]:
        """Get filtered alert history.
        
        Args:
            alert_type (AlertType, optional): Filter by alert type
            severity (AlertSeverity, optional): Filter by severity
            start_time (datetime, optional): Start of time range
            end_time (datetime, optional): End of time range
            
        Returns:
            List[Alert]: Filtered alerts
        """
        alerts = self._alert_history
        
        if alert_type:
            alerts = [a for a in alerts if a.type == alert_type]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        if start_time:
            alerts = [a for a in alerts if a.timestamp >= start_time]
        if end_time:
            alerts = [a for a in alerts if a.timestamp <= end_time]
            
        return alerts 