"""
Monitoring system initialization.

This module provides functions to initialize and start the monitoring system,
including the health monitor and web dashboard.
"""

import uvicorn
from typing import Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .health import HealthMonitor
from .dashboard import init_dashboard, app

class MonitoringSystem:
    """System monitoring manager."""
    
    def __init__(self, db_manager, host: str = "0.0.0.0", port: int = 8080):
        """Initialize monitoring system.
        
        Args:
            db_manager: Database manager to monitor
            host (str): Host to bind dashboard to
            port (int): Port to run dashboard on
        """
        self.host = host
        self.port = port
        self.monitor = HealthMonitor(db_manager)
        self._dashboard_task: Optional[asyncio.Task] = None
        self._executor = ThreadPoolExecutor(max_workers=1)
        
        # Initialize dashboard
        init_dashboard(self.monitor)
    
    async def start(self):
        """Start monitoring system."""
        # Start health monitoring
        await self.monitor.start_monitoring()
        
        # Start dashboard in separate thread
        self._dashboard_task = asyncio.get_event_loop().run_in_executor(
            self._executor,
            uvicorn.run,
            app,
            {
                "host": self.host,
                "port": self.port,
                "log_level": "info"
            }
        )
    
    async def stop(self):
        """Stop monitoring system."""
        # Stop health monitoring
        await self.monitor.stop_monitoring()
        
        # Stop dashboard
        if self._dashboard_task:
            self._executor.shutdown(wait=True)
            self._dashboard_task.cancel()
            try:
                await self._dashboard_task
            except asyncio.CancelledError:
                pass
            self._dashboard_task = None 