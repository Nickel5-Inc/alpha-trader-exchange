"""
Web dashboard for system monitoring.

This module provides a FastAPI-based web interface for:
1. Viewing real-time system health
2. Monitoring database metrics
3. Tracking performance statistics
4. Viewing system resource usage
5. Managing system alerts
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import json
from datetime import datetime, timedelta
from typing import Optional, List

from .health import HealthMonitor
from .alerts import AlertType, AlertSeverity

app = FastAPI(title="Alpha Trade Monitor")
monitor: Optional[HealthMonitor] = None

def init_dashboard(health_monitor: HealthMonitor):
    """Initialize the dashboard with a health monitor.
    
    Args:
        health_monitor (HealthMonitor): Monitor instance to use
    """
    global monitor
    monitor = health_monitor

@app.get("/health")
async def health_check():
    """Get current system health status."""
    if not monitor:
        raise HTTPException(status_code=500, detail="Monitor not initialized")
    return await monitor.check_health()

@app.get("/metrics/database")
async def database_metrics():
    """Get current database metrics."""
    if not monitor:
        raise HTTPException(status_code=500, detail="Monitor not initialized")
    return await monitor.get_db_metrics()

@app.get("/metrics/system")
async def system_metrics():
    """Get current system metrics."""
    if not monitor:
        raise HTTPException(status_code=500, detail="Monitor not initialized")
    return monitor.get_system_metrics()

@app.get("/alerts")
async def get_alerts(
    alert_type: Optional[str] = None,
    severity: Optional[str] = None,
    hours: Optional[int] = Query(24, ge=1, le=168)  # 1 hour to 1 week
):
    """Get filtered alert history."""
    if not monitor:
        raise HTTPException(status_code=500, detail="Monitor not initialized")
        
    # Convert string parameters to enums
    alert_type_enum = AlertType(alert_type) if alert_type else None
    severity_enum = AlertSeverity(severity) if severity else None
    
    # Get time range
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    
    alerts = monitor.alert_manager.get_alert_history(
        alert_type=alert_type_enum,
        severity=severity_enum,
        start_time=start_time,
        end_time=end_time
    )
    
    return {
        'alerts': [
            {
                'type': alert.type.value,
                'severity': alert.severity.value,
                'message': alert.message,
                'timestamp': alert.timestamp.isoformat(),
                'value': alert.value,
                'threshold': alert.threshold
            }
            for alert in alerts
        ]
    }

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Render monitoring dashboard."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Alpha Trade Monitor</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { 
                font-family: Arial, sans-serif; 
                margin: 0; 
                padding: 20px;
                background: #f5f5f5;
            }
            .grid { 
                display: grid; 
                grid-template-columns: repeat(2, 1fr); 
                gap: 20px; 
                margin-bottom: 20px;
            }
            .card {
                background: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            h1 { color: #333; }
            .status { 
                font-size: 24px; 
                margin: 10px 0; 
                padding: 10px;
                border-radius: 4px;
            }
            .healthy { 
                background: #4caf50; 
                color: white;
            }
            .unhealthy { 
                background: #f44336;
                color: white;
            }
            .alerts {
                margin-top: 20px;
                padding: 0;
                list-style: none;
            }
            .alert {
                padding: 10px;
                margin: 5px 0;
                border-radius: 4px;
                border-left: 4px solid;
            }
            .alert.info { 
                background: #e3f2fd;
                border-color: #2196f3;
            }
            .alert.warning {
                background: #fff3e0;
                border-color: #ff9800;
            }
            .alert.error {
                background: #ffebee;
                border-color: #f44336;
            }
            .alert.critical {
                background: #ff1744;
                border-color: #d50000;
                color: white;
            }
            .alert-time {
                float: right;
                color: #666;
                font-size: 0.9em;
            }
            .chart {
                height: 300px;
            }
        </style>
    </head>
    <body>
        <h1>System Monitor</h1>
        <div id="status" class="status"></div>
        
        <!-- Active Alerts -->
        <div class="card">
            <h2>Active Alerts</h2>
            <ul id="activeAlerts" class="alerts"></ul>
        </div>
        
        <div class="grid">
            <div class="card">
                <h2>Database Metrics</h2>
                <div id="dbMetrics"></div>
            </div>
            <div class="card">
                <h2>System Resources</h2>
                <div id="sysMetrics"></div>
            </div>
            <div class="card">
                <h2>Query Performance</h2>
                <div id="queryChart" class="chart"></div>
            </div>
            <div class="card">
                <h2>Resource Usage</h2>
                <div id="resourceChart" class="chart"></div>
            </div>
        </div>
        
        <!-- Alert History -->
        <div class="card">
            <h2>Alert History</h2>
            <ul id="alertHistory" class="alerts"></ul>
        </div>
        
        <script>
            function formatTime(isoString) {
                const date = new Date(isoString);
                return date.toLocaleString();
            }
            
            function updateAlerts(alerts) {
                const activeAlerts = document.getElementById('activeAlerts');
                activeAlerts.innerHTML = alerts.length ? '' : '<li>No active alerts</li>';
                
                alerts.forEach(alert => {
                    const li = document.createElement('li');
                    li.className = `alert ${alert.severity}`;
                    li.innerHTML = `
                        <span class="alert-time">${formatTime(alert.timestamp)}</span>
                        <strong>${alert.type}:</strong> ${alert.message}
                    `;
                    activeAlerts.appendChild(li);
                });
            }
            
            function updateStatus() {
                fetch('/health')
                    .then(response => response.json())
                    .then(data => {
                        const status = document.getElementById('status');
                        status.textContent = `Status: ${data.status.toUpperCase()}`;
                        status.className = `status ${data.status}`;
                        
                        // Update alerts
                        updateAlerts(data.alerts || []);
                        
                        // Update database metrics
                        const dbMetrics = document.getElementById('dbMetrics');
                        dbMetrics.innerHTML = `
                            <p>Active Connections: ${data.database.connections.active}</p>
                            <p>Idle Connections: ${data.database.connections.idle}</p>
                            <p>Total Queries: ${data.database.queries.total}</p>
                            <p>Failed Queries: ${data.database.queries.failed}</p>
                            <p>Avg Query Time: ${data.database.queries.avg_time.toFixed(3)}s</p>
                            <p>Last Snapshot: ${data.database.snapshots.last_snapshot || 'Never'}</p>
                        `;
                        
                        // Update system metrics
                        const sysMetrics = document.getElementById('sysMetrics');
                        sysMetrics.innerHTML = `
                            <p>CPU Usage: ${data.system.cpu_percent.toFixed(1)}%</p>
                            <p>Memory Usage: ${data.system.memory_percent.toFixed(1)}%</p>
                            <p>Disk Usage: ${data.system.disk_usage_percent.toFixed(1)}%</p>
                            <p>Open File Descriptors: ${data.system.open_fds}</p>
                            <p>Uptime: ${Math.floor(data.uptime / 3600)}h ${Math.floor((data.uptime % 3600) / 60)}m</p>
                        `;
                    });
            }
            
            function updateAlertHistory() {
                fetch('/alerts')
                    .then(response => response.json())
                    .then(data => {
                        const history = document.getElementById('alertHistory');
                        history.innerHTML = data.alerts.length ? '' : '<li>No alerts in the last 24 hours</li>';
                        
                        data.alerts.forEach(alert => {
                            const li = document.createElement('li');
                            li.className = `alert ${alert.severity}`;
                            li.innerHTML = `
                                <span class="alert-time">${formatTime(alert.timestamp)}</span>
                                <strong>${alert.type}:</strong> ${alert.message}
                                <br>
                                <small>Value: ${alert.value.toFixed(2)} (Threshold: ${alert.threshold})</small>
                            `;
                            history.appendChild(li);
                        });
                    });
            }
            
            // Initialize charts
            const queryTrace = {
                y: [],
                type: 'scatter',
                name: 'Query Time'
            };
            
            const resourceTrace = {
                y: [],
                type: 'scatter',
                name: 'CPU Usage'
            };
            
            Plotly.newPlot('queryChart', [queryTrace], {
                title: 'Query Response Time',
                yaxis: { title: 'Time (s)' }
            });
            
            Plotly.newPlot('resourceChart', [resourceTrace], {
                title: 'CPU Usage',
                yaxis: { title: '%' }
            });
            
            // Update status and alerts every 5 seconds
            setInterval(updateStatus, 5000);
            updateStatus();
            
            // Update alert history every minute
            setInterval(updateAlertHistory, 60000);
            updateAlertHistory();
            
            // Update charts every 30 seconds
            setInterval(() => {
                fetch('/metrics/database')
                    .then(response => response.json())
                    .then(data => {
                        Plotly.extendTraces('queryChart', {
                            y: [[data.avg_query_time]]
                        }, [0]);
                    });
                    
                fetch('/metrics/system')
                    .then(response => response.json())
                    .then(data => {
                        Plotly.extendTraces('resourceChart', {
                            y: [[data.cpu_percent]]
                        }, [0]);
                    });
            }, 30000);
        </script>
    </body>
    </html>
    """