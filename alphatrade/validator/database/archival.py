"""
Database archival module for managing historical data.
"""

import asyncio
import logging
import bittensor as bt
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from .manager import DatabaseManager
from .models import Position, Trade, PositionTrade

class DatabaseArchival:
    """Archival manager for database cleanup operations."""
    
    def __init__(self):
        """Initialize archival manager."""
        self._db = DatabaseManager()  # Get singleton instance
        self._archive_task = None
        
    async def archive_closed_positions(
        self,
        miner_uid: int,
        archive_after: timedelta,
        reference_time: Optional[datetime] = None
    ) -> Dict[str, int]:
        """Archive closed positions older than the specified duration.
        
        Args:
            miner_uid: Miner's UID
            archive_after: Archive positions older than this
            reference_time: Reference time for cutoff calculation
                Defaults to current UTC time
                
        Returns:
            Dict containing counts of archived items
        """
        reference_time = reference_time or datetime.now(timezone.utc)
        if reference_time.tzinfo is None:
            reference_time = reference_time.replace(tzinfo=timezone.utc)
        cutoff_time = reference_time - archive_after
        
        async with self._db.connection.get_connection() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                # Get positions to archive
                cursor = await conn.execute(
                    f"""
                    SELECT DISTINCT p.* 
                    FROM miner_{miner_uid}_positions p
                    JOIN miner_{miner_uid}_position_trades pt ON p.position_id = pt.position_id
                    JOIN miner_{miner_uid}_trades t ON pt.trade_id = t.trade_id
                    WHERE p.status = ?
                    GROUP BY p.position_id
                    HAVING strftime('%Y-%m-%d %H:%M:%f', MAX(t.exit_timestamp)) < strftime('%Y-%m-%d %H:%M:%f', ?)
                    """,
                    (
                        'CLOSED',
                        cutoff_time.isoformat()
                    )
                )
                positions = await cursor.fetchall()
                
                if not positions:
                    return {"positions_archived": 0, "trades_archived": 0}
                    
                # Get related trades
                position_ids = [p[0] for p in positions]
                placeholders = ",".join("?" * len(position_ids))
                cursor = await conn.execute(
                    f"""
                    SELECT DISTINCT t.*
                    FROM miner_{miner_uid}_trades t
                    JOIN miner_{miner_uid}_position_trades pt
                        ON t.trade_id = pt.trade_id
                    WHERE pt.position_id IN ({placeholders})
                    """,
                    position_ids
                )
                trades = await cursor.fetchall()
                
                # Archive positions
                await conn.executemany(
                    f"""
                    INSERT INTO miner_{miner_uid}_archived_positions
                    SELECT * FROM miner_{miner_uid}_positions
                    WHERE position_id = ?
                    """,
                    [(p[0],) for p in positions]
                )
                
                # Archive trades
                await conn.executemany(
                    f"""
                    INSERT INTO miner_{miner_uid}_archived_trades
                    SELECT * FROM miner_{miner_uid}_trades
                    WHERE trade_id = ?
                    """,
                    [(t[0],) for t in trades]
                )
                
                # Archive position-trade mappings
                await conn.execute(
                    f"""
                    INSERT INTO miner_{miner_uid}_archived_position_trades
                    SELECT pt.* 
                    FROM miner_{miner_uid}_position_trades pt
                    WHERE pt.position_id IN ({placeholders})
                    """,
                    position_ids
                )
                
                # Delete archived data from active tables
                await conn.execute(
                    f"""
                    DELETE FROM miner_{miner_uid}_position_trades
                    WHERE position_id IN ({placeholders})
                    """,
                    position_ids
                )
                
                await conn.execute(
                    f"""
                    DELETE FROM miner_{miner_uid}_positions
                    WHERE position_id IN ({placeholders})
                    """,
                    position_ids
                )
                
                trade_ids = [t[0] for t in trades]
                trade_placeholders = ",".join("?" * len(trade_ids))
                await conn.execute(
                    f"""
                    DELETE FROM miner_{miner_uid}_trades
                    WHERE trade_id IN ({trade_placeholders})
                    """,
                    trade_ids
                )
                
                await conn.commit()
                
                return {
                    "positions_archived": len(positions),
                    "trades_archived": len(trades)
                }
                
            except Exception as e:
                await conn.rollback()
                raise DatabaseError(f"Failed to archive positions: {str(e)}")
    
    async def start_archival_task(
        self,
        archive_interval: int = 86400,  # Default to daily
        archive_after: timedelta = timedelta(days=7)
    ):
        """Start periodic archival of old positions.
        
        Args:
            archive_interval: Seconds between archive runs
            archive_after: Archive closed positions after this time
        """
        async def archive_loop():
            while True:
                try:
                    # Get all miners
                    async with self._db.connection.get_connection() as conn:
                        cursor = await conn.execute("SELECT uid FROM miners")
                        miner_uids = [row[0] async for row in cursor]
                    
                    # Archive old positions for each miner
                    for uid in miner_uids:
                        try:
                            result = await self.archive_closed_positions(
                                uid,
                                archive_after=archive_after
                            )
                            if result['positions_archived'] > 0 or result['trades_archived'] > 0:
                                bt.logging.info(
                                    f"Archived {result['positions_archived']} positions and "
                                    f"{result['trades_archived']} trades for miner {uid}"
                                )
                        except Exception as e:
                            bt.logging.error(f"Failed to archive positions for miner {uid}: {str(e)}")
                            
                    # Wait for next interval
                    await asyncio.sleep(archive_interval)
                    
                except Exception as e:
                    bt.logging.error(f"Error in archive loop: {str(e)}")
                    await asyncio.sleep(60)  # Wait before retry
                    
        self._archive_task = asyncio.create_task(archive_loop())
    
    async def stop_archival_task(self):
        """Stop the archival task."""
        if self._archive_task:
            self._archive_task.cancel()
            try:
                await self._archive_task
            except asyncio.CancelledError:
                pass
            self._archive_task = None
            
    async def cleanup_old_data(self):
        """Clean up old data based on retention policy."""
        if not self._db.performance_config:
            return
            
        cutoff = datetime.now(timezone.utc) - self._db.performance_config.retention_period
        
        async with self._db.connection.get_connection() as conn:
            # Archive old snapshots
            await conn.execute(
                """
                DELETE FROM performance_snapshots 
                WHERE timestamp < ?
                """,
                (self._db._format_timestamp(cutoff),)
            )
            
            # Archive old positions and trades
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT uid FROM miners")
                miners = await cursor.fetchall()
                
                for miner in miners:
                    await self.archive_closed_positions(
                        miner['uid'],
                        self._db.performance_config.retention_period,
                        reference_time=datetime.now(timezone.utc)
                    )
            
            await conn.commit() 