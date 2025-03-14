"""
Database maintenance module for recovery and cleanup operations.

This module handles database maintenance tasks such as:
- Database state recovery after crashes
- Database integrity verification
- Performance optimization
"""

from typing import Dict
from datetime import datetime, timezone
import logging
import bittensor as bt

from .manager import DatabaseManager
from .exceptions import DatabaseError
from .miner import MinerManager

# Remove logger initialization
# logger = logging.getLogger(__name__)

class DatabaseMaintenance:
    """Manager class for database maintenance operations."""
    
    def __init__(self):
        """Initialize maintenance manager."""
        self._db = DatabaseManager()  # Get singleton instance
        self._miner_manager = MinerManager()
    
    async def recover_database_state(self) -> Dict[str, int]:
        """Recover database state after crash.
        
        This method:
        1. Verifies database integrity
        2. Ensures all required tables exist
        3. Ensures all miner tables exist
        4. Recalculates miner statistics
        
        Returns:
            Dict[str, int]: Recovery statistics
        
        Raises:
            DatabaseError: If recovery fails
        """
        stats = {
            'tables_verified': 0,
            'miners_recovered': 0,
            'positions_verified': 0,
            'trades_verified': 0,
            'errors': 0
        }
        
        try:
            # 1. Verify database integrity
            integrity_ok = await self.verify_database_integrity()
            if not integrity_ok:
                raise DatabaseError("Database integrity check failed")
                
            # 2. Ensure all required tables exist
            await self._db.setup_database()
            stats['tables_verified'] = 1
            
            # 3. Ensure all miner tables exist
            await self._miner_manager.ensure_miner_tables_exist()
            
            # 4. Get all miners
            async with self._db.connection.get_connection() as conn:
                cursor = await conn.execute("SELECT uid FROM miners")
                miners = await cursor.fetchall()
                
                # 5. For each miner, verify tables and recalculate statistics
                for miner in miners:
                    miner_uid = miner[0]
                    try:
                        # Ensure tables exist
                        await self._miner_manager.ensure_miner_tables_exist_for_miner_uid(miner_uid)
                        
                        # Recalculate statistics
                        await self._recalculate_miner_statistics(conn, miner_uid)
                        
                        stats['miners_recovered'] += 1
                    except Exception as e:
                        bt.logging.info(
                            f"Error recovering miner {miner_uid}: {str(e)}"
                        )
                        stats['errors'] += 1
                        
            return stats
            
        except Exception as e:
            bt.logging.error(f"Database recovery failed: {str(e)}")
            raise DatabaseError(f"Failed to recover database state: {str(e)}")
    
    async def verify_database_integrity(self) -> bool:
        """Verify database integrity.
        
        This method:
        1. Runs SQLite integrity check
        2. Verifies foreign key constraints
        3. Checks for missing indexes
        
        Returns:
            bool: True if database is intact, False otherwise
        """
        try:
            async with self._db.connection.get_connection() as conn:
                # 1. Run SQLite integrity check
                cursor = await conn.execute("PRAGMA integrity_check")
                result = await cursor.fetchone()
                
                if result[0] != "ok":
                    bt.logging.error(f"Database integrity check failed: {result[0]}")
                    return False
                
                # 2. Verify foreign key constraints
                cursor = await conn.execute("PRAGMA foreign_key_check")
                violations = await cursor.fetchall()
                
                if violations:
                    bt.logging.error(f"Foreign key violations found: {violations}")
                    return False
                
                # 3. Check for missing indexes
                # Get all tables
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                tables = await cursor.fetchall()
                
                # Check each table for indexes
                missing_indexes = []
                for table in tables:
                    table_name = table[0]
                    if table_name.startswith("sqlite_"):
                        continue  # Skip SQLite internal tables
                        
                    cursor = await conn.execute(
                        f"PRAGMA index_list({table_name})"
                    )
                    indexes = await cursor.fetchall()
                    
                    if not indexes and not table_name.endswith("_archived"):
                        missing_indexes.append(table_name)
                
                if missing_indexes:
                    bt.logging.error(f"Missing indexes found: {missing_indexes}")
                    return False
                
                bt.logging.info("Database integrity verified successfully")
                return True
                
        except Exception as e:
            bt.logging.error(f"Database integrity check failed: {str(e)}")
            return False
    
    async def optimize_database(self):
        """Optimize database performance.
        
        This method:
        1. Runs VACUUM to reclaim space
        2. Runs ANALYZE to update statistics
        3. Rebuilds indexes
        
        Raises:
            DatabaseError: If optimization fails
        """
        try:
            async with self._db.connection.get_connection() as conn:
                # 1. Run VACUUM to reclaim space
                await conn.execute("VACUUM")
                
                # 2. Run ANALYZE to update statistics
                await conn.execute("ANALYZE")
                
                # 3. Rebuild indexes
                # Get all indexes
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                )
                indexes = await cursor.fetchall()
                
                # Rebuild each index
                for index in indexes:
                    index_name = index[0]
                    await conn.execute(f"REINDEX {index_name}")
                
                bt.logging.info("Database optimization completed successfully")
                
        except Exception as e:
            bt.logging.error(f"Database optimization failed: {str(e)}")
            raise DatabaseError(f"Failed to optimize database: {str(e)}")
    
    async def _recalculate_miner_statistics(self, conn, miner_uid: int):
        """Recalculate miner statistics.
        
        Args:
            conn: Database connection
            miner_uid: Miner's UID
        """
        # Get total trades
        cursor = await conn.execute(
            f"SELECT COUNT(*) FROM miner_{miner_uid}_trades"
        )
        total_trades = (await cursor.fetchone())[0]
        
        # Get total volume
        cursor = await conn.execute(
            f"""
            SELECT 
                SUM(exit_tao) as total_tao,
                SUM(exit_alpha) as total_alpha
            FROM miner_{miner_uid}_trades
            """
        )
        row = await cursor.fetchone()
        total_tao = row[0] or 0.0
        total_alpha = row[1] or 0.0
        
        # Get ROI and duration
        cursor = await conn.execute(
            f"""
            SELECT 
                AVG(roi_tao) as avg_roi,
                AVG(duration) as avg_duration
            FROM miner_{miner_uid}_position_trades
            """
        )
        row = await cursor.fetchone()
        avg_roi = row[0] or 0.0
        avg_duration = row[1] or 0
        
        # Get win rate
        cursor = await conn.execute(
            f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN roi_tao > 0 THEN 1 ELSE 0 END) as wins
            FROM miner_{miner_uid}_position_trades
            """
        )
        row = await cursor.fetchone()
        total = row[0] or 0
        wins = row[1] or 0
        win_rate = (wins / total * 100) if total > 0 else 0.0
        
        # Get current positions
        cursor = await conn.execute(
            f"""
            SELECT COUNT(*) FROM miner_{miner_uid}_positions
            WHERE status = 'open'
            """
        )
        current_positions = (await cursor.fetchone())[0]
        
        # Update miner statistics
        await conn.execute(
            """
            UPDATE miners
            SET 
                total_trades = ?,
                total_volume_tao = ?,
                total_volume_alpha = ?,
                cumulative_roi = ?,
                avg_trade_duration = ?,
                win_rate = ?,
                current_positions = ?
            WHERE uid = ?
            """,
            (
                total_trades,
                total_tao,
                total_alpha,
                avg_roi,
                avg_duration,
                win_rate,
                current_positions,
                miner_uid
            )
        )
        await conn.commit() 