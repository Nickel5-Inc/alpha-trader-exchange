"""
Database manager for the Alpha Trade Exchange subnet.

This module provides core database management functionality including:
1. Connection management and pooling
2. Database initialization and setup
3. Session handling
4. Base table creation

Key Features
-----------
1. Singleton Pattern:
   - Ensures single database manager instance
   - Consistent connection management
   - Shared configuration across application

2. Connection Management:
   - Asynchronous SQLite operations
   - Connection pooling with SQLAlchemy
   - WAL journal mode for concurrent access

3. Session Handling:
   - SQLAlchemy async sessions
   - Transaction management
   - Automatic cleanup

Usage Example
------------
```python
# Get singleton instance
db = DatabaseManager("alpha_trade.db")

# Initialize database
await db.setup_database()

# Get a session for database operations
async with db.session() as session:
    # Perform database operations
    ...

# Close connections when done
await db.close()
```

Implementation Notes
------------------
1. Connection Management:
   - Uses SQLAlchemy async engine for connection pooling
   - Semaphore limits concurrent database access
   - Connections are automatically returned to pool

2. Error Handling:
   - Graceful handling of concurrent access
   - Proper transaction management
   - Validation of position/trade relationships

3. Performance Optimization:
   - Efficient indexing for common queries
   - Batch updates where possible
   - Minimized transaction overhead
"""

import logging
import os
from pathlib import Path
from sqlite3 import DatabaseError
from typing import Optional, List
from datetime import datetime, timezone, timedelta

import bittensor as bt
from sqlalchemy import text

from .models import (
    PerformanceConfig,
    Base,
    MinerStatus,
    Position,
    Trade,
    PositionStatus,
    PositionTrade,
    process_close_event
)
from .schema import (
    CREATE_MINER_TABLE,
    CREATE_PERFORMANCE_SNAPSHOTS_TABLE,
    CREATE_PERFORMANCE_SNAPSHOT_INDEXES,
    CREATE_ARCHIVE_TABLES,
    CREATE_ARCHIVE_INDEXES
)
from .connection import DatabaseConnection

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manager class for database operations.
    
    This class is a singleton that provides a high-level interface for database operations.
    It handles core database functionality like connection management, session handling,
    and database initialization.
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """Ensure only one instance exists."""
        if not cls._instance:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
            cls._instance._connection_initialized = False
        return cls._instance
    
    def __init__(
        self,
        db_path: str = "alpha_trade.db",
        max_connections: int = 5,
        performance_config: Optional[PerformanceConfig] = None
    ):
        """Initialize database manager if not already initialized.
        
        Args:
            db_path: Path to SQLite database file
            max_connections: Maximum number of concurrent connections
            performance_config: Configuration for performance tracking
        """
        if self._initialized:
            return
            
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        
        self.db_path = db_path
        self.performance_config = performance_config or PerformanceConfig()
        self.connection = DatabaseConnection(db_path, max_connections)
        self._initialized = True

    @property
    def session(self):
        """Get database session."""
        if not self._connection_initialized:
            raise RuntimeError("Database not initialized. Call setup_database() first.")
        return self.connection.session

    async def setup_database(self):
        """Set up database tables and indexes.
        
        This method creates the core database tables and indexes required
        by the application. It should be called once during application startup.
        """
        bt.logging.info(f"Setting up database at {self.db_path}")
        try:
            # Initialize the connection
            await self.connection.initialize()
            
            # Create core tables
            async with self.connection.get_connection() as conn:
                # Create miners table
                await conn.execute(CREATE_MINER_TABLE)
                
                # Create performance tracking tables
                await conn.execute(CREATE_PERFORMANCE_SNAPSHOTS_TABLE)
                
                for index in CREATE_PERFORMANCE_SNAPSHOT_INDEXES:
                    await conn.execute(index)
                
                # Create archive tables
                for table in CREATE_ARCHIVE_TABLES:
                    await conn.execute(table)
                
                for index in CREATE_ARCHIVE_INDEXES:
                    await conn.execute(index)
                
                # Commit changes
                await conn.commit()
                
            self._connection_initialized = True
            bt.logging.info("Database setup completed successfully")
            
        except Exception as e:
            bt.logging.error(f"Failed to set up database: {str(e)}")
            raise DatabaseError(f"Failed to set up database: {str(e)}")

    async def close(self):
        """Close database connections.
        
        This method should be called during application shutdown to ensure
        all database connections are properly closed.
        """
        await self.connection.close()
        self._connection_initialized = False

    @property
    def engine(self):
        """Get SQLAlchemy engine.
        
        Returns:
            AsyncEngine: The SQLAlchemy async engine instance
        """
        if not self._connection_initialized:
            raise RuntimeError("Database not initialized. Call setup_database() first.")
        return self.connection.engine

    @property
    def async_session(self):
        """Get async session factory.
        
        Returns:
            async_sessionmaker: Factory for creating new async sessions
        """
        if not self._connection_initialized:
            raise RuntimeError("Database not initialized. Call setup_database() first.")
        return self.connection.async_session

    async def cleanup_connections(self):
        """Clean up all database connections."""
        await self.connection.close()
        self._connection_initialized = False

    async def add_position(self, miner_uid: int, position: Position) -> int:
        """Add a position to the database.
        
        Args:
            miner_uid: Miner's UID
            position: Position to add
            
        Returns:
            int: Position ID
            
        Raises:
            ValidationError: If position is invalid
            DatabaseError: If database operation fails
        """
        try:
            async with self.connection.get_connection() as conn:
                # Check if position with this extrinsic_id already exists
                cursor = await conn.execute(
                    f"""
                    SELECT position_id FROM miner_{miner_uid}_positions
                    WHERE extrinsic_id = ?
                    """,
                    (position.extrinsic_id,)
                )
                existing_position = await cursor.fetchone()
                
                if existing_position:
                    bt.logging.info(f"Position with extrinsic_id {position.extrinsic_id} already exists. Returning existing position ID.")
                    return existing_position[0]
                
                # Insert new position
                await conn.execute(
                    f"""
                    INSERT INTO miner_{miner_uid}_positions (
                        miner_uid,
                        netuid,
                        hotkey,
                        entry_block,
                        entry_timestamp,
                        entry_tao,
                        entry_alpha,
                        remaining_alpha,
                        status,
                        extrinsic_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        miner_uid,
                        position.netuid,
                        position.hotkey,
                        position.entry_block,
                        position.entry_timestamp.isoformat(),
                        position.entry_tao,
                        position.entry_alpha,
                        position.remaining_alpha,
                        position.status.value,
                        position.extrinsic_id
                    )
                )
                
                # Get new position ID
                cursor = await conn.execute("SELECT last_insert_rowid()")
                position_id = (await cursor.fetchone())[0]
                
                # Update miner stats
                await conn.execute(
                    """
                    UPDATE miners 
                    SET total_volume_tao = total_volume_tao + ?,
                        total_volume_alpha = total_volume_alpha + ?,
                        current_positions = current_positions + 1,
                        last_active = ?
                    WHERE uid = ?
                    """,
                    (position.entry_tao, position.entry_alpha, datetime.now(timezone.utc).isoformat(), miner_uid)
                )
                
                await conn.commit()
                
                # Set position ID
                position.position_id = position_id
                
                bt.logging.info(f"Added position {position_id} for miner {miner_uid} with {position.entry_alpha} alpha")
                return position_id
                
        except Exception as e:
            bt.logging.error(f"Error adding position: {str(e)}")
            raise DatabaseError(f"Failed to add position: {str(e)}")

    async def process_close(
        self, 
        miner_uid: int, 
        trade: Trade, 
        positions: List[Position],
        min_blocks: int = 0
    ) -> List[PositionTrade]:
        """Process a close event against a list of open positions using strict FIFO.
        
        This method implements proper FIFO trading with partial position handling:
        1. First fully close any partially open positions (oldest first)
        2. Then partially close newer positions as needed
        
        Args:
            miner_uid: Miner's UID
            trade: Trade data representing the close event (StakeRemoved)
            positions: List of open positions to match against
            min_blocks: Minimum blocks required for valid trade (default 0)
            
        Returns:
            List[PositionTrade]: List of position-trade relationships created
        """
        if not positions:
            return []
        
        # Validate trade amounts - don't process trades with zero amounts
        if trade.exit_alpha <= 0 or trade.exit_tao <= 0:
            bt.logging.warning(f"Skipping trade with zero or negative amounts: alpha={trade.exit_alpha}, tao={trade.exit_tao}")
            return []
        
        # Filter positions for the same netuid as the trade
        positions = [p for p in positions if p.netuid == trade.netuid]
        
        if not positions:
            return []
        
        # Check if this is a self-trade (same extrinsic_id) and filter positions
        self_trade_positions = [p for p in positions if p.extrinsic_id == trade.extrinsic_id]
        if self_trade_positions:
            positions = self_trade_positions
        
        # Load existing partial positions
        async with self.connection.get_connection() as conn:
            cursor = await conn.execute(
                f"""
                SELECT * FROM miner_{miner_uid}_positions
                WHERE netuid = ? AND status = 'partial'
                ORDER BY entry_timestamp ASC
                """,
                (trade.netuid,)
            )
            partial_rows = await cursor.fetchall()
            
        # Create Position objects from database rows if any found
        from_db_partials = []
        for row in partial_rows:
            # Ensure timestamp is timezone-aware
            entry_timestamp = datetime.fromisoformat(row['entry_timestamp'])
            if entry_timestamp.tzinfo is None:
                entry_timestamp = entry_timestamp.replace(tzinfo=timezone.utc)
                
            pos = Position(
                position_id=row[0],
                miner_uid=row[1],
                netuid=row[2],
                hotkey=row[3],
                entry_block=row[4],
                entry_timestamp=entry_timestamp,
                entry_tao=row[6],
                entry_alpha=row[7],
                remaining_alpha=row[8],
                status=PositionStatus(row[9]),
                extrinsic_id=row[12]
            )
            from_db_partials.append(pos)
        
        # First include partial positions, then open positions sorted by entry timestamp (FIFO)
        partial_positions = from_db_partials
        partial_positions += [p for p in positions if p.status == PositionStatus.PARTIAL 
                            and p.position_id not in [x.position_id for x in partial_positions]]
        
        # Get open positions sorted by entry timestamp
        open_positions = [p for p in positions if p.status == PositionStatus.OPEN]
        open_positions.sort(key=lambda p: p.entry_timestamp)
        
        # Combine partial and open positions in the correct order (oldest partial first, then oldest open)
        ordered_positions = partial_positions + open_positions
        
        # Log positions being processed
        bt.logging.info(f"Processing trade exit_alpha={trade.exit_alpha}, exit_tao={trade.exit_tao} against {len(ordered_positions)} positions for miner {miner_uid}")
        for i, pos in enumerate(ordered_positions):
            bt.logging.info(f"Position {i+1}: id={pos.position_id}, status={pos.status}, entry_alpha={pos.entry_alpha}, remaining_alpha={pos.remaining_alpha}")
        
        # Check for self-trade case
        is_self_trade = False
        for pos in ordered_positions:
            if pos.extrinsic_id == trade.extrinsic_id:
                is_self_trade = True
                break
        
        remaining_alpha = trade.exit_alpha
        remaining_tao = trade.exit_tao
        position_trades = []
        
        async with self.connection.get_connection() as conn:
            try:
                # Insert the trade first to get trade_id
                await conn.execute(
                    f"""
                    INSERT INTO miner_{miner_uid}_trades (
                        miner_uid, netuid, hotkey, exit_block, exit_timestamp, 
                        exit_tao, exit_alpha, extrinsic_id
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        miner_uid, trade.netuid, trade.hotkey, trade.exit_block,
                        trade.exit_timestamp.isoformat(), trade.exit_tao,
                        trade.exit_alpha, trade.extrinsic_id
                    )
                )
                
                # Get the trade_id
                cursor = await conn.execute("SELECT last_insert_rowid()")
                trade_id = await cursor.fetchone()
                trade_id = trade_id[0]
                trade.trade_id = trade_id
                
                # Process positions in strict FIFO order
                for position_index, position in enumerate(ordered_positions):
                    if remaining_alpha <= 0:
                        bt.logging.info(f"No remaining alpha to allocate, stopping position processing")
                        break
                    
                    # Calculate how much alpha to close from this position
                    alpha_to_close = min(position.remaining_alpha, remaining_alpha)
                    
                    if alpha_to_close <= 0:
                        bt.logging.info(f"Position {position.position_id} has no remaining alpha, skipping")
                        continue
                    
                    bt.logging.info(f"Processing position {position.position_id}: closing {alpha_to_close} alpha out of {position.remaining_alpha} remaining")
                    
                    # HANDLE SELF-TRADES: If this position has the same extrinsic_id as the trade
                    if position.extrinsic_id == trade.extrinsic_id:
                        bt.logging.info(f"Handling self-trade for position {position.position_id}")
                        
                        # For self-trades, calculate the absolute TAO difference
                        tao_gain_loss = trade.exit_tao - position.entry_tao
                        
                        # For self-trades, ensure very small gain/loss since these are nearly simultaneous
                        if abs(tao_gain_loss) > 0.001:
                            tao_gain_loss = 0.0001 if tao_gain_loss > 0 else -0.0001
                        
                        bt.logging.info(f"Self-trade with TAO gain/loss: {tao_gain_loss}")
                        
                        position_trade = PositionTrade(
                            position_id=position.position_id,
                            trade_id=trade.trade_id,
                            alpha_amount=alpha_to_close,
                            tao_amount=trade.exit_tao,
                            roi_tao=tao_gain_loss,
                            duration=0,
                            min_blocks_met=True
                        )
                        
                        position.remaining_alpha = 0
                        position.status = PositionStatus.CLOSED
                        position.closed_at = position.entry_timestamp
                        position.final_roi = tao_gain_loss
                        
                        # Save the position trade mapping
                        await conn.execute(
                            f"""
                            INSERT INTO miner_{miner_uid}_position_trades (
                                position_id, trade_id, alpha_amount, tao_amount, roi_tao, duration, min_blocks_met
                            ) VALUES (
                                ?, ?, ?, ?, ?, ?, ?
                            )
                            """,
                            (
                                position.position_id, trade_id, alpha_to_close,
                                trade.exit_tao, tao_gain_loss, 0, True
                            )
                        )
                        
                        # Update position in database
                        await conn.execute(
                            f"""
                            UPDATE miner_{miner_uid}_positions
                            SET remaining_alpha = 0,
                                status = 'closed',
                                closed_at = ?,
                                final_roi = ?
                            WHERE position_id = ?
                            """,
                            (
                                position.entry_timestamp.isoformat(),  # Use entry time
                                tao_gain_loss,
                                position.position_id
                            )
                        )
                        
                        position_trades.append(position_trade)
                        remaining_alpha -= alpha_to_close
                        remaining_tao -= trade.exit_tao
                        
                        # For self-trades, we've handled everything specially, so continue to next position
                        continue
                    
                    # STANDARD PROCESSING FOR NON-SELF-TRADES
                    
                    # For partial close
                    position_alpha_ratio = alpha_to_close / position.entry_alpha
                    entry_tao_portion = position.entry_tao * position_alpha_ratio

                    # Calculate what fraction of the exit this position represents
                    trade_alpha_ratio = alpha_to_close / trade.exit_alpha
                    exit_tao_portion = trade.exit_tao * trade_alpha_ratio

                    # ROI is the difference
                    tao_gain_loss = exit_tao_portion - entry_tao_portion
                    
                    # Log calculation details
                    bt.logging.info(f"Position {position.position_id} close calculation: " +
                                f"Entry: {entry_tao_portion} TAO for {alpha_to_close} alpha, " +
                                f"Exit: {exit_tao_portion} TAO, Gain/Loss: {tao_gain_loss} TAO")
                    
                    # Check if minimum blocks requirement is met (always true since min_blocks=0)
                    blocks_held = trade.exit_block - position.entry_block
                    min_blocks_met = True
                    
                    # Calculate duration
                    duration = int((trade.exit_timestamp - position.entry_timestamp).total_seconds())
                    
                    # Create position trade record
                    position_trade = PositionTrade(
                        position_id=position.position_id,
                        trade_id=trade.trade_id,
                        alpha_amount=alpha_to_close,
                        tao_amount=exit_tao_portion,
                        roi_tao=tao_gain_loss,
                        duration=duration,
                        min_blocks_met=min_blocks_met
                    )
                    
                    # Save the position trade mapping
                    await conn.execute(
                        f"""
                        INSERT INTO miner_{miner_uid}_position_trades (
                            position_id, trade_id, alpha_amount, tao_amount, roi_tao, duration, min_blocks_met
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        (
                            position.position_id, trade_id, alpha_to_close,
                            exit_tao_portion, tao_gain_loss, position_trade.duration, min_blocks_met
                        )
                    )
                    
                    # Update position remaining_alpha
                    position.remaining_alpha -= alpha_to_close
                    
                    # Handle position status update - use a small epsilon to handle floating point precision issues
                    epsilon = 1e-6
                    if position.remaining_alpha <= epsilon:
                        # Position is fully closed - force to exactly zero to avoid floating point issues
                        position.remaining_alpha = 0  
                        position.status = PositionStatus.CLOSED
                        position.closed_at = trade.exit_timestamp
                        position.final_roi = tao_gain_loss
                        
                        bt.logging.info(f"Position {position.position_id} is now fully closed with TAO gain/loss of {tao_gain_loss}")
                        
                        # Update position in database - fully closed
                        await conn.execute(
                            f"""
                            UPDATE miner_{miner_uid}_positions
                            SET remaining_alpha = 0,
                                status = 'closed',
                                closed_at = ?,
                                final_roi = ?
                            WHERE position_id = ?
                            """,
                            (
                                position.closed_at.isoformat() if position.closed_at else None,
                                position.final_roi,
                                position.position_id
                            )
                        )
                    else:
                        # Position is partially closed
                        position.status = PositionStatus.PARTIAL
                        
                        bt.logging.info(f"Position {position.position_id} is partially closed: {position.remaining_alpha} alpha remaining")
                        
                        # Update position in database - partially closed
                        await conn.execute(
                            f"""
                            UPDATE miner_{miner_uid}_positions
                            SET remaining_alpha = ?,
                                status = 'partial'
                            WHERE position_id = ?
                            """,
                            (
                                position.remaining_alpha,
                                position.position_id
                            )
                        )
                    
                    position_trades.append(position_trade)
                    
                    # Update remaining amounts
                    remaining_alpha -= alpha_to_close
                    remaining_tao -= exit_tao_portion
                
                # Update miner stats
                await self._update_miner_stats_with_min_blocks(miner_uid, conn, min_blocks)
                
                # Commit all changes
                await conn.commit()
                
                # Run a final check to make sure we don't have multiple partial positions
                partial_count = await self._count_partial_positions(miner_uid, trade.netuid)
                if partial_count > 1:
                    # Force fix any duplicate partial positions
                    await self._fix_duplicate_partials(miner_uid, trade.netuid)
                
                return position_trades
                
            except Exception as e:
                bt.logging.error(f"Error processing close: {str(e)}")
                await conn.rollback()
                raise DatabaseError(f"Failed to process close: {str(e)}")

    async def recalculate_all_rois(self, miner_uid):
        """Recalculate ROIs for all closed positions of a miner."""
        bt.logging.info(f"Recalculating ROIs for miner {miner_uid}")
        
        async with self.connection.get_connection() as conn:
            # Get all position-trade mappings with corresponding position and trade data
            cursor = await conn.execute(
                f"""
                SELECT pt.position_id, pt.trade_id, pt.alpha_amount, pt.tao_amount,
                    p.entry_tao, p.entry_alpha, p.extrinsic_id as p_extrinsic,
                    t.exit_tao, t.exit_alpha, t.extrinsic_id as t_extrinsic
                FROM miner_{miner_uid}_position_trades pt
                JOIN miner_{miner_uid}_positions p ON pt.position_id = p.position_id
                JOIN miner_{miner_uid}_trades t ON pt.trade_id = t.trade_id
                """
            )
            mappings = await cursor.fetchall()
            
            for mapping in mappings:
                position_id = mapping[0]
                trade_id = mapping[1]
                alpha_amount = mapping[2]
                tao_amount = mapping[3]
                entry_tao = mapping[4]
                entry_alpha = mapping[5]
                position_extrinsic = mapping[6]
                exit_tao = mapping[7]
                exit_alpha = mapping[8]
                trade_extrinsic = mapping[9]
                
                # Check if this is a self-trade (same extrinsic_id)
                is_self_trade = position_extrinsic == trade_extrinsic
                
                if is_self_trade:
                    # For self-trades, calculate ROI directly from the raw values
                    corrected_roi = ((exit_tao - entry_tao) / entry_tao) * 100 if entry_tao > 0 else 0
                    bt.logging.info(f"Recalculating self-trade ROI for position {position_id}, trade {trade_id}: "
                                f"(({exit_tao} - {entry_tao}) / {entry_tao}) * 100 = {corrected_roi:.8f}%")
                else:
                    # Standard positions - calculate position fraction and proportional entry TAO
                    position_fraction = alpha_amount / entry_alpha if entry_alpha > 0 else 0
                    entry_tao_portion = entry_tao * position_fraction
                    
                    # Calculate corrected ROI
                    corrected_roi = ((tao_amount - entry_tao_portion) / entry_tao_portion) * 100 if entry_tao_portion > 0 else 0
                    bt.logging.info(f"Recalculating ROI for position {position_id}, trade {trade_id}: "
                                f"(({tao_amount} - {entry_tao_portion}) / {entry_tao_portion}) * 100 = {corrected_roi:.8f}%")
                
                # Update the position-trade mapping with corrected ROI
                await conn.execute(
                    f"""
                    UPDATE miner_{miner_uid}_position_trades
                    SET roi_tao = ?
                    WHERE position_id = ? AND trade_id = ?
                    """,
                    (corrected_roi, position_id, trade_id)
                )
                
                # Get all position-trade mappings for this position to calculate weighted avg ROI
                cursor = await conn.execute(
                    f"""
                    SELECT SUM(alpha_amount), SUM(roi_tao * alpha_amount) / SUM(alpha_amount)
                    FROM miner_{miner_uid}_position_trades
                    WHERE position_id = ?
                    """,
                    (position_id,)
                )
                pt_info = await cursor.fetchone()
                
                if pt_info and pt_info[0]:
                    weighted_roi = pt_info[1] if pt_info[1] is not None else 0.0
                    
                    # Update final_roi for fully closed positions
                    await conn.execute(
                        f"""
                        UPDATE miner_{miner_uid}_positions
                        SET final_roi = ?
                        WHERE position_id = ? AND status = 'closed'
                        """,
                        (weighted_roi, position_id)
                    )
            
            # Update miner stats after recalculating all ROIs
            await self._update_miner_stats_with_min_blocks(miner_uid, conn)
            
            await conn.commit()
            bt.logging.info(f"Updated ROIs for {len(mappings)} position-trade mappings for miner {miner_uid}")
            
            return len(mappings)

    async def _count_partial_positions(self, miner_uid: int, netuid: int) -> int:
        """Count the number of partial positions for a specific netuid.
        
        Args:
            miner_uid: Miner's UID
            netuid: Subnet ID
            
        Returns:
            int: Number of partial positions
        """
        async with self.connection.get_connection() as conn:
            cursor = await conn.execute(
                f"""
                SELECT COUNT(*) FROM miner_{miner_uid}_positions
                WHERE netuid = ? AND status = 'partial'
                """,
                (netuid,)
            )
            count = await cursor.fetchone()
            return count[0] if count else 0

    async def _update_miner_stats(self, miner_uid: int, conn):
        """Update miner statistics (fallback method).
        
        Args:
            miner_uid: Miner's UID
            conn: Database connection
        """
        try:
            # Count total trades
            cursor = await conn.execute(
                f"""
                SELECT COUNT(*) FROM miner_{miner_uid}_trades
                """
            )
            total_trades = await cursor.fetchone()
            total_trades = total_trades[0] if total_trades else 0

            # Calculate total volume
            cursor = await conn.execute(
                f"""
                SELECT 
                    SUM(entry_tao) as total_tao,
                    SUM(entry_alpha) as total_alpha
                FROM miner_{miner_uid}_positions
                """
            )
            volume = await cursor.fetchone()
            total_tao = volume[0] if volume and volume[0] else 0.0
            total_alpha = volume[1] if volume and volume[1] else 0.0

            # Calculate ROI stats
            cursor = await conn.execute(
                f"""
                SELECT 
                    AVG(final_roi) as avg_roi,
                    SUM(CASE WHEN final_roi > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
                    AVG(CAST(strftime('%s', closed_at) - strftime('%s', entry_timestamp) AS INTEGER)) as avg_duration
                FROM miner_{miner_uid}_positions
                WHERE status = 'closed' AND closed_at IS NOT NULL
                """
            )
            roi_stats = await cursor.fetchone()
            
            cumulative_roi = roi_stats[0] if roi_stats and roi_stats[0] is not None else 0.0
            win_rate = roi_stats[1] if roi_stats and roi_stats[1] is not None else 0.0
            avg_duration = roi_stats[2] if roi_stats and roi_stats[2] is not None else 0

            # Count current open positions
            cursor = await conn.execute(
                f"""
                SELECT COUNT(*) FROM miner_{miner_uid}_positions
                WHERE status IN ('open', 'partial')
                """
            )
            current_positions = await cursor.fetchone()
            current_positions = current_positions[0] if current_positions else 0

            # Update miner stats
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
                    current_positions = ?,
                    last_active = ?
                WHERE uid = ?
                """,
                (
                    total_trades,
                    total_tao,
                    total_alpha,
                    cumulative_roi,
                    avg_duration,
                    win_rate,
                    current_positions,
                    datetime.now(timezone.utc).isoformat(),
                    miner_uid
                )
            )

            bt.logging.info(
                f"Updated miner {miner_uid} stats: trades={total_trades}, tao={total_tao:.2f}, "
                f"alpha={total_alpha:.2f}, roi={cumulative_roi:.2f}%, win_rate={win_rate:.2f}%, "
                f"positions={current_positions}"
            )
        except Exception as e:
            bt.logging.error(f"Error updating miner stats: {str(e)}")

    async def _update_miner_stats_with_min_blocks(self, miner_uid: int, conn, min_blocks: int = 0):
        """Update miner statistics with consideration for minimum blocks requirement.
        
        This method calculates accurate ROI and other statistics based on position trades.
        
        Args:
            miner_uid: Miner's UID
            conn: Database connection
            min_blocks: Minimum blocks required for valid trade (default 0)
        """
        try:
            # Count total trades
            cursor = await conn.execute(
                f"""
                SELECT COUNT(*) FROM miner_{miner_uid}_trades
                """
            )
            total_trades = await cursor.fetchone()
            total_trades = total_trades[0] if total_trades else 0

            # Calculate total volume
            cursor = await conn.execute(
                f"""
                SELECT 
                    SUM(entry_tao) as total_tao,
                    SUM(entry_alpha) as total_alpha
                FROM miner_{miner_uid}_positions
                """
            )
            volume = await cursor.fetchone()
            total_tao = volume[0] if volume and volume[0] else 0.0
            total_alpha = volume[1] if volume and volume[1] else 0.0

            # Calculate total TAO gain/loss from all position trades
            cursor = await conn.execute(
                f"""
                SELECT 
                    SUM(pt.roi_tao) as total_tao_gain_loss,
                    COUNT(CASE WHEN pt.roi_tao > 0 THEN 1 END) as profitable_trades,
                    COUNT(*) as total_position_trades,
                    AVG(pt.duration) as avg_duration
                FROM miner_{miner_uid}_position_trades pt
                WHERE pt.min_blocks_met = 1
                """
            )
            roi_stats = await cursor.fetchone()
            
            # Get net TAO gain/loss (not percentage ROI)
            total_tao_gain_loss = roi_stats[0] if roi_stats and roi_stats[0] is not None else 0.0
            profitable_trades = roi_stats[1] if roi_stats and roi_stats[1] is not None else 0
            total_position_trades = roi_stats[2] if roi_stats and roi_stats[2] is not None else 0
            
            # Calculate win rate
            win_rate = (profitable_trades / total_position_trades * 100) if total_position_trades > 0 else 0.0
            
            # Average trade duration
            avg_duration = roi_stats[3] if roi_stats and roi_stats[3] is not None else 0

            # Count current open and partial positions
            cursor = await conn.execute(
                f"""
                SELECT COUNT(*) FROM miner_{miner_uid}_positions
                WHERE status IN ('open', 'partial')
                """
            )
            current_positions = await cursor.fetchone()
            current_positions = current_positions[0] if current_positions else 0

            # Update miner stats
            await conn.execute(
                """
                UPDATE miners
                SET 
                    total_trades = ?,
                    total_volume_tao = ?,
                    total_volume_alpha = ?,
                    cumulative_roi = ?,  -- Now stores total TAO gain/loss
                    avg_trade_duration = ?,
                    win_rate = ?,
                    current_positions = ?,
                    last_active = ?
                WHERE uid = ?
                """,
                (
                    total_trades,
                    total_tao,
                    total_alpha,
                    total_tao_gain_loss,
                    avg_duration,
                    win_rate,
                    current_positions,
                    datetime.now(timezone.utc).isoformat(),
                    miner_uid
                )
            )

            bt.logging.info(
                f"Updated miner {miner_uid} stats: trades={total_trades}, tao={total_tao:.2f}, "
                f"alpha={total_alpha:.2f}, net_tao_gain_loss={total_tao_gain_loss:.6f}, win_rate={win_rate:.2f}%, "
                f"positions={current_positions}"
            )
        except Exception as e:
            bt.logging.error(f"Error updating miner stats: {str(e)}")

    async def take_performance_snapshot(
        self,
        miner_uid: int,
        timestamp: datetime
    ) -> None:
        """Take a performance snapshot.
        
        Args:
            miner_uid: Miner's UID
            timestamp: Snapshot timestamp
            
        Raises:
            DatabaseError: If database operation fails
        """
        async with self.connection.session() as session:
            async with session.begin():
                # Get miner hotkey (retain for column value, if needed)
                result = await session.execute(
                    text("SELECT hotkey FROM miners WHERE uid = :uid"),
                    {"uid": miner_uid}
                )
                row = result.fetchone()
                if not row:
                    raise DatabaseError(f"Miner {miner_uid} not found")
                fetched_hotkey = row[0]
                
                # Get window start/end parameters (assumed to be set in a variable 'parameters')
                # Previously the query used the hotkey in the table name:
                # text(f"""
                #     SELECT COUNT(*) as total_trades,
                #            SUM(entry_tao) as total_volume_tao,
                #            ...
                #     FROM miner_{fetched_hotkey}_positions
                #     WHERE miner_uid = :miner_uid
                #       AND entry_timestamp BETWEEN :window_start AND :window_end
                # """)
                
                # Update: use miner_uid for the table name instead
                result = await session.execute(
                    text(f"""
                    SELECT COUNT(*) as total_trades,
                           SUM(entry_tao) as total_volume_tao,
                           SUM(entry_alpha) as total_volume_alpha,
                           AVG(final_roi) as roi_simple,
                           SUM(CASE WHEN final_roi > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
                           AVG(CAST(strftime('%s', closed_at) - strftime('%s', entry_timestamp) AS INTEGER)) as avg_duration,
                           COUNT(CASE WHEN status != 'closed' THEN 1 END) as open_positions,
                           AVG(CAST(strftime('%s', :current_time) - strftime('%s', entry_timestamp) AS INTEGER)) as avg_position_age
                    FROM miner_{miner_uid}_positions
                    WHERE miner_uid = :miner_uid
                      AND entry_timestamp BETWEEN :window_start AND :window_end
                    """),
                    {
                        "miner_uid": miner_uid,
                        "window_start": timestamp - timedelta(days=1),#self.performance_config.rolling_window_days),
                        "window_end": timestamp,
                        "current_time": timestamp
                    }
                )
                snapshot_data = result.fetchone()
                
                # Convert tuple to dict
                metrics_dict = {
                    'total_trades': snapshot_data[0] or 0,
                    'total_volume_tao': snapshot_data[1] or 0.0,
                    'total_volume_alpha': snapshot_data[2] or 0.0,
                    'roi_simple': snapshot_data[3] or 0.0,
                    'win_rate': snapshot_data[4] or 0.0,
                    'avg_duration': snapshot_data[5] or 0,
                    'open_positions': snapshot_data[6] or 0,
                    'avg_position_age': snapshot_data[7] or 0
                }
                
                # Calculate weighted ROI
                result = await session.execute(
                    text(f"""
                    SELECT 
                        final_roi,
                        entry_tao
                    FROM miner_{miner_uid}_positions
                    WHERE miner_uid = :miner_uid
                    AND entry_timestamp BETWEEN :window_start AND :window_end
                    AND status = 'closed'
                    """),
                    {
                        "miner_uid": miner_uid,
                        "window_start": timestamp - timedelta(1),#self.performance_config.rolling_window_days),
                        "window_end": timestamp
                    }
                )
                trades = result.fetchall()
                
                total_tao = sum(trade[1] for trade in trades)
                roi_weighted = (
                    sum(trade[0] * trade[1] for trade in trades) / total_tao
                    if total_tao > 0 else 0.0
                )
                
                # Calculate final score
                final_score = None
                if metrics_dict['total_trades'] >= self.performance_config.min_trades_for_scoring:
                    final_score = (
                        roi_weighted * self.performance_config.roi_weight +
                        metrics_dict['total_volume_tao'] * self.performance_config.volume_weight +
                        metrics_dict['win_rate'] * self.performance_config.win_rate_weight
                    )
                
                # Save snapshot
                await session.execute(
                    text("""
                    INSERT INTO performance_snapshots (
                        miner_uid,
                        timestamp,
                        window_start,
                        window_end,
                        total_trades,
                        total_volume_tao,
                        total_volume_alpha,
                        roi_simple,
                        roi_weighted,
                        win_rate,
                        avg_trade_duration,
                        open_positions,
                        avg_position_age,
                        final_score
                    ) VALUES (
                        :miner_uid,
                        :timestamp,
                        :window_start,
                        :window_end,
                        :total_trades,
                        :total_volume_tao,
                        :total_volume_alpha,
                        :roi_simple,
                        :roi_weighted,
                        :win_rate,
                        :avg_trade_duration,
                        :open_positions,
                        :avg_position_age,
                        :final_score
                    )
                    """),
                    {
                        "miner_uid": miner_uid,
                        "timestamp": timestamp,
                        "window_start": timestamp - timedelta(days=1),#self.performance_config.rolling_window_days),
                        "window_end": timestamp,
                        "total_trades": metrics_dict['total_trades'],
                        "total_volume_tao": metrics_dict['total_volume_tao'],
                        "total_volume_alpha": metrics_dict['total_volume_alpha'],
                        "roi_simple": metrics_dict['roi_simple'],
                        "roi_weighted": roi_weighted,
                        "win_rate": metrics_dict['win_rate'],
                        "avg_trade_duration": metrics_dict['avg_duration'],
                        "open_positions": metrics_dict['open_positions'],
                        "avg_position_age": metrics_dict['avg_position_age'],
                        "final_score": final_score
                    }
                )

    async def cleanup_old_snapshots(self, cutoff_date: datetime) -> None:
        """Clean up old performance snapshots.
        
        Args:
            cutoff_date: Delete snapshots older than this date
            
        Raises:
            DatabaseError: If cleanup fails
        """
        async with self.connection.session() as session:
            async with session.begin():
                try:
                    # Delete old snapshots
                    await session.execute(
                        text("""
                        DELETE FROM performance_snapshots 
                        WHERE timestamp < :cutoff_date
                        """),
                        {"cutoff_date": cutoff_date}
                    )
                except Exception as e:
                    raise DatabaseError(f"Failed to clean up old snapshots: {str(e)}")