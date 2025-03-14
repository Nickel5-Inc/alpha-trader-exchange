"""
Pipeline for processing stake/unstake events and updating the database.

This module provides an asynchronous pipeline for processing stake and unstake events
from the blockchain and updating the database accordingly. It handles the complex logic
of maintaining FIFO accounting while supporting concurrent processing of multiple miners.

Key Components
------------
1. Event Processing:
   - Filters events based on tracking start date
   - Handles both stake (position open) and unstake (position close) events
   - Maintains FIFO order for position tracking

2. Miner Management:
   - Automatic miner registration
   - Table creation for new miners
   - Statistics updates after each event

3. Concurrent Processing:
   - Supports batch processing of multiple miners
   - Controlled concurrency with semaphores
   - Safe database access through connection pooling

Event Flow Example
---------------
```python
# Initialize pipeline
pipeline = AsyncUpdatePipeline(db_manager)

# Process events for a single miner
events = [
    StakeEvent(action=StakeAction.STAKE_ADDED, alpha=10, tao=100),
    StakeEvent(action=StakeAction.STAKE_REMOVED, alpha=5, tao=60)
]
await pipeline.process_events(coldkey, events, current_block)

# Process multiple miners concurrently
events_by_miner = {
    "miner1": events1,
    "miner2": events2
}
results = await pipeline.sync_all_miners(events_by_miner, current_block)
```

Implementation Notes
-----------------
1. Event Ordering:
   - Events are sorted by block number
   - Only events after tracking_start_date are processed
   - FIFO order is maintained for position closes

2. Error Handling:
   - Graceful handling of missing miners
   - Validation of event sequences
   - Transaction rollback on errors

3. Performance:
   - Batch processing where possible
   - Controlled concurrent processing
   - Efficient database operations
"""

from datetime import datetime
from typing import List, Optional, Dict, Set
import asyncio

import aiosqlite

from alphatrade.database.exceptions import DatabaseError

from .manager import DatabaseManager
from .models import (
    Miner, Position, Trade, StakeEvent, StakeAction,
    PositionStatus, MinerStatus, PositionTrade
)

class AsyncUpdatePipeline:
    """Pipeline for processing stake/unstake events and updating the database.
    
    This class provides a high-level interface for processing blockchain events
    and maintaining the miner position database. It handles:
    
    1. Event Processing:
       - Filtering events by tracking start date
       - Converting stake events to positions
       - Converting unstake events to trades
       - Maintaining FIFO accounting
    
    2. Database Updates:
       - Creating new positions
       - Processing position closes
       - Updating miner statistics
       - Managing position-trade mappings
    
    3. Concurrent Operations:
       - Batch processing of miners
       - Controlled concurrent access
       - Safe database operations
    
    The pipeline ensures that all events are processed in the correct order
    while maintaining data consistency and FIFO accounting rules.
    
    Example Usage:
    ```python
    # Initialize pipeline
    pipeline = AsyncUpdatePipeline(db_manager)
    
    # Process new events
    await pipeline.process_events(
        coldkey="0x123...",
        events=[stake_event1, unstake_event1],
        current_block=1000
    )
    
    # Sync multiple miners
    results = await pipeline.sync_all_miners(
        events_by_coldkey={
            "0x123...": events1,
            "0x456...": events2
        },
        current_block=1000
    )
    ```
    
    Note:
        The pipeline assumes events are provided in chronological order.
        Events before a miner's tracking_start_date are automatically filtered out.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize the pipeline.
        
        Args:
            db_manager (DatabaseManager): Database manager instance
        """
        self.db = db_manager

    async def process_events(
        self,
        coldkey: str,
        events: List[StakeEvent],
        current_block: int
    ) -> bool:
        """Process a list of stake/unstake events for a miner.
        
        Args:
            coldkey (str): Miner's coldkey
            events (List[StakeEvent]): List of events to process
            current_block (int): Current block number for updating last_active
            
        Returns:
            bool: True if processing succeeded
            
        Note:
            Events should be sorted by block number in ascending order
        """
        async with self.db.get_connection() as conn:
            # Get miner
            cursor = await conn.execute(
                "SELECT * FROM miners WHERE coldkey = ?",
                (coldkey,)
            )
            row = await cursor.fetchone()
            if not row:
                return False
                
            miner = Miner(
                uid=row[0],
                coldkey=row[1],
                registration_date=datetime.fromisoformat(row[2]),
                last_active=datetime.fromisoformat(row[3]),
                status=MinerStatus(row[4]),
                total_trades=row[5],
                total_volume_tao=row[6],
                total_volume_alpha=row[7],
                cumulative_roi=row[8],
                avg_trade_duration=row[9],
                win_rate=row[10],
                current_positions=row[11],
                tracking_start_date=datetime.fromisoformat(row[12])
            )
            
            # Skip events before tracking start date
            valid_events = [
                e for e in events 
                if e.timestamp >= miner.tracking_start_date
            ]
            
            if not valid_events:
                return True
                
            # Process events by type
            for event in valid_events:
                if event.action == StakeAction.STAKE_ADDED:
                    await self._handle_stake(conn, miner.uid, event)
                elif event.action == StakeAction.STAKE_REMOVED:
                    await self._handle_unstake(conn, miner.uid, event)
            
            await conn.commit()
            return True

    async def _handle_stake(
        self,
        conn: aiosqlite.Connection,
        miner_uid: int,
        event: StakeEvent
    ):
        """Handle a stake (position open) event.
        
        Args:
            conn (aiosqlite.Connection): Database connection
            miner_uid (int): Miner's UID
            event (StakeEvent): Stake event to process
        """
        # Insert position
        cursor = await conn.execute(
            f"""
            INSERT INTO miner_{miner_uid}_positions (
                netuid, hotkey, entry_block, entry_timestamp,
                entry_tao, entry_alpha, remaining_alpha, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.netuid, event.hotkey,
                event.block_number, event.timestamp,
                event.tao_amount, event.alpha_amount,
                event.alpha_amount, PositionStatus.OPEN.value
            )
        )
        
        # Update miner stats
        await self._update_miner_stats(conn, miner_uid)

    async def _handle_unstake(
        self,
        conn: aiosqlite.Connection,
        miner_uid: int,
        event: StakeEvent
    ):
        """Handle an unstake (position close) event.
        
        Args:
            conn (aiosqlite.Connection): Database connection
            miner_uid (int): Miner's UID
            event (StakeEvent): Unstake event to process
        """
        # Insert trade record
        cursor = await conn.execute(
            f"""
            INSERT INTO miner_{miner_uid}_trades (
                netuid, hotkey, exit_block, exit_timestamp,
                exit_tao, exit_alpha
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.netuid, event.hotkey,
                event.block_number, event.timestamp,
                event.tao_amount, event.alpha_amount
            )
        )
        trade_id = cursor.lastrowid
        
        # Get open positions
        cursor = await conn.execute(
            f"""
            SELECT * FROM miner_{miner_uid}_positions
            WHERE status != ? 
            AND remaining_alpha > 0
            AND netuid = ?
            ORDER BY entry_timestamp ASC
            """,
            (PositionStatus.CLOSED.value, event.netuid)
        )
        rows = await cursor.fetchall()
        
        positions = [
            Position(
                position_id=row[0],
                netuid=row[1],
                hotkey=row[2],
                entry_block=row[3],
                entry_timestamp=datetime.fromisoformat(row[4]),
                entry_tao=row[5],
                entry_alpha=row[6],
                remaining_alpha=row[7],
                status=PositionStatus(row[8])
            )
            for row in rows
        ]
        
        # Check if we have enough alpha to close
        total_available = sum(p.remaining_alpha for p in positions)
        if total_available < event.alpha_amount:
            print(
                f"Warning: Not enough open positions ({total_available} alpha) "
                f"to cover unstake ({event.alpha_amount} alpha)"
            )
            return
            
        # Process positions in FIFO order
        remaining_alpha = event.alpha_amount
        remaining_tao = event.tao_amount
        
        for position in positions:
            if remaining_alpha <= 0:
                break
                
            # Calculate portion to close
            alpha_to_close = min(position.remaining_alpha, remaining_alpha)
            tao_portion = (alpha_to_close / event.alpha_amount) * event.tao_amount
            
            # Update position
            new_remaining = position.remaining_alpha - alpha_to_close
            new_status = (
                PositionStatus.CLOSED.value if new_remaining <= 0
                else PositionStatus.PARTIAL.value
            )
            
            await conn.execute(
                f"""
                UPDATE miner_{miner_uid}_positions
                SET remaining_alpha = ?, status = ?
                WHERE position_id = ?
                """,
                (new_remaining, new_status, position.position_id)
            )
            
            # Calculate ROI and duration
            entry_tao_portion = position.entry_tao * (alpha_to_close / position.entry_alpha)
            roi = ((tao_portion - entry_tao_portion) / entry_tao_portion) * 100
            duration = int((event.timestamp - position.entry_timestamp).total_seconds())
            
            # Insert position-trade mapping
            await conn.execute(
                f"""
                INSERT INTO miner_{miner_uid}_position_trades (
                    position_id, trade_id, alpha_amount,
                    tao_amount, roi_tao, duration
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    position.position_id, trade_id,
                    alpha_to_close, tao_portion,
                    roi, duration
                )
            )
            
            remaining_alpha -= alpha_to_close
            remaining_tao -= tao_portion
        
        # Update miner stats
        await self._update_miner_stats(conn, miner_uid)

    async def _update_miner_stats(
        self,
        conn: aiosqlite.Connection,
        miner_uid: int
    ):
        """Update a miner's statistics based on their trades.
        
        Args:
            conn (aiosqlite.Connection): Database connection
            miner_uid (int): Miner's UID
        """
        cursor = await conn.execute(
            f"""
            SELECT 
                COUNT(DISTINCT t.trade_id) as total_trades,
                SUM(t.exit_tao) as total_volume_tao,
                SUM(t.exit_alpha) as total_volume_alpha,
                AVG(pt.roi_tao) as avg_roi,
                AVG(pt.duration) as avg_duration,
                COUNT(CASE WHEN pt.roi_tao > 0 THEN 1 END) * 100.0 / COUNT(*) as win_rate,
                COUNT(DISTINCT CASE WHEN p.status != 'closed' THEN p.position_id END) as current_positions
            FROM miner_{miner_uid}_trades t
            LEFT JOIN miner_{miner_uid}_position_trades pt ON t.trade_id = pt.trade_id
            LEFT JOIN miner_{miner_uid}_positions p ON p.position_id = pt.position_id
            """
        )
        stats = await cursor.fetchone()
        
        await conn.execute(
            """
            UPDATE miners
            SET total_trades = ?,
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
                stats[0] or 0,
                stats[1] or 0.0,
                stats[2] or 0.0,
                stats[3] or 0.0,
                int(stats[4] or 0),
                stats[5] or 0.0,
                stats[6] or 0,
                datetime.utcnow(),
                miner_uid
            )
        )

    async def sync_miner(
        self,
        coldkey: str,
        all_events: List[StakeEvent],
        current_block: int
    ) -> bool:
        """Sync a miner's data with the latest events.
        
        This method handles the complete sync process:
        1. Registers miner if needed
        2. Determines initial state (positions that existed before tracking)
        3. Processes all new events
        
        Args:
            coldkey (str): Miner's coldkey
            all_events (List[StakeEvent]): Complete list of historical events
            current_block (int): Current block number
            
        Returns:
            bool: True if sync succeeded
        """
        # Sort events by block number
        events = sorted(all_events, key=lambda e: e.block_number)
        
        async with self.db.get_connection() as conn:
            # Get or create miner
            cursor = await conn.execute(
                "SELECT * FROM miners WHERE coldkey = ?",
                (coldkey,)
            )
            row = await cursor.fetchone()
            
            if not row:
                # Find first stake event
                first_stake = next(
                    (e for e in events if e.action == StakeAction.STAKE_ADDED),
                    None
                )
                if not first_stake:
                    return False
                    
                # Register miner
                cursor = await conn.execute(
                    """
                    INSERT INTO miners (
                        coldkey, registration_date, last_active,
                        status, tracking_start_date
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        coldkey,
                        datetime.utcnow(),
                        datetime.utcnow(),
                        MinerStatus.ACTIVE.value,
                        first_stake.timestamp
                    )
                )
                miner_uid = cursor.lastrowid
                
                # Create miner tables
                await self.db.create_miner_tables(miner_uid)
                await conn.commit()
            
        # Process events
        return await self.process_events(coldkey, events, current_block)

    async def sync_all_miners(
        self,
        events_by_coldkey: Dict[str, List[StakeEvent]],
        current_block: int,
        max_concurrent: int = 5
    ) -> Dict[str, bool]:
        """Sync multiple miners in batch.
        
        Args:
            events_by_coldkey (Dict[str, List[StakeEvent]]): Events grouped by coldkey
            current_block (int): Current block number
            max_concurrent (int): Maximum number of miners to sync concurrently
            
        Returns:
            Dict[str, bool]: Success status for each miner
        """
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def _sync_one(coldkey: str, events: List[StakeEvent]):
            async with semaphore:
                try:
                    success = await self.sync_miner(coldkey, events, current_block)
                    results[coldkey] = success
                except Exception as e:
                    print(f"Error syncing miner {coldkey}: {str(e)}")
                    results[coldkey] = False
        
        # Create tasks for all miners
        tasks = [
            _sync_one(coldkey, events)
            for coldkey, events in events_by_coldkey.items()
        ]
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
        return results 

    async def process_out_of_order_events(
        self,
        coldkey: str,
        events: List[StakeEvent],
        current_block: int
    ) -> bool:
        """Process events that may be out of order.
        
        This method:
        1. Sequences events by block number and timestamp
        2. Validates event dependencies
        3. Handles missing intermediate events
        4. Processes events in correct order
        
        Args:
            coldkey (str): Miner's coldkey
            events (List[StakeEvent]): Potentially out-of-order events
            current_block (int): Current block number
            
        Returns:
            bool: True if processing succeeded
        """
        async with self.db.get_connection() as conn:
            try:
                # Start transaction
                await conn.execute("BEGIN IMMEDIATE")
                
                # Get miner info
                cursor = await conn.execute(
                    "SELECT uid, tracking_start_date FROM miners WHERE coldkey = ?",
                    (coldkey,)
                )
                row = await cursor.fetchone()
                if not row:
                    return False
                    
                miner_uid, tracking_start = row
                tracking_start = datetime.fromisoformat(tracking_start)
                
                # Sort events by block number and timestamp
                sorted_events = sorted(
                    events,
                    key=lambda e: (e.block_number, e.timestamp)
                )
                
                # Filter events before tracking start
                valid_events = [
                    e for e in sorted_events
                    if e.timestamp >= tracking_start
                ]
                
                # Group events by subnet for dependency tracking
                subnet_events = {}
                for event in valid_events:
                    if event.netuid not in subnet_events:
                        subnet_events[event.netuid] = []
                    subnet_events[event.netuid].append(event)
                
                # Process each subnet's events
                for netuid, net_events in subnet_events.items():
                    # Get current subnet state
                    cursor = await conn.execute(
                        f"""
                        SELECT SUM(remaining_alpha) as total_alpha
                        FROM miner_{miner_uid}_positions
                        WHERE netuid = ? AND status != 'closed'
                        """,
                        (netuid,)
                    )
                    row = await cursor.fetchone()
                    current_alpha = float(row[0] or 0)
                    
                    # Track running alpha balance
                    alpha_balance = current_alpha
                    
                    # Process events in order
                    for event in net_events:
                        if event.action == StakeAction.STAKE_ADDED:
                            # Add position
                            alpha_balance += event.alpha_amount
                            await self._handle_stake(conn, miner_uid, event)
                            
                        elif event.action == StakeAction.STAKE_REMOVED:
                            # Validate sufficient alpha
                            if alpha_balance < event.alpha_amount:
                                print(
                                    f"Warning: Insufficient alpha in subnet {netuid} "
                                    f"({alpha_balance} < {event.alpha_amount})"
                                )
                                continue
                                
                            alpha_balance -= event.alpha_amount
                            await self._handle_unstake(conn, miner_uid, event)
                
                # Update miner stats
                await self.db._update_miner_stats(conn, miner_uid)
                
                # Commit all changes
                await conn.commit()
                return True
                
            except Exception as e:
                await conn.rollback()
                raise DatabaseError(f"Failed to process out-of-order events: {str(e)}") 

    async def reconcile_positions(
        self,
        coldkey: str,
        chain_positions: Dict[int, float],  # netuid -> alpha_balance
        current_block: int
    ) -> Dict[str, int]:
        """Reconcile database positions with chain state.
        
        This method:
        1. Compares database positions with chain state
        2. Fixes any discrepancies found
        3. Updates position states accordingly
        4. Returns count of fixes made
        
        Args:
            coldkey (str): Miner's coldkey
            chain_positions (Dict[int, float]): Current position balances from chain
            current_block (int): Current block number
            
        Returns:
            Dict[str, int]: Count of fixes by type
        """
        fixes = {
            'missing_positions': 0,
            'extra_positions': 0,
            'balance_mismatches': 0
        }
        
        async with self.db.get_connection() as conn:
            try:
                # Start transaction
                await conn.execute("BEGIN IMMEDIATE")
                
                # Get miner info
                cursor = await conn.execute(
                    "SELECT uid FROM miners WHERE coldkey = ?",
                    (coldkey,)
                )
                row = await cursor.fetchone()
                if not row:
                    return fixes
                    
                miner_uid = row[0]
                
                # Get current database positions by subnet
                cursor = await conn.execute(
                    f"""
                    SELECT netuid, SUM(remaining_alpha) as total_alpha
                    FROM miner_{miner_uid}_positions
                    WHERE status != 'closed'
                    GROUP BY netuid
                    """
                )
                db_positions = {
                    row[0]: float(row[1] or 0)
                    async for row in cursor
                }
                
                # Find missing positions (in chain but not db)
                for netuid, chain_alpha in chain_positions.items():
                    if netuid not in db_positions:
                        # Create missing position
                        await conn.execute(
                            f"""
                            INSERT INTO miner_{miner_uid}_positions (
                                netuid, hotkey, entry_block, entry_timestamp,
                                entry_tao, entry_alpha, remaining_alpha, status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                netuid, "unknown",  # Placeholder hotkey
                                current_block, datetime.utcnow(),
                                0.0, chain_alpha,  # Unknown entry price
                                chain_alpha, PositionStatus.OPEN.value
                            )
                        )
                        fixes['missing_positions'] += 1
                        continue
                        
                    # Check for balance mismatch
                    db_alpha = db_positions[netuid]
                    if abs(db_alpha - chain_alpha) > 1e-8:  # Allow small float error
                        # Find positions to adjust
                        cursor = await conn.execute(
                            f"""
                            SELECT position_id, remaining_alpha
                            FROM miner_{miner_uid}_positions
                            WHERE netuid = ? AND status != 'closed'
                            ORDER BY entry_timestamp DESC
                            """,
                            (netuid,)
                        )
                        positions = [(row[0], float(row[1])) async for row in cursor]
                        
                        # Adjust newest position to match chain state
                        if positions:
                            newest_id = positions[0][0]
                            adjustment = chain_alpha - db_alpha
                            
                            await conn.execute(
                                f"""
                                UPDATE miner_{miner_uid}_positions
                                SET remaining_alpha = remaining_alpha + ?
                                WHERE position_id = ?
                                """,
                                (adjustment, newest_id)
                            )
                            fixes['balance_mismatches'] += 1
                
                # Find extra positions (in db but not chain)
                for netuid in db_positions:
                    if netuid not in chain_positions:
                        # Close extra positions
                        await conn.execute(
                            f"""
                            UPDATE miner_{miner_uid}_positions
                            SET remaining_alpha = 0, status = ?
                            WHERE netuid = ? AND status != 'closed'
                            """,
                            (PositionStatus.CLOSED.value, netuid)
                        )
                        fixes['extra_positions'] += 1
                
                # Update miner stats
                await self.db._update_miner_stats(conn, miner_uid)
                
                # Commit changes
                await conn.commit()
                return fixes
                
            except Exception as e:
                await conn.rollback()
                raise DatabaseError(f"Failed to reconcile positions: {str(e)}") 

    async def verify_trade(
        self,
        trade: Trade,
        chain_data: Dict[str, float],
        current_block: int
    ) -> Dict[str, bool]:
        """Verify a trade against chain data.
        
        This method:
        1. Verifies trade amounts match chain data
        2. Validates trade timing and block numbers
        3. Checks trade against pool state
        4. Ensures trade is within valid price range
        
        Args:
            trade (Trade): Trade to verify
            chain_data (Dict[str, float]): Chain data including:
                - block_number: Block where trade occurred
                - timestamp: Trade timestamp
                - tao_amount: TAO amount from chain
                - alpha_amount: Alpha amount from chain
                - pool_tao: Pool TAO reserve
                - pool_alpha: Pool alpha reserve
            current_block (int): Current block number
            
        Returns:
            Dict[str, bool]: Verification results by check type
        """
        results = {
            'amounts_match': False,
            'timing_valid': False,
            'price_valid': False,
            'pool_state_valid': False
        }
        
        # Verify amounts match chain data
        tao_diff = abs(trade.exit_tao - chain_data['tao_amount'])
        alpha_diff = abs(trade.exit_alpha - chain_data['alpha_amount'])
        results['amounts_match'] = (
            tao_diff < 1e-8 and  # Allow small float errors
            alpha_diff < 1e-8
        )
        
        # Verify timing
        results['timing_valid'] = (
            trade.exit_block == chain_data['block_number'] and
            abs((trade.exit_timestamp - chain_data['timestamp']).total_seconds()) < 60 and
            trade.exit_block <= current_block
        )
        
        # Calculate and verify price
        if chain_data['alpha_amount'] > 0:
            trade_price = trade.exit_tao / trade.exit_alpha
            pool_price = chain_data['pool_tao'] / chain_data['pool_alpha']
            
            # Allow 1% price deviation from pool price
            price_deviation = abs(trade_price - pool_price) / pool_price
            results['price_valid'] = price_deviation <= 0.01
        
        # Verify against pool state
        results['pool_state_valid'] = (
            trade.exit_tao <= chain_data['pool_tao'] and
            trade.exit_alpha <= chain_data['pool_alpha']
        )
        
        return results

    async def verify_and_process_trade(
        self,
        miner_uid: int,
        trade: Trade,
        chain_data: Dict[str, float],
        current_block: int
    ) -> Optional[List[PositionTrade]]:
        """Verify and process a trade, handling any verification failures.
        
        Args:
            miner_uid (int): Miner's UID
            trade (Trade): Trade to verify and process
            chain_data (Dict[str, float]): Chain data for verification
            current_block (int): Current block number
            
        Returns:
            Optional[List[PositionTrade]]: Position-trade mappings if successful
        """
        # Verify trade
        verification = await self.verify_trade(trade, chain_data, current_block)
        
        # Check if any verification failed
        if not all(verification.values()):
            failed_checks = [
                check for check, passed in verification.items()
                if not passed
            ]
            print(f"Trade verification failed: {failed_checks}")
            
            # Handle specific verification failures
            if not verification['amounts_match']:
                # Update trade amounts to match chain
                trade.exit_tao = chain_data['tao_amount']
                trade.exit_alpha = chain_data['alpha_amount']
            
            if not verification['timing_valid']:
                # Update trade timing
                trade.exit_block = chain_data['block_number']
                trade.exit_timestamp = chain_data['timestamp']
            
            if not verification['price_valid'] or not verification['pool_state_valid']:
                # Trade is invalid - don't process
                return None
        
        # Process verified trade
        try:
            return await self.db.process_close(miner_uid, trade)
        except Exception as e:
            print(f"Failed to process verified trade: {str(e)}")
            return None 

    async def handle_failed_transaction(
        self,
        coldkey: str,
        operation_type: str,
        failed_event: StakeEvent,
        last_known_good_state: Dict,
        current_block: int
    ) -> bool:
        """Handle a failed transaction by rolling back to last known good state.
        
        This method:
        1. Identifies the type of failure
        2. Rolls back affected database state
        3. Restores last known good state
        4. Updates miner statistics
        
        Args:
            coldkey (str): Miner's coldkey
            operation_type (str): Type of failed operation ('stake' or 'unstake')
            failed_event (StakeEvent): Event that failed to process
            last_known_good_state (Dict): State before operation started
            current_block (int): Current block number
            
        Returns:
            bool: True if recovery succeeded
        """
        async with self.db.get_connection() as conn:
            try:
                # Start recovery transaction
                await conn.execute("BEGIN IMMEDIATE")
                
                # Get miner info
                cursor = await conn.execute(
                    "SELECT uid FROM miners WHERE coldkey = ?",
                    (coldkey,)
                )
                row = await cursor.fetchone()
                if not row:
                    return False
                    
                miner_uid = row[0]
                
                if operation_type == 'stake':
                    # For failed stakes, remove any partially created position
                    if 'position_id' in last_known_good_state:
                        await conn.execute(
                            f"""
                            DELETE FROM miner_{miner_uid}_positions
                            WHERE position_id = ?
                            """,
                            (last_known_good_state['position_id'],)
                        )
                
                elif operation_type == 'unstake':
                    # For failed unstakes:
                    # 1. Restore original position states
                    for pos in last_known_good_state.get('positions', []):
                        await conn.execute(
                            f"""
                            UPDATE miner_{miner_uid}_positions
                            SET remaining_alpha = ?,
                                status = ?
                            WHERE position_id = ?
                            """,
                            (
                                pos['remaining_alpha'],
                                pos['status'],
                                pos['position_id']
                            )
                        )
                    
                    # 2. Remove any created trade and mappings
                    if 'trade_id' in last_known_good_state:
                        # Remove position-trade mappings first
                        await conn.execute(
                            f"""
                            DELETE FROM miner_{miner_uid}_position_trades
                            WHERE trade_id = ?
                            """,
                            (last_known_good_state['trade_id'],)
                        )
                        
                        # Then remove the trade
                        await conn.execute(
                            f"""
                            DELETE FROM miner_{miner_uid}_trades
                            WHERE trade_id = ?
                            """,
                            (last_known_good_state['trade_id'],)
                        )
                
                # Update miner stats
                await self._update_miner_stats(conn, miner_uid)
                
                # Log recovery action
                print(
                    f"Recovered from failed {operation_type} for miner {coldkey} "
                    f"in subnet {failed_event.netuid}"
                )
                
                # Commit recovery changes
                await conn.commit()
                return True
                
            except Exception as e:
                await conn.rollback()
                raise DatabaseError(
                    f"Failed to recover from failed transaction: {str(e)}"
                )

    async def process_event_with_recovery(
        self,
        coldkey: str,
        event: StakeEvent,
        current_block: int
    ) -> bool:
        """Process a single event with failure recovery.
        
        This method:
        1. Captures current state before processing
        2. Attempts to process the event
        3. Recovers from failures if needed
        
        Args:
            coldkey (str): Miner's coldkey
            event (StakeEvent): Event to process
            current_block (int): Current block number
            
        Returns:
            bool: True if processing succeeded
        """
        async with self.db.get_connection() as conn:
            try:
                # Get miner info
                cursor = await conn.execute(
                    "SELECT uid FROM miners WHERE coldkey = ?",
                    (coldkey,)
                )
                row = await cursor.fetchone()
                if not row:
                    return False
                    
                miner_uid = row[0]
                
                # Capture current state
                last_known_good_state = {}
                
                if event.action == StakeAction.STAKE_ADDED:
                    # For stakes, we just need to track any created position
                    operation_type = 'stake'
                    
                elif event.action == StakeAction.STAKE_REMOVED:
                    # For unstakes, capture affected position states
                    operation_type = 'unstake'
                    cursor = await conn.execute(
                        f"""
                        SELECT position_id, remaining_alpha, status
                        FROM miner_{miner_uid}_positions
                        WHERE netuid = ? AND status != 'closed'
                        ORDER BY entry_timestamp ASC
                        """,
                        (event.netuid,)
                    )
                    last_known_good_state['positions'] = [
                        {
                            'position_id': row[0],
                            'remaining_alpha': row[1],
                            'status': row[2]
                        }
                        async for row in cursor
                    ]
                
                # Try to process the event
                try:
                    if event.action == StakeAction.STAKE_ADDED:
                        cursor = await self._handle_stake(conn, miner_uid, event)
                        if cursor and cursor.lastrowid:
                            last_known_good_state['position_id'] = cursor.lastrowid
                    else:
                        cursor = await self._handle_unstake(conn, miner_uid, event)
                        if cursor and cursor.lastrowid:
                            last_known_good_state['trade_id'] = cursor.lastrowid
                            
                    await conn.commit()
                    return True
                    
                except Exception as e:
                    # Rollback and attempt recovery
                    await conn.rollback()
                    print(f"Event processing failed: {str(e)}")
                    
                    return await self.handle_failed_transaction(
                        coldkey=coldkey,
                        operation_type=operation_type,
                        failed_event=event,
                        last_known_good_state=last_known_good_state,
                        current_block=current_block
                    )
                    
            except Exception as e:
                raise DatabaseError(f"Failed to process event: {str(e)}") 