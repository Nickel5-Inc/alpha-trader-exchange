"""
End-to-end tests for database functionality.
"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

from alphatrade.database.models import (
    Position, Trade, PositionStatus, MinerStatus,
    PositionTrade, PerformanceConfig,
    StakeEvent, StakeAction, ApiTrade, PoolData
)
from alphatrade.database.exceptions import (
    ValidationError, IntegrityError, StateError, InsufficientAlphaError,
    ConnectionError, DatabaseError
)

from .fixtures import test_data, db, mock_api

@pytest.mark.asyncio
async def test_miner_registration(db, test_data):
    """Test miner registration and table creation."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time,
        MinerStatus.ACTIVE
    )
    assert miner_uid > 0
    
    # Verify miner was created
    miner = await db.get_miner(test_data.coldkey)
    assert miner is not None
    assert miner.coldkey == test_data.coldkey
    assert miner.status == MinerStatus.ACTIVE
    
    # Try registering same miner again
    with pytest.raises(IntegrityError):
        await db.register_miner(
            test_data.coldkey,
            test_data.start_time
        )

@pytest.mark.asyncio
async def test_position_lifecycle(db, test_data):
    """Test full position lifecycle including partial closes."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Add position
    position = test_data.generate_position()
    position_id = await db.add_position(miner_uid, position)
    assert position_id > 0
    
    # Verify position was created
    stored_position = await db.get_position(miner_uid, position_id)
    assert stored_position is not None
    assert stored_position.entry_tao == position.entry_tao
    assert stored_position.status == PositionStatus.OPEN
    
    # Partially close position
    trade = test_data.generate_trade(exit_alpha=5.0)
    mappings = await db.process_close(miner_uid, trade, [stored_position])
    assert len(mappings) == 1
    assert mappings[0].alpha_amount == 5.0
    
    # Verify position is now partial
    stored_position = await db.get_position(miner_uid, position_id)
    assert stored_position.status == PositionStatus.PARTIAL
    assert stored_position.remaining_alpha == 5.0
    
    # Fully close position
    trade = test_data.generate_trade(exit_alpha=5.0)
    mappings = await db.process_close(miner_uid, trade, [stored_position])
    assert len(mappings) == 1
    
    # Verify position is now closed
    stored_position = await db.get_position(miner_uid, position_id)
    assert stored_position.status == PositionStatus.CLOSED
    assert stored_position.remaining_alpha == 0.0

@pytest.mark.asyncio
async def test_multi_position_trade(db, test_data):
    """Test trade closing multiple positions."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Add two positions
    position1 = test_data.generate_position(entry_tao=100.0, entry_alpha=10.0)
    position2 = test_data.generate_position(entry_tao=200.0, entry_alpha=20.0)
    
    pos1_id = await db.add_position(miner_uid, position1)
    pos2_id = await db.add_position(miner_uid, position2)
    
    # Close both positions in one trade
    trade = test_data.generate_trade(exit_tao=360.0, exit_alpha=30.0)
    positions = [
        await db.get_position(miner_uid, pos1_id),
        await db.get_position(miner_uid, pos2_id)
    ]
    
    mappings = await db.process_close(miner_uid, trade, positions)
    assert len(mappings) == 2
    
    # Verify both positions are closed
    for pos_id in [pos1_id, pos2_id]:
        position = await db.get_position(miner_uid, pos_id)
        assert position.status == PositionStatus.CLOSED

@pytest.mark.asyncio
async def test_performance_metrics(db, test_data):
    """Test performance metric calculations."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Add and close multiple positions over time
    positions = []
    trades = []
    
    # Create positions and trades over 7 days
    for i in range(7):
        timestamp = test_data.start_time + timedelta(days=i)
        
        # Add position
        position = test_data.generate_position(
            entry_tao=100.0,
            entry_alpha=10.0
        )
        position.entry_timestamp = timestamp
        pos_id = await db.add_position(miner_uid, position)
        positions.append(await db.get_position(miner_uid, pos_id))
        
        # Close with 10% ROI
        trade = test_data.generate_trade(
            exit_tao=110.0,
            exit_alpha=10.0,
            exit_timestamp=timestamp + timedelta(hours=12)
        )
        await db.process_close(miner_uid, trade, [positions[-1]])
        trades.append(trade)
    
    # Take performance snapshot
    snapshot = await db.take_performance_snapshot(
        miner_uid,
        test_data.start_time + timedelta(days=7)
    )
    
    assert snapshot is not None
    assert snapshot.total_trades == 7
    assert abs(snapshot.roi_simple - 10.0) < 0.1  # ~10% ROI
    assert snapshot.win_rate == 100.0  # All trades profitable

@pytest.mark.asyncio
async def test_archival(db, test_data):
    """Test position and trade archival."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Add and close positions
    old_position = test_data.generate_position()
    old_position.entry_timestamp = test_data.start_time - timedelta(days=30)
    old_pos_id = await db.add_position(miner_uid, old_position)
    
    new_position = test_data.generate_position()
    new_pos_id = await db.add_position(miner_uid, new_position)
    
    # Close old position
    trade = test_data.generate_trade(
        exit_timestamp=test_data.start_time - timedelta(days=29)
    )
    await db.process_close(
        miner_uid,
        trade,
        [await db.get_position(miner_uid, old_pos_id)]
    )
    
    # Archive old data
    archive_stats = await db.archive_closed_positions(
        miner_uid,
        archive_after=timedelta(days=7)
    )
    
    assert archive_stats['positions_archived'] == 1
    assert archive_stats['trades_archived'] == 1
    
    # Verify old position was archived
    old_position = await db.get_position(miner_uid, old_pos_id)
    assert old_position is None
    
    # Verify new position remains
    new_position = await db.get_position(miner_uid, new_pos_id)
    assert new_position is not None

@pytest.mark.asyncio
async def test_error_recovery(db, test_data):
    """Test error recovery mechanisms."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Add position
    position = test_data.generate_position()
    pos_id = await db.add_position(miner_uid, position)
    
    # Record initial state
    initial_state = {
        'position_id': pos_id,
        'status': PositionStatus.OPEN.value,
        'remaining_alpha': position.remaining_alpha
    }
    
    # Simulate failed trade
    trade = test_data.generate_trade()
    stored_position = await db.get_position(miner_uid, pos_id)
    
    # Force error during trade processing
    with pytest.raises(Exception):
        async with db.get_connection() as conn:
            await conn.execute("INVALID SQL")
            await db.process_close(miner_uid, trade, [stored_position])
    
    # Recover from failed operation
    success = await db.recover_failed_operation(
        miner_uid,
        'close_position',
        initial_state
    )
    assert success
    
    # Verify position was restored
    position = await db.get_position(miner_uid, pos_id)
    assert position.status == PositionStatus.OPEN
    assert position.remaining_alpha == initial_state['remaining_alpha']

@pytest.mark.asyncio
async def test_edge_cases(db, test_data):
    """Test various edge cases and error conditions."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Test invalid position amounts
    with pytest.raises(ValidationError):
        position = test_data.generate_position(entry_tao=-100.0)
        await db.add_position(miner_uid, position)
    
    # Test closing more alpha than available
    position = test_data.generate_position(entry_alpha=10.0)
    pos_id = await db.add_position(miner_uid, position)
    
    trade = test_data.generate_trade(exit_alpha=20.0)
    with pytest.raises(InsufficientAlphaError):
        stored_position = await db.get_position(miner_uid, pos_id)
        await db.process_close(miner_uid, trade, [stored_position])
    
    # Test invalid state transitions
    position = test_data.generate_position()
    pos_id = await db.add_position(miner_uid, position)
    
    # Close position
    trade = test_data.generate_trade()
    stored_position = await db.get_position(miner_uid, pos_id)
    await db.process_close(miner_uid, trade, [stored_position])
    
    # Try to close again
    with pytest.raises(StateError):
        stored_position = await db.get_position(miner_uid, pos_id)
        await db.process_close(miner_uid, trade, [stored_position])

@pytest.mark.asyncio
async def test_concurrent_operations(db, test_data):
    """Test concurrent database operations."""
    # Register miner
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Add multiple positions concurrently
    async def add_position():
        position = test_data.generate_position()
        return await db.add_position(miner_uid, position)
    
    position_ids = await asyncio.gather(
        *[add_position() for _ in range(10)]
    )
    assert len(position_ids) == 10
    assert len(set(position_ids)) == 10  # All IDs should be unique
    
    # Close positions concurrently
    positions = []
    for pos_id in position_ids:
        positions.append(await db.get_position(miner_uid, pos_id))
    
    async def close_position(position):
        trade = test_data.generate_trade(exit_alpha=position.entry_alpha)
        return await db.process_close(miner_uid, trade, [position])
    
    results = await asyncio.gather(
        *[close_position(pos) for pos in positions]
    )
    assert len(results) == 10
    
    # Verify all positions are closed
    for pos_id in position_ids:
        position = await db.get_position(miner_uid, pos_id)
        assert position.status == PositionStatus.CLOSED

@pytest.mark.asyncio
async def test_connection_management(db):
    """Test database connection management."""
    # Test connection pool
    async with db.get_connection() as conn1:
        assert conn1 is not None
        async with db.get_connection() as conn2:
            assert conn2 is not None
            assert conn1 != conn2  # Different connections
            
    # Test connection reuse
    async with db.get_connection() as conn3:
        assert conn3 is not None  # Should reuse a connection from pool
        
    # Test connection validation
    await db.cleanup_connections()  # Clear pool
    async with db.get_connection() as conn:
        await conn.execute("PRAGMA integrity_check")  # Should work
        
    # Test max connections
    connections = []
    for _ in range(5):  # db.max_connections is 5
        async with db.get_connection() as conn:
            connections.append(conn)
            
    assert len(connections) == 5
    
    # Test connection cleanup
    await db.cleanup_connections()
    assert len(db._connection_pool) == 0

@pytest.mark.asyncio
async def test_transaction_handling(db, test_data):
    """Test transaction handling and rollbacks."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Test successful transaction
    position = test_data.generate_position()
    pos_id = await db.add_position(miner_uid, position)
    assert pos_id > 0
    
    # Test transaction rollback
    async with db.get_connection() as conn:
        await conn.execute("BEGIN")
        await conn.execute(
            f"""
            INSERT INTO miner_{miner_uid}_positions (
                netuid, hotkey, entry_block, entry_timestamp,
                entry_tao, entry_alpha, remaining_alpha, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "test", 1, datetime.now(timezone.utc), 100, 10, 10, PositionStatus.OPEN.value)
        )
        await conn.rollback()
    
    # Verify rollback worked
    cursor = await conn.execute(
        f"SELECT COUNT(*) FROM miner_{miner_uid}_positions WHERE status = ?",
        ("invalid_status",)
    )
    count = (await cursor.fetchone())[0]
    assert count == 0

@pytest.mark.asyncio
async def test_position_validation(db, test_data):
    """Test position validation and state transitions."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Test invalid position data
    invalid_cases = [
        (0, 10),      # Zero TAO
        (-100, 10),   # Negative TAO
        (100, 0),     # Zero alpha
        (100, -10),   # Negative alpha
        (float('inf'), 10),  # Infinite TAO
        (100, float('nan'))  # NaN alpha
    ]
    
    for entry_tao, entry_alpha in invalid_cases:
        with pytest.raises(ValidationError):
            position = test_data.generate_position(
                entry_tao=entry_tao,
                entry_alpha=entry_alpha
            )
            await db.add_position(miner_uid, position)
    
    # Test invalid state transitions
    position = test_data.generate_position()
    pos_id = await db.add_position(miner_uid, position)
    
    # Try to close more than remaining
    trade = test_data.generate_trade(exit_alpha=position.entry_alpha * 2)
    with pytest.raises(InsufficientAlphaError):
        stored_position = await db.get_position(miner_uid, pos_id)
        await db.process_close(miner_uid, trade, [stored_position])
    
    # Close position
    trade = test_data.generate_trade(exit_alpha=position.entry_alpha)
    stored_position = await db.get_position(miner_uid, pos_id)
    await db.process_close(miner_uid, trade, [stored_position])
    
    # Try various invalid transitions
    stored_position = await db.get_position(miner_uid, pos_id)
    assert stored_position.status == PositionStatus.CLOSED
    
    with pytest.raises(StateError):
        # Try to close again
        await db.process_close(miner_uid, trade, [stored_position])
    
    with pytest.raises(StateError):
        # Try to modify closed position
        async with db.get_connection() as conn:
            await conn.execute(
                f"""
                UPDATE miner_{miner_uid}_positions
                SET remaining_alpha = ?
                WHERE position_id = ?
                """,
                (5.0, pos_id)
            )

@pytest.mark.asyncio
async def test_complex_position_scenarios(db, test_data):
    """Test complex position scenarios and FIFO accounting."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Create multiple positions with different entry prices
    positions = []
    for i in range(3):
        position = test_data.generate_position(
            entry_tao=100.0 * (i + 1),  # 100, 200, 300
            entry_alpha=10.0            # Same alpha amount
        )
        pos_id = await db.add_position(miner_uid, position)
        positions.append(await db.get_position(miner_uid, pos_id))
    
    # Test partial closes in various orders
    # Close half of first position
    trade1 = test_data.generate_trade(
        exit_tao=60.0,   # 20% ROI
        exit_alpha=5.0   # Half of first position
    )
    mappings = await db.process_close(miner_uid, trade1, [positions[0]])
    assert len(mappings) == 1
    assert abs(mappings[0].roi_tao - 20.0) < 1e-10  # Verify ROI calculation with tolerance
    
    # Get current state of positions
    pos1 = await db.get_position(miner_uid, 1)  # First position with 5 alpha remaining
    pos2 = await db.get_position(miner_uid, 2)  # Second position with 10 alpha
    
    # Close across multiple positions
    trade2 = test_data.generate_trade(
        exit_tao=375.0,  # 25% ROI
        exit_alpha=15.0  # Rest of first + half of second
    )
    mappings = await db.process_close(miner_uid, trade2, [pos1, pos2])
    assert len(mappings) == 2
    
    # Verify FIFO order was maintained
    assert mappings[0].position_id == 1  # First position
    assert mappings[1].position_id == 2  # Second position
    
    # Verify final state
    positions = []
    for i in range(3):
        pos = await db.get_position(miner_uid, i + 1)
        positions.append(pos)
    
    # First position should be CLOSED (all alpha closed)
    assert positions[0].status == PositionStatus.CLOSED
    # Second position should be CLOSED (all alpha closed)
    assert positions[1].status == PositionStatus.CLOSED
    # Third position should be OPEN (untouched)
    assert positions[2].status == PositionStatus.OPEN
    assert abs(positions[2].remaining_alpha - 10.0) < 1e-10  # Use tolerance for float comparison

@pytest.mark.asyncio
async def test_performance_metrics_detailed(db, test_data):
    """Test detailed performance metric calculations."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Create positions and trades with known ROIs
    rois = [10.0, -5.0, 15.0, 20.0, -8.0]  # Mixed performance
    positions = []
    trades = []
    
    for i, roi in enumerate(rois):
        # Create position
        entry_tao = 100.0
        entry_alpha = 10.0
        position = test_data.generate_position(
            entry_tao=entry_tao,
            entry_alpha=entry_alpha
        )
        pos_id = await db.add_position(miner_uid, position)
        positions.append(await db.get_position(miner_uid, pos_id))
        
        # Create trade with specified ROI
        exit_tao = entry_tao * (1 + roi/100)
        trade = test_data.generate_trade(
            exit_tao=exit_tao,
            exit_alpha=entry_alpha,
            exit_timestamp=test_data.start_time + timedelta(days=i)
        )
        await db.process_close(miner_uid, trade, [positions[-1]])
        trades.append(trade)
    
    # Take performance snapshot
    snapshot = await db.take_performance_snapshot(
        miner_uid,
        test_data.start_time + timedelta(days=len(rois))
    )
    
    # Verify metrics
    assert snapshot.total_trades == len(rois)
    assert abs(snapshot.roi_simple - sum(rois)/len(rois)) < 0.1
    assert snapshot.win_rate == 60.0  # 3 out of 5 trades profitable
    
    # Verify time-weighted ROI (more recent trades should have higher weight)
    assert snapshot.roi_weighted != snapshot.roi_simple  # Should be weighted

@pytest.mark.asyncio
async def test_archival_system_detailed(db, test_data):
    """Test detailed archival system functionality."""
    
    async def cleanup_miner(miner_uid: int):
        """Clean up miner data from all tables."""
        async with db.get_connection() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                # Delete in order respecting foreign key constraints
                # First delete position-trade mappings (they reference both positions and trades)
                await conn.execute(f"DELETE FROM miner_{miner_uid}_position_trades")
                await conn.execute(f"DELETE FROM miner_{miner_uid}_archived_position_trades")
                
                # Then delete trades
                await conn.execute(f"DELETE FROM miner_{miner_uid}_trades")
                await conn.execute(f"DELETE FROM miner_{miner_uid}_archived_trades")
                
                # Then delete positions
                await conn.execute(f"DELETE FROM miner_{miner_uid}_positions")
                await conn.execute(f"DELETE FROM miner_{miner_uid}_archived_positions")
                
                # Finally delete the miner
                await conn.execute("DELETE FROM miners WHERE uid = ?", (miner_uid,))
                
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
    
    async def setup_test_data():
        """Helper to set up test data and return the miner_uid and test objects."""
        miner_uid = await db.register_miner(
            test_data.coldkey,
            test_data.start_time
        )
        
        # Create positions with different ages
        positions = []
        trades = []
        for days_ago in [30, 20, 10, 5]:
            position = test_data.generate_position()
            position.entry_timestamp = test_data.start_time.replace(tzinfo=timezone.utc) - timedelta(days=days_ago)
            pos_id = await db.add_position(miner_uid, position)
            positions.append(await db.get_position(miner_uid, pos_id))
        
        # Close old positions
        for i in range(2):  # Close two oldest positions
            trade = test_data.generate_trade(
                exit_timestamp=(positions[i].entry_timestamp + timedelta(days=1)).replace(tzinfo=timezone.utc)
            )
            await db.process_close(miner_uid, trade, [positions[i]])
            trades.append(trade)
            print(f"Position {i+1} exit timestamp: {trade.exit_timestamp.isoformat()}")
            
        return miner_uid, positions, trades
    
    # Test each archival threshold independently
    for days in [5, 15, 25]:
        print(f"\nTesting {days} day threshold")
        miner_uid, positions, trades = await setup_test_data()
        
        cutoff_time = test_data.start_time.replace(tzinfo=timezone.utc) - timedelta(days=days)
        print(f"Cutoff time: {cutoff_time.isoformat()}")
        for i, t in enumerate(trades):
            print(f"Trade {i+1} exit time: {t.exit_timestamp.isoformat()}")
            print(f"Should archive? {t.exit_timestamp < cutoff_time}")
        
        stats = await db.archive_closed_positions(
            miner_uid,
            archive_after=timedelta(days=days),
            reference_time=test_data.start_time.replace(tzinfo=timezone.utc)
        )

        # Re-fetch positions to determine which ones are archived from the active table
        re_fetched = [await db.get_position(miner_uid, p.position_id) for p in positions]

        # Compute expected archived count based on exit timestamps
        expected_archived = sum(
            1 for p, t in zip(positions[:2], trades)  # Only check closed positions
            if t.exit_timestamp < test_data.start_time.replace(tzinfo=timezone.utc) - timedelta(days=days)
        )
        print(f"Expected archived: {expected_archived}, got: {stats['positions_archived']}")
        assert stats['positions_archived'] == expected_archived, f"For threshold {days} days, expected {expected_archived} archived but got {stats['positions_archived']}"

        # Verify each position: if it's CLOSED and its exit timestamp is older than cutoff, it should be archived
        for i, (p, r) in enumerate(zip(positions, re_fetched)):
            if i < 2:  # Only check closed positions
                if trades[i].exit_timestamp < test_data.start_time.replace(tzinfo=timezone.utc) - timedelta(days=days):
                    assert r is None, f"Position {p.position_id} should be archived"
                else:
                    assert r is not None, f"Position {p.position_id} should not be archived"
            else:
                assert r is not None, f"Position {p.position_id} should not be archived (not closed)"
        
        # Clean up after each iteration
        await cleanup_miner(miner_uid)

@pytest.mark.asyncio
async def test_error_recovery_detailed(db, test_data):
    """Test detailed error recovery scenarios."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Test recovery from failed position creation
    async def simulate_failed_position():
        position = test_data.generate_position()
        async with db.get_connection() as conn:
            await conn.execute("BEGIN")
            cursor = await conn.execute(
                f"""
                INSERT INTO miner_{miner_uid}_positions (
                    netuid, hotkey, entry_block, entry_timestamp,
                    entry_tao, entry_alpha, remaining_alpha, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position.netuid, position.hotkey,
                    position.entry_block, position.entry_timestamp,
                    position.entry_tao, position.entry_alpha,
                    position.entry_alpha, PositionStatus.OPEN.value
                )
            )
            position_id = cursor.lastrowid
            # Simulate failure before commit
            raise DatabaseError("Simulated failure")
            
    with pytest.raises(DatabaseError):
        await simulate_failed_position()
    
    # Verify no position was created
    async with db.get_connection() as conn:
        cursor = await conn.execute(
            f"SELECT COUNT(*) FROM miner_{miner_uid}_positions"
        )
        count = (await cursor.fetchone())[0]
        assert count == 0
    
    # Test recovery from failed trade
    position = test_data.generate_position()
    pos_id = await db.add_position(miner_uid, position)
    
    async def simulate_failed_trade():
        trade = test_data.generate_trade()
        stored_position = await db.get_position(miner_uid, pos_id)
        async with db.get_connection() as conn:
            await conn.execute("BEGIN")
            # Insert trade
            cursor = await conn.execute(
                f"""
                INSERT INTO miner_{miner_uid}_trades (
                    netuid, hotkey, exit_block, exit_timestamp,
                    exit_tao, exit_alpha
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.netuid, trade.hotkey,
                    trade.exit_block, trade.exit_timestamp,
                    trade.exit_tao, trade.exit_alpha
                )
            )
            trade_id = cursor.lastrowid
            # Simulate failure before position update
            raise DatabaseError("Simulated failure")
    
    with pytest.raises(DatabaseError):
        await simulate_failed_trade()
    
    # Verify position state was not changed
    stored_position = await db.get_position(miner_uid, pos_id)
    assert stored_position.status == PositionStatus.OPEN
    assert stored_position.remaining_alpha == position.entry_alpha
    
    # Verify no trade was created
    async with db.get_connection() as conn:
        cursor = await conn.execute(
            f"SELECT COUNT(*) FROM miner_{miner_uid}_trades"
        )
        count = (await cursor.fetchone())[0]
        assert count == 0 