"""
Tests focused on position tracking, trade processing, and ROI calculations.

This module provides comprehensive testing of:
1. Basic position/trade flows
2. Complex multi-position trades
3. Partial closes and ROI tracking
4. Edge cases and error conditions
5. Time window queries for scoring
"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from alphatrade.database.models import (
    Position, Trade, PositionStatus, PositionTrade
)
from alphatrade.database.exceptions import (
    ValidationError, IntegrityError, StateError,
    InsufficientAlphaError
)

from .fixtures import test_data, db

@pytest.mark.asyncio
async def test_basic_position_flow(db, test_data):
    """Test basic position lifecycle with ROI tracking."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Open position
    position = test_data.generate_position(
        entry_tao=1000.0,
        entry_alpha=100.0
    )
    pos_id = await db.add_position(miner_uid, position)
    
    # Verify initial state
    stored_pos = await db.get_position(miner_uid, pos_id)
    assert stored_pos.status == PositionStatus.OPEN
    assert stored_pos.final_roi is None
    assert stored_pos.closed_at is None
    
    # Close position with 10% ROI
    trade = test_data.generate_trade(
        exit_tao=1100.0,  # 10% more TAO
        exit_alpha=100.0  # Full position
    )
    await db.process_close(miner_uid, trade, [stored_pos])
    
    # Verify final state
    stored_pos = await db.get_position(miner_uid, pos_id)
    assert stored_pos.status == PositionStatus.CLOSED
    assert stored_pos.remaining_alpha == 0.0
    assert abs(stored_pos.final_roi - 10.0) < 1e-8  # 10% ROI with float tolerance
    assert stored_pos.closed_at == trade.exit_timestamp

@pytest.mark.asyncio
async def test_partial_closes(db, test_data):
    """Test partial position closes with ROI tracking."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Open position
    position = test_data.generate_position(
        entry_tao=1000.0,
        entry_alpha=100.0
    )
    pos_id = await db.add_position(miner_uid, position)
    
    # Close 60% with 20% ROI
    trade1 = test_data.generate_trade(
        exit_tao=720.0,   # (1000 * 0.6) * 1.2
        exit_alpha=60.0   # 60% of position
    )
    await db.process_close(miner_uid, trade1, [await db.get_position(miner_uid, pos_id)])
    
    # Verify partial state
    stored_pos = await db.get_position(miner_uid, pos_id)
    assert stored_pos.status == PositionStatus.PARTIAL
    assert stored_pos.remaining_alpha == 40.0
    assert stored_pos.final_roi is None
    assert stored_pos.closed_at is None
    
    # Close remaining 40% with 30% ROI
    trade2 = test_data.generate_trade(
        exit_tao=520.0,   # (1000 * 0.4) * 1.3
        exit_alpha=40.0   # Remaining 40%
    )
    await db.process_close(miner_uid, trade2, [stored_pos])
    
    # Verify final state - weighted average ROI should be 24%
    # (60% at 20% ROI + 40% at 30% ROI = 24% overall)
    stored_pos = await db.get_position(miner_uid, pos_id)
    assert stored_pos.status == PositionStatus.CLOSED
    assert stored_pos.remaining_alpha == 0.0
    assert abs(stored_pos.final_roi - 24.0) < 1e-8
    assert stored_pos.closed_at == trade2.exit_timestamp

@pytest.mark.asyncio
async def test_multi_position_trade(db, test_data):
    """Test trade closing multiple positions with different entry prices."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Create positions with different entry prices
    positions = []
    for i, (tao, alpha) in enumerate([
        (1000.0, 100.0),  # 10 TAO/alpha
        (1500.0, 100.0),  # 15 TAO/alpha
        (2000.0, 100.0)   # 20 TAO/alpha
    ]):
        position = test_data.generate_position(
            entry_tao=tao,
            entry_alpha=alpha
        )
        pos_id = await db.add_position(miner_uid, position)
        positions.append(await db.get_position(miner_uid, pos_id))
    
    # Close first two positions with blended ROI
    trade = test_data.generate_trade(
        exit_tao=3000.0,  # 20% ROI on average
        exit_alpha=200.0  # First two positions
    )
    await db.process_close(miner_uid, trade, positions[:2])
    
    # Verify ROIs - should be different due to different entry prices
    pos1 = await db.get_position(miner_uid, positions[0].position_id)
    pos2 = await db.get_position(miner_uid, positions[1].position_id)
    
    # Position 1 (10 TAO/alpha) should have higher ROI than Position 2 (15 TAO/alpha)
    assert pos1.final_roi > pos2.final_roi
    # Both should be closed
    assert pos1.status == pos2.status == PositionStatus.CLOSED
    # Third position should be untouched
    pos3 = await db.get_position(miner_uid, positions[2].position_id)
    assert pos3.status == PositionStatus.OPEN

@pytest.mark.asyncio
async def test_time_window_queries(db, test_data):
    """Test querying positions closed within specific time windows."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Create and close positions across different times
    position_data = [
        # (days_ago, entry_tao, entry_alpha, exit_tao, exit_alpha)
        (30, 1000.0, 100.0, 1100.0, 100.0),  # 10% ROI, 30 days ago
        (20, 1000.0, 100.0, 1200.0, 100.0),  # 20% ROI, 20 days ago
        (10, 1000.0, 100.0, 1300.0, 100.0),  # 30% ROI, 10 days ago
        (5,  1000.0, 100.0, 1400.0, 100.0),  # 40% ROI, 5 days ago
    ]
    
    positions = []
    for days_ago, entry_tao, entry_alpha, exit_tao, exit_alpha in position_data:
        # Create position
        position = test_data.generate_position(
            entry_tao=entry_tao,
            entry_alpha=entry_alpha
        )
        position.entry_timestamp = test_data.start_time - timedelta(days=days_ago)
        pos_id = await db.add_position(miner_uid, position)
        stored_pos = await db.get_position(miner_uid, pos_id)
        positions.append(stored_pos)
        
        # Close position
        trade = test_data.generate_trade(
            exit_tao=exit_tao,
            exit_alpha=exit_alpha,
            exit_timestamp=position.entry_timestamp + timedelta(hours=1)
        )
        await db.process_close(miner_uid, trade, [stored_pos])
    
    # Test various time windows
    async def check_window(days: int, expected_count: int, min_roi: float, max_roi: float):
        start_time = test_data.start_time - timedelta(days=days)
        closed = await db.get_closed_positions_in_window(
            miner_uid,
            start_time,
            test_data.start_time
        )
        assert len(closed) == expected_count
        if expected_count > 0:
            # Verify ROI range
            assert all(p.final_roi >= min_roi for p in closed)
            assert all(p.final_roi <= max_roi for p in closed)
            # Verify ROI ordering only if multiple positions
            if expected_count > 1:
                assert closed[0].final_roi > closed[-1].final_roi
    
    # Last 7 days should have 1 position (40% ROI)
    await check_window(7, 1, 35.0, 45.0)
    
    # Last 15 days should have 2 positions (30-40% ROI)
    await check_window(15, 2, 25.0, 45.0)
    
    # Last 25 days should have 3 positions (20-40% ROI)
    await check_window(25, 3, 15.0, 45.0)
    
    # Full range should have all 4 positions (10-40% ROI)
    await check_window(35, 4, 5.0, 45.0)

@pytest.mark.asyncio
async def test_edge_cases(db, test_data):
    """Test various edge cases in position/trade processing."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Test case 1: Zero remaining alpha after partial close
    position = test_data.generate_position(
        entry_tao=1000.0,
        entry_alpha=100.0
    )
    pos_id = await db.add_position(miner_uid, position)
    
    # Close exactly 50%
    trade = test_data.generate_trade(
        exit_tao=500.0,
        exit_alpha=50.0
    )
    await db.process_close(miner_uid, trade, [await db.get_position(miner_uid, pos_id)])
    
    stored_pos = await db.get_position(miner_uid, pos_id)
    assert stored_pos.status == PositionStatus.PARTIAL
    assert abs(stored_pos.remaining_alpha - 50.0) < 1e-8
    
    # Test case 2: Multiple partial closes summing to exactly 100%
    trades = [
        # (exit_tao, exit_alpha)
        (200.0, 20.0),  # 20%
        (150.0, 15.0),  # 15%
        (150.0, 15.0)   # 15% (total 50% remaining)
    ]
    
    for exit_tao, exit_alpha in trades:
        trade = test_data.generate_trade(
            exit_tao=exit_tao,
            exit_alpha=exit_alpha
        )
        await db.process_close(
            miner_uid,
            trade,
            [await db.get_position(miner_uid, pos_id)]
        )
    
    stored_pos = await db.get_position(miner_uid, pos_id)
    assert stored_pos.status == PositionStatus.CLOSED
    assert stored_pos.remaining_alpha == 0.0
    
    # Test case 3: Attempt to close more than available
    position = test_data.generate_position(
        entry_tao=1000.0,
        entry_alpha=100.0
    )
    pos_id = await db.add_position(miner_uid, position)
    
    trade = test_data.generate_trade(
        exit_tao=1100.0,
        exit_alpha=101.0  # More than available
    )
    with pytest.raises(InsufficientAlphaError):
        await db.process_close(
            miner_uid,
            trade,
            [await db.get_position(miner_uid, pos_id)]
        )
    
    # Test case 4: Close with exactly 0% ROI
    trade = test_data.generate_trade(
        exit_tao=1000.0,  # Exactly entry amount
        exit_alpha=100.0
    )
    await db.process_close(miner_uid, trade, [await db.get_position(miner_uid, pos_id)])
    
    stored_pos = await db.get_position(miner_uid, pos_id)
    assert stored_pos.status == PositionStatus.CLOSED
    assert abs(stored_pos.final_roi - 0.0) < 1e-8
    
    # Test case 5: Attempt to modify closed position
    with pytest.raises(StateError):
        trade = test_data.generate_trade(
            exit_tao=100.0,
            exit_alpha=10.0
        )
        await db.process_close(miner_uid, trade, [stored_pos])

@pytest.mark.asyncio
async def test_concurrent_operations(db, test_data):
    """Test concurrent position operations."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Create multiple positions
    positions = []
    for i in range(5):
        position = test_data.generate_position(
            entry_tao=1000.0,
            entry_alpha=100.0
        )
        pos_id = await db.add_position(miner_uid, position)
        positions.append(await db.get_position(miner_uid, pos_id))
    
    # Concurrently close positions with different ROIs
    async def close_position(position: Position, roi: float):
        trade = test_data.generate_trade(
            exit_tao=position.entry_tao * (1 + roi/100),
            exit_alpha=position.entry_alpha
        )
        return await db.process_close(miner_uid, trade, [position])
    
    # Close positions with ROIs from 10% to 50%
    tasks = [
        close_position(pos, (i + 1) * 10)
        for i, pos in enumerate(positions)
    ]
    
    await asyncio.gather(*tasks)
    
    # Verify all positions were closed with correct ROIs
    for i, position in enumerate(positions):
        stored_pos = await db.get_position(miner_uid, position.position_id)
        assert stored_pos.status == PositionStatus.CLOSED
        expected_roi = (i + 1) * 10
        assert abs(stored_pos.final_roi - expected_roi) < 1e-8

@pytest.mark.asyncio
async def test_complex_scenarios(db, test_data):
    """Test complex scenarios combining multiple operations."""
    miner_uid = await db.register_miner(
        test_data.coldkey,
        test_data.start_time
    )
    
    # Scenario 1: Multiple positions with varying entry prices,
    # closed in different orders with different ROIs
    positions = []
    for i, (tao, alpha) in enumerate([
        (1000.0, 100.0),  # 10 TAO/alpha
        (1200.0, 100.0),  # 12 TAO/alpha
        (1500.0, 100.0),  # 15 TAO/alpha
        (1800.0, 100.0)   # 18 TAO/alpha
    ]):
        position = test_data.generate_position(
            entry_tao=tao,
            entry_alpha=alpha
        )
        pos_id = await db.add_position(miner_uid, position)
        positions.append(await db.get_position(miner_uid, pos_id))
    
    # Close in non-sequential order with different partial amounts
    trades = [
        # (position_index, exit_tao, exit_alpha)
        (2, 900.0, 60.0),   # Close 60% of position 3
        (0, 600.0, 60.0),   # Close 60% of position 1
        (1, 1200.0, 100.0), # Close 100% of position 2
        (3, 1080.0, 60.0),  # Close 60% of position 4
        (0, 400.0, 40.0),   # Close remaining 40% of position 1
        (2, 600.0, 40.0),   # Close remaining 40% of position 3
        (3, 720.0, 40.0)    # Close remaining 40% of position 4
    ]
    
    for pos_idx, exit_tao, exit_alpha in trades:
        position = await db.get_position(miner_uid, positions[pos_idx].position_id)
        trade = test_data.generate_trade(
            exit_tao=exit_tao,
            exit_alpha=exit_alpha
        )
        await db.process_close(miner_uid, trade, [position])
    
    # Verify final states
    final_positions = [
        await db.get_position(miner_uid, p.position_id)
        for p in positions
    ]
    
    # All positions should be closed
    assert all(p.status == PositionStatus.CLOSED for p in final_positions)
    
    # ROIs should reflect the weighted average of partial closes
    expected_rois = [
        # Position 1: (600/600 - 1)*100 + (400/400 - 1)*100 = 0%
        0.0,
        # Position 2: (1200/1200 - 1)*100 = 0%
        0.0,
        # Position 3: (900/900 - 1)*100 + (600/600 - 1)*100 = 0%
        0.0,
        # Position 4: (1080/1080 - 1)*100 + (720/720 - 1)*100 = 0%
        0.0
    ]
    
    for pos, expected_roi in zip(final_positions, expected_rois):
        assert abs(pos.final_roi - expected_roi) < 1e-8 