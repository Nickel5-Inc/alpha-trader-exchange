"""
End-to-end tests for the Alpha Trade Exchange validator.

This test suite simulates a complete validator lifecycle including:
1. Database initialization
2. Miner registration
3. Position tracking
4. Performance monitoring
5. Weight setting
"""

import os
import tempfile
import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, AsyncMock

from alphatrade.database.manager import DatabaseManager
from alphatrade.database.models import (
    PerformanceConfig,
    StakeEvent,
    StakeAction,
    Position,
    Trade,
    MinerStatus
)
from alphatrade.validator import Validator
from alphatrade.monitoring import HealthMonitor

class MockMetagraph:
    """Mock bittensor metagraph."""
    def __init__(self):
        self.hotkeys = [
            "0x1234...miner1",
            "0x5678...miner2",
            "0x9abc...miner3"
        ]
        self.coldkeys = [
            "0x1111...cold1",
            "0x2222...cold2",
            "0x3333...cold3"
        ]
        self.dividends = [0, 0, 0]  # All are miners
        self.n = len(self.hotkeys)
        self.scores = [0.0] * self.n

class MockAPI:
    """Mock Alpha Trade Exchange API."""
    def __init__(self):
        self.events = []
        self.current_block = 1000
    
    async def get_stake_events(self, coldkey: str, since_block: int = 0):
        """Get stake events for a coldkey."""
        return [e for e in self.events if e.coldkey == coldkey and e.block_number > since_block]
    
    def add_event(self, event: StakeEvent):
        """Add a new event."""
        self.events.append(event)
        self.current_block = max(self.current_block, event.block_number)

@pytest.fixture
async def test_env():
    """Set up test environment."""
    # Create temporary database
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    
    # Initialize components
    config = PerformanceConfig(
        rolling_window_days=7,
        snapshot_interval_hours=1,  # Shorter for testing
        retention_days=30
    )
    
    db = DatabaseManager(db_path, max_connections=5, performance_config=config)
    await db.setup_database()
    
    # Create mocks
    metagraph = MockMetagraph()
    api = MockAPI()
    
    # Create validator with mocks
    validator = Validator(
        database=db,
        metagraph=metagraph,
        api=api
    )
    
    # Create monitor
    monitor = HealthMonitor(db)
    
    yield {
        'db': db,
        'validator': validator,
        'monitor': monitor,
        'metagraph': metagraph,
        'api': api
    }
    
    # Cleanup
    await db.close()
    try:
        os.unlink(db_path)
        for ext in ['-wal', '-shm']:
            wal_file = db_path + ext
            if os.path.exists(wal_file):
                os.unlink(wal_file)
    except OSError:
        pass

@pytest.mark.asyncio
async def test_validator_lifecycle(test_env):
    """Test complete validator lifecycle."""
    db = test_env['db']
    validator = test_env['validator']
    monitor = test_env['monitor']
    api = test_env['api']
    
    # 1. Initial state
    # Validator should sync metagraph and register miners
    await validator.forward()
    
    # Check miners were registered
    async with db.session() as session:
        result = await session.execute("SELECT COUNT(*) FROM miners")
        count = (await result.fetchone())[0]
        assert count == 3  # All miners registered
    
    # 2. Process stake events
    # Add some stake events
    now = datetime.now(timezone.utc)
    events = [
        # Miner 1 stakes
        StakeEvent(
            block_number=1001,
            timestamp=now,
            netuid=1,
            action=StakeAction.DELEGATE,
            coldkey="0x1111...cold1",
            hotkey="0x1234...miner1",
            tao_amount=100.0,
            alpha_amount=10.0,
            extrinsic_id="0x123",
            alpha_price_in_tao=10.0,
            alpha_price_in_usd=1.0,
            usd_amount=100.0
        ),
        # Miner 2 stakes
        StakeEvent(
            block_number=1002,
            timestamp=now + timedelta(minutes=1),
            netuid=1,
            action=StakeAction.DELEGATE,
            coldkey="0x2222...cold2",
            hotkey="0x5678...miner2",
            tao_amount=200.0,
            alpha_amount=20.0,
            extrinsic_id="0x456",
            alpha_price_in_tao=10.0,
            alpha_price_in_usd=1.0,
            usd_amount=200.0
        )
    ]
    
    for event in events:
        api.add_event(event)
    
    # Process events
    await validator.forward()
    
    # Check positions were created
    async with db.session() as session:
        result = await session.execute(
            """
            SELECT COUNT(*) FROM miner_1_positions 
            WHERE status = 'open'
            """
        )
        count = (await result.fetchone())[0]
        assert count == 1  # Miner 1's position
        
        result = await session.execute(
            """
            SELECT COUNT(*) FROM miner_2_positions 
            WHERE status = 'open'
            """
        )
        count = (await result.fetchone())[0]
        assert count == 1  # Miner 2's position
    
    # 3. Process unstake events
    # Miner 1 unstakes half
    api.add_event(StakeEvent(
        block_number=1003,
        timestamp=now + timedelta(minutes=2),
        netuid=1,
        action=StakeAction.UNDELEGATE,
        coldkey="0x1111...cold1",
        hotkey="0x1234...miner1",
        tao_amount=60.0,  # 20% ROI
        alpha_amount=5.0,  # Half of position
        extrinsic_id="0x789",
        alpha_price_in_tao=12.0,  # Price increased
        alpha_price_in_usd=1.2,
        usd_amount=60.0
    ))
    
    await validator.forward()
    
    # Check position was partially closed
    async with db.session() as session:
        result = await session.execute(
            """
            SELECT status, remaining_alpha FROM miner_1_positions 
            WHERE hotkey = '0x1234...miner1'
            """
        )
        row = await result.fetchone()
        assert row['status'] == 'partial'
        assert row['remaining_alpha'] == 5.0
        
        # Check trade was recorded
        result = await session.execute(
            """
            SELECT COUNT(*) FROM miner_1_trades
            """
        )
        count = (await result.fetchone())[0]
        assert count == 1
    
    # 4. Check monitoring
    metrics = await monitor.get_health_metrics()
    assert metrics.active_connections >= 0
    assert metrics.total_queries > 0
    assert metrics.failed_queries == 0
    
    # 5. Check performance metrics
    stats = await validator.get_miner_performance(uid=1)
    assert stats['total_trades'] == 1
    assert stats['win_rate'] == 100.0  # Profitable trade
    assert stats['current_positions'] == 1  # Still has partial position
    
    # 6. Weight setting
    # Simulate some more activity
    await asyncio.sleep(0.1)  # Let some time pass
    await validator.forward()  # This should update scores
    
    # Weights should reflect performance
    weights = validator.get_weights()
    assert weights[0] > 0  # Miner 1 should have positive weight
    assert weights[1] > 0  # Miner 2 should have positive weight
    assert weights[2] == 0  # Miner 3 has no activity

@pytest.mark.asyncio
async def test_error_recovery(test_env):
    """Test validator error recovery."""
    validator = test_env['validator']
    api = test_env['api']
    
    # Simulate API error
    api.get_stake_events = AsyncMock(side_effect=Exception("API Error"))
    
    # Validator should handle error gracefully
    await validator.forward()
    
    # System should still be operational
    assert validator.is_running()
    
    # Restore API and verify recovery
    api.get_stake_events = AsyncMock(return_value=[])
    await validator.forward()
    
    assert validator.is_running()

@pytest.mark.asyncio
async def test_concurrent_operations(test_env):
    """Test concurrent validator operations."""
    validator = test_env['validator']
    monitor = test_env['monitor']
    
    # Run multiple operations concurrently
    tasks = [
        validator.forward(),
        monitor.get_health_metrics(),
        validator.get_miner_performance(uid=1)
    ]
    
    # Should complete without deadlock
    await asyncio.gather(*tasks)

@pytest.mark.asyncio
async def test_performance_tracking(test_env):
    """Test performance tracking over time."""
    db = test_env['db']
    validator = test_env['validator']
    api = test_env['api']
    
    # Create sequence of events over time
    now = datetime.now(timezone.utc)
    events = []
    
    # Multiple stake/unstake cycles
    for i in range(3):
        # Stake
        events.append(StakeEvent(
            block_number=1000 + i*2,
            timestamp=now + timedelta(hours=i),
            netuid=1,
            action=StakeAction.DELEGATE,
            coldkey="0x1111...cold1",
            hotkey="0x1234...miner1",
            tao_amount=100.0,
            alpha_amount=10.0,
            extrinsic_id=f"0x{i}a",
            alpha_price_in_tao=10.0,
            alpha_price_in_usd=1.0,
            usd_amount=100.0
        ))
        
        # Unstake with varying ROI
        roi = 1.1 + (i * 0.1)  # Increasing ROI
        events.append(StakeEvent(
            block_number=1001 + i*2,
            timestamp=now + timedelta(hours=i, minutes=30),
            netuid=1,
            action=StakeAction.UNDELEGATE,
            coldkey="0x1111...cold1",
            hotkey="0x1234...miner1",
            tao_amount=100.0 * roi,
            alpha_amount=10.0,
            extrinsic_id=f"0x{i}b",
            alpha_price_in_tao=10.0 * roi,
            alpha_price_in_usd=1.0 * roi,
            usd_amount=100.0 * roi
        ))
    
    # Process events in sequence
    for event in events:
        api.add_event(event)
        await validator.forward()
        await asyncio.sleep(0.1)  # Let processing complete
    
    # Check performance metrics
    stats = await validator.get_miner_performance(uid=1)
    assert stats['total_trades'] == 3
    assert stats['win_rate'] == 100.0
    assert stats['avg_roi'] > 20.0  # Average ROI should be significant
    
    # Check performance snapshots
    async with db.session() as session:
        result = await session.execute(
            """
            SELECT COUNT(*) FROM performance_snapshots
            WHERE miner_uid = 1
            """
        )
        count = (await result.fetchone())[0]
        assert count > 0  # Should have performance snapshots 