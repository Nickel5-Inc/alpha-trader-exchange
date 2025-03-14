"""
Test fixtures and data generators for database testing.
"""

import os
import asyncio
import tempfile
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, AsyncGenerator
from dataclasses import replace

import pytest_asyncio

from alphatrade.database.manager import DatabaseManager
from alphatrade.database.models import (
    Position, Trade, PositionStatus, MinerStatus,
    PositionTrade, PerformanceConfig,
    StakeEvent, StakeAction, ApiTrade, PoolData
)
from .mocks.api_mock import MockAlphaAPI

class TestData:
    """Container for test data and utilities."""
    
    def __init__(self):
        """Initialize test data."""
        # SS58 format addresses for testing
        self.coldkey = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"  # cold1
        self.hotkey = "5DD26kC2kxajmwfbbZmVmxhrY9VeeyR1Gpzy9i8wxLUg6zxp"  # miner1
        self.netuid = 1
        self.start_time = datetime.now(timezone.utc)
        
    def generate_stake_event(
        self,
        action: StakeAction,
        timestamp: Optional[datetime] = None,
        tao_amount: float = 100.0,
        alpha_amount: float = 10.0,
        extrinsic_id: str = "0x123",
        alpha_price_in_tao: float = 10.0,
        alpha_price_in_usd: float = 1.0,
        usd_amount: float = 100.0
    ) -> StakeEvent:
        """Generate a test stake event.
        
        Args:
            action (StakeAction): Type of stake action
            timestamp (datetime, optional): Event timestamp
            tao_amount (float): TAO amount for event
            alpha_amount (float): Alpha amount for event
            extrinsic_id (str): Extrinsic ID for event
            alpha_price_in_tao (float): Alpha price in TAO
            alpha_price_in_usd (float): Alpha price in USD
            usd_amount (float): USD amount for event
            
        Returns:
            StakeEvent: Generated event
        """
        if timestamp is None:
            timestamp = self.start_time
            
        return StakeEvent(
            block_number=1000,
            timestamp=timestamp,
            netuid=self.netuid,
            action=action,
            coldkey=self.coldkey,
            hotkey=self.hotkey,
            tao_amount=tao_amount,
            alpha_amount=alpha_amount,
            extrinsic_id=extrinsic_id,
            alpha_price_in_tao=alpha_price_in_tao,
            alpha_price_in_usd=alpha_price_in_usd,
            usd_amount=usd_amount
        )
        
    def generate_position(
        self,
        position_id: Optional[int] = None,
        entry_tao: float = 100.0,
        entry_alpha: float = 10.0,
        status: PositionStatus = PositionStatus.OPEN,
        miner_uid: int = 1
    ) -> Position:
        """Generate a test position.
        
        Args:
            position_id (int, optional): Position ID
            entry_tao (float): Entry TAO amount
            entry_alpha (float): Entry alpha amount
            status (PositionStatus): Position status
            miner_uid (int): Miner's UID
            
        Returns:
            Position: Generated position
        """
        return Position(
            position_id=position_id,
            miner_uid=miner_uid,
            netuid=self.netuid,
            hotkey=self.hotkey,
            entry_block=1000,
            entry_timestamp=self.start_time,
            entry_tao=entry_tao,
            entry_alpha=entry_alpha,
            remaining_alpha=entry_alpha if status != PositionStatus.CLOSED else 0.0,
            status=status
        )
        
    def generate_trade(
        self,
        trade_id: Optional[int] = None,
        exit_tao: float = 120.0,
        exit_alpha: float = 10.0,
        exit_timestamp: Optional[datetime] = None,
        miner_uid: int = 1
    ) -> Trade:
        """Generate a test trade.
        
        Args:
            trade_id (int, optional): Trade ID
            exit_tao (float): Exit TAO amount
            exit_alpha (float): Exit alpha amount
            exit_timestamp (datetime, optional): Trade timestamp
            miner_uid (int): Miner's UID
            
        Returns:
            Trade: Generated trade
        """
        if exit_timestamp is None:
            exit_timestamp = self.start_time + timedelta(hours=1)
            
        return Trade(
            trade_id=trade_id,
            miner_uid=miner_uid,
            netuid=self.netuid,
            hotkey=self.hotkey,
            exit_block=1100,
            exit_timestamp=exit_timestamp,
            exit_tao=exit_tao,
            exit_alpha=exit_alpha
        )
        
    def generate_pool_data(
        self,
        tao_reserve: float = 1000000.0,
        alpha_reserve: float = 100000.0
    ) -> PoolData:
        """Generate test pool data.
        
        Args:
            tao_reserve (float): TAO reserve amount
            alpha_reserve (float): Alpha reserve amount
            
        Returns:
            PoolData: Generated pool data
        """
        return PoolData(
            netuid=self.netuid,
            tao_reserve=tao_reserve,
            alpha_reserve=alpha_reserve,
            alpha_supply=alpha_reserve,
            emission_rate=0.1,
            price=tao_reserve / alpha_reserve
        )

@pytest_asyncio.fixture
async def test_data() -> TestData:
    """Fixture providing test data utilities."""
    return TestData()

@pytest_asyncio.fixture(scope="function")
async def db() -> AsyncGenerator[DatabaseManager, None]:
    """Fixture providing test database."""
    # Create temporary database file
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    
    # Initialize database manager with test config
    config = PerformanceConfig(
        rolling_window_days=7,
        snapshot_interval_hours=24,
        retention_days=30
    )
    
    db = DatabaseManager(db_path, max_connections=1, performance_config=config)
    await db.setup_database()
    
    try:
        yield db
    finally:
        await db.close()
        try:
            os.unlink(db_path)
        except OSError:
            pass

@pytest_asyncio.fixture
async def mock_api() -> MockAlphaAPI:
    """Fixture providing mock API client."""
    return MockAlphaAPI() 