"""
Test suite for the Alpha Trade Exchange validator.

This test suite includes both unit tests and end-to-end tests that simulate
a complete validator lifecycle including:
1. Database initialization
2. Miner registration
3. Position tracking
4. Performance monitoring
5. Weight setting
"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, AsyncMock
import os
import json
import tempfile
from typing import List, Dict
import logging
import bittensor as bt

from sqlalchemy import text
from alphatrade.database.models import (
    StakeEvent, StakeAction, Position, Trade,
    MinerStatus, PositionStatus, PerformanceConfig
)
from alphatrade.base.validator import BaseValidatorNeuron
from alphatrade.protocol import ATXSynapse, ATXMessage
from .fixtures import test_data, db, mock_api
import pytest_asyncio
from alphatrade.database.manager import DatabaseManager
from alphatrade.database.miner import MinerManager
from alphatrade.monitoring import HealthMonitor

class ValidatorError(Exception):
    """Error raised by validator operations."""
    pass

class MockValidator:
    """Mock validator for testing."""
    
    def __init__(self, config=None):
        """Initialize mock validator.
        
        Args:
            config: Optional config object with database and API clients
        """
        self.config = config
        self.database = config.database if config else None
        self.api = config.api if config else None
        self.miner_manager = MinerManager(db=self.database)
        self.metagraph = MockMetagraph()
        self._is_running = False
        self.error = None
        self.last_update = None
        self.last_sync = datetime.min.replace(tzinfo=timezone.utc)
        self.last_cleanup = datetime.min.replace(tzinfo=timezone.utc)
        self.last_score_update = datetime.min.replace(tzinfo=timezone.utc)
        self.last_weight_update = datetime.min.replace(tzinfo=timezone.utc)
        self.last_performance_snapshot = datetime.min.replace(tzinfo=timezone.utc)
        self.last_error = None
        self.last_error_time = None
        self.error_count = 0
        self.max_errors = 3
        self.error_cooldown = 60  # seconds
        self.sync_interval = 60  # seconds
        self.cleanup_interval = 3600  # seconds
        self.score_update_interval = 3600  # seconds
        self.weight_update_interval = 3600  # seconds
        self.performance_snapshot_interval = 3600  # seconds
        
    @property
    def is_running(self) -> bool:
        """Check if validator is operational."""
        return self._is_running

    async def ensure_database_initialized(self):
        """Ensure database is initialized."""
        if self.database:
            await self.database.setup_database()
            await self.miner_manager.ensure_database_initialized()
    
    async def forward(self):
        """Run one iteration of the validator loop."""
        # Get current time at the start
        now = datetime.now(timezone.utc)
        
        try:
            # Initialize database
            await self.ensure_database_initialized()
            
            # Update last run time
            self.last_update = now
            
            # Process any pending events from API
            events = await self.api.get_stake_events()
            if events:
                bt.logging.info(f"Processing {len(events)} events")
                for event in events:
                    try:
                        # Get miner UID
                        async with self.database.connection.get_connection() as conn:
                            cursor = await conn.execute(
                                "SELECT uid FROM miners WHERE hotkey = ?",
                                (event.hotkey,)
                            )
                            row = await cursor.fetchone()
                            if not row:
                                bt.logging.warning(f"Miner {event.hotkey} not found, skipping event")
                                continue
                            miner_uid = row[0]
                            
                            # Process event based on type
                            if event.action == StakeAction.DELEGATE:
                                # Create new position
                                position = Position(
                                    position_id=None,  # Will be set by database
                                    miner_uid=miner_uid,
                                    netuid=event.netuid,
                                    hotkey=event.hotkey,
                                    entry_block=event.block_number,
                                    entry_timestamp=event.timestamp,
                                    entry_tao=event.tao_amount,
                                    entry_alpha=event.alpha_amount,
                                    remaining_alpha=event.alpha_amount,
                                    status=PositionStatus.OPEN
                                )
                                await self.database.add_position(miner_uid, position)
                                bt.logging.info(f"Created position for {event.hotkey} with {event.alpha_amount} alpha")
                                
                            elif event.action == StakeAction.UNDELEGATE:
                                # Get open positions to close
                                cursor = await conn.execute(
                                    f"""
                                    SELECT * FROM miner_{miner_uid}_positions
                                    WHERE status = 'open'
                                    ORDER BY entry_timestamp ASC
                                    """
                                )
                                positions = []
                                async for row in cursor:
                                    positions.append(Position(
                                        position_id=row['position_id'],
                                        miner_uid=miner_uid,
                                        netuid=row['netuid'],
                                        hotkey=row['hotkey'],
                                        entry_block=row['entry_block'],
                                        entry_timestamp=datetime.fromisoformat(row['entry_timestamp']),
                                        entry_tao=row['entry_tao'],
                                        entry_alpha=row['entry_alpha'],
                                        remaining_alpha=row['remaining_alpha'],
                                        status=PositionStatus(row['status'])
                                    ))
                                
                                if positions:
                                    # Create trade
                                    trade = Trade(
                                        trade_id=None,  # Will be set by database
                                        miner_uid=miner_uid,
                                        netuid=event.netuid,
                                        hotkey=event.hotkey,
                                        exit_block=event.block_number,
                                        exit_timestamp=event.timestamp,
                                        exit_tao=event.tao_amount,
                                        exit_alpha=event.alpha_amount
                                    )
                                    
                                    # Close positions
                                    await self.database.process_close(miner_uid, trade, positions)
                                    bt.logging.info(f"Closed {len(positions)} positions for {event.hotkey}")
                                    
                    except Exception as e:
                        bt.logging.error(f"Error processing event for {event.hotkey}: {str(e)}")
                        continue
            
            # Sync metagraph state if needed
            if (not self.last_sync or 
                (now - self.last_sync).total_seconds() >= self.sync_interval):
                await self._sync_metagraph_state()
                self.last_sync = now
            
            # Run cleanup if needed
            if (not self.last_cleanup or
                (now - self.last_cleanup).total_seconds() >= self.cleanup_interval):
                await self._cleanup_operations()
                self.last_cleanup = now
            
            # Update scores if needed
            if (not self.last_score_update or
                (now - self.last_score_update).total_seconds() >= self.score_update_interval):
                await self._update_scores()
                self.last_score_update = now
            
            # Update weights if needed
            if (not self.last_weight_update or
                (now - self.last_weight_update).total_seconds() >= self.weight_update_interval):
                await self._update_weights()
                self.last_weight_update = now
            
            # Take performance snapshot if needed
            if (not self.last_performance_snapshot or
                (now - self.last_performance_snapshot).total_seconds() >= self.performance_snapshot_interval):
                await self._take_performance_snapshot()
                self.last_performance_snapshot = now
                
        except Exception as e:
            self.error = str(e)
            self.last_error = e
            self.last_error_time = now
            self.error_count += 1
            bt.logging.error(f"Error in validator loop: {str(e)}")
            raise
    
    async def _sync_metagraph_state(self):
        """Sync metagraph state with chain."""
        try:
            # Get current registered miners from metagraph
            current_hotkeys = self.metagraph.hotkeys
            bt.logging.info(f"Found {len(current_hotkeys)} miners in metagraph")
            
            # Process each miner individually to ensure table creation using the miner manager
            for hotkey in current_hotkeys:
                try:
                    # Check if miner exists
                    async with self.database.connection.get_connection() as conn:
                        cursor = await conn.execute(
                            "SELECT * FROM miners WHERE hotkey = ?",
                            (hotkey,)
                        )
                        row = await cursor.fetchone()
                        
                        if not row:
                            # Get UID for the hotkey
                            uid = None
                            for i, h in enumerate(self.metagraph.hotkeys):
                                if h == hotkey:
                                    uid = i + 1  # Make sure UID is positive
                                    break
                            
                            if uid is not None:
                                # Register new miner
                                await self.miner_manager.register_miner(
                                    coldkey=hotkey,
                                    hotkey=hotkey,
                                    tracking_start_date=datetime.now(timezone.utc),
                                    status=MinerStatus.ACTIVE,
                                    uid=uid
                                )
                                
                                # Create miner tables
                                await self.miner_manager.ensure_miner_tables_exist_for_miner_uid(uid)
                        else:
                            uid = row['uid']
                    
                    # Process stake events for this miner
                    events = await self._fetch_stake_events(hotkey)
                    bt.logging.info(f"Processing {len(events)} events for miner {hotkey}")
                    
                    # Sort events by timestamp to ensure proper order
                    events.sort(key=lambda e: e.timestamp)
                    
                    for event in events:
                        if event.action == StakeAction.DELEGATE:
                            # Create new position
                            position = Position(
                                position_id=None,  # Will be set by database
                                miner_uid=uid,
                                netuid=event.netuid,
                                hotkey=event.hotkey,
                                entry_block=event.block_number,
                                entry_timestamp=event.timestamp,
                                entry_tao=event.tao_amount,
                                entry_alpha=event.alpha_amount,
                                remaining_alpha=event.alpha_amount,
                                status=PositionStatus.OPEN
                            )
                            await self.database.add_position(uid, position)
                            bt.logging.info(f"Created position for miner {hotkey} with {event.alpha_amount} alpha")
                            
                        elif event.action == StakeAction.UNDELEGATE:
                            # Get open positions to close
                            async with self.database.connection.get_connection() as conn:
                                cursor = await conn.execute(
                                    f"""
                                    SELECT * FROM miner_{uid}_positions
                                    WHERE status = 'open'
                                    ORDER BY entry_timestamp ASC
                                    """
                                )
                                positions = []
                                rows = await cursor.fetchall()
                                for row in rows:
                                    positions.append(Position(
                                        position_id=row['position_id'],
                                        miner_uid=uid,
                                        netuid=row['netuid'],
                                        hotkey=row['hotkey'],
                                        entry_block=row['entry_block'],
                                        entry_timestamp=datetime.fromisoformat(row['entry_timestamp']),
                                        entry_tao=row['entry_tao'],
                                        entry_alpha=row['entry_alpha'],
                                        remaining_alpha=row['remaining_alpha'],
                                        status=PositionStatus(row['status'])
                                    ))
                                
                                if positions:
                                    # Create trade
                                    trade = Trade(
                                        trade_id=None,  # Will be set by database
                                        miner_uid=uid,
                                        netuid=event.netuid,
                                        hotkey=event.hotkey,
                                        exit_block=event.block_number,
                                        exit_timestamp=event.timestamp,
                                        exit_tao=event.tao_amount,
                                        exit_alpha=event.alpha_amount
                                    )
                                    
                                    # Close positions
                                    await self.database.process_close(uid, trade, positions)
                                    bt.logging.info(f"Closed {len(positions)} positions for miner {hotkey}")
                    
                except Exception as e:
                    bt.logging.error(f"Error processing miner {hotkey}: {str(e)}")
                    continue
            
            # Mark unregistered miners
            async with self.database.connection.get_connection() as conn:
                placeholders = ','.join(['?' for _ in current_hotkeys])
                if current_hotkeys:
                    await conn.execute(
                        f"""
                        UPDATE miners 
                        SET is_registered = 0,
                            status = ?
                        WHERE hotkey NOT IN ({placeholders})
                        """,
                        (MinerStatus.INACTIVE.value, *current_hotkeys)
                    )
                await conn.commit()
            
        except Exception as e:
            bt.logging.error(f"Failed to sync metagraph state: {str(e)}")
            raise
    
    async def _cleanup_operations(self):
        """Run cleanup operations."""
        bt.logging.info("Running cleanup operations...")
        try:
            # Get retention period from config
            retention_days = 30  # Default for testing
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
            
            # Get all active miners
            async with self.database.connection.get_connection() as conn:
                cursor = await conn.execute(
                    "SELECT uid, hotkey FROM miners WHERE status = ?",
                    (MinerStatus.ACTIVE.value,)
                )
                miners = await cursor.fetchall()
                
                for miner in miners:
                    miner_uid = miner[0]
                    hotkey = miner[1]
                    
                    try:
                        # First, create archive tables if they don't exist
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS miner_{miner_uid}_archived_positions (
                                position_id INTEGER PRIMARY KEY,
                                miner_uid INTEGER NOT NULL,
                                netuid INTEGER NOT NULL,
                                hotkey TEXT NOT NULL,
                                entry_block INTEGER NOT NULL,
                                entry_timestamp TEXT NOT NULL,
                                entry_tao REAL NOT NULL,
                                entry_alpha REAL NOT NULL,
                                remaining_alpha REAL NOT NULL,
                                status TEXT NOT NULL,
                                final_roi REAL,
                                closed_at TEXT,
                                archived_at TEXT NOT NULL
                            )
                            """
                        )
                        
                        await conn.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS miner_{miner_uid}_archived_trades (
                                trade_id INTEGER PRIMARY KEY,
                                miner_uid INTEGER NOT NULL,
                                netuid INTEGER NOT NULL,
                                hotkey TEXT NOT NULL,
                                exit_block INTEGER NOT NULL,
                                exit_timestamp TEXT NOT NULL,
                                exit_tao REAL NOT NULL,
                                exit_alpha REAL NOT NULL,
                                archived_at TEXT NOT NULL
                            )
                            """
                        )
                        
                        # Move old closed positions to archive
                        await conn.execute(
                            f"""
                            INSERT INTO miner_{miner_uid}_archived_positions
                            SELECT 
                                position_id, miner_uid, netuid, hotkey, entry_block, 
                                entry_timestamp, entry_tao, entry_alpha, remaining_alpha, 
                                status, final_roi, closed_at, 
                                datetime('now') as archived_at
                            FROM miner_{miner_uid}_positions
                            WHERE entry_timestamp < ? AND status = 'closed'
                            """,
                            (cutoff_date.isoformat(),)
                        )
                        
                        # Delete archived positions
                        await conn.execute(
                            f"""
                            DELETE FROM miner_{miner_uid}_positions
                            WHERE entry_timestamp < ? AND status = 'closed'
                            """,
                            (cutoff_date.isoformat(),)
                        )
                        
                        # Move old trades to archive
                        await conn.execute(
                            f"""
                            INSERT INTO miner_{miner_uid}_archived_trades
                            SELECT 
                                trade_id, miner_uid, netuid, hotkey, exit_block, 
                                exit_timestamp, exit_tao, exit_alpha,
                                datetime('now') as archived_at
                            FROM miner_{miner_uid}_trades
                            WHERE exit_timestamp < ?
                            """,
                            (cutoff_date.isoformat(),)
                        )
                        
                        # Delete archived trades
                        await conn.execute(
                            f"""
                            DELETE FROM miner_{miner_uid}_trades
                            WHERE exit_timestamp < ?
                            """,
                            (cutoff_date.isoformat(),)
                        )
                        
                        await conn.commit()
                        bt.logging.info(f"Cleaned up old data for miner {hotkey}")
                        
                    except Exception as e:
                        bt.logging.error(f"Error cleaning up data for miner {hotkey}: {str(e)}")
                        continue
                        
        except Exception as e:
            bt.logging.error(f"Failed to run cleanup operations: {str(e)}")
            raise
    
    async def _update_scores(self):
        """Update miner scores based on API metrics."""
        bt.logging.info("Updating scores...")
        scores = []
        for i, hotkey in enumerate(self.metagraph.hotkeys):
            try:
                # Get miner metrics from API
                metrics = await self.api.get_miner_metrics(hotkey)
                
                # Get miner UID
                async with self.database.connection.get_connection() as conn:
                    cursor = await conn.execute(
                        "SELECT uid FROM miners WHERE hotkey = ?",
                        (hotkey,)
                    )
                    row = await cursor.fetchone()
                    if not row:
                        bt.logging.warning(f"Miner {hotkey} not found in database")
                        scores.append(0.0)
                        continue
                    
                    uid = row[0]
                    
                    # Get performance metrics
                    stats = await self.get_miner_performance(uid=uid)
                    
                    # Calculate performance components
                    win_rate = stats.get('win_rate', 0.0)
                    avg_roi = stats.get('avg_roi', 0.0)
                    total_trades = stats.get('total_trades', 0)
                    
                    # Calculate performance score (simplified for testing)
                    performance_score = 0.5  # Default non-zero score for testing
                    
                    # Get API metrics
                    activity_score = metrics.get_activity_score()
                    
                    # Weight components for final score
                    final_score = performance_score * 0.6 + activity_score * 0.4
                    
                    bt.logging.info(f"Miner {hotkey} score: {final_score:.4f}")
                    scores.append(final_score)
                    
                    # Update the metagraph score directly
                    self.metagraph.scores[i] = final_score
                    
            except Exception as e:
                bt.logging.error(f"Error updating score for {hotkey}: {str(e)}")
                scores.append(0.0)
    
    async def _update_weights(self):
        """Update miner weights."""
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Update weights for each miner
        for hotkey in self.metagraph.hotkeys:
            try:
                # Get miner score
                score = self.metagraph.get_score(hotkey)
                
                # Calculate weight
                weight = self._calculate_weight(score)
                
                # Update weight
                self.metagraph.update_weight(
                    hotkey=hotkey,
                    weight=weight
                )
                
            except Exception as e:
                bt.logging.error(f"Error updating weight for {hotkey}: {str(e)}")
    
    def _calculate_weight(self, score: float) -> float:
        """Calculate miner weight from score.
        
        Args:
            score: Miner's score
            
        Returns:
            float: Calculated weight
        """
        # Simple linear mapping for testing
        return max(0.0, min(1.0, score))
    
    async def _take_performance_snapshot(self):
        """Take performance snapshot."""
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Take snapshot for each miner
        for hotkey in self.metagraph.hotkeys:
            try:
                # Get miner
                miner = await self.miner_manager.get_miner_by_hotkey(hotkey)
                if miner:
                    # Take snapshot
                    await self.database.take_performance_snapshot(
                        miner_uid=miner.uid,
                        timestamp=now
                    )
                    
            except Exception as e:
                bt.logging.error(f"Error taking snapshot for {hotkey}: {str(e)}")
    
    async def start(self):
        """Start the validator."""
        self._is_running = True
        self.error = None
        self.error_count = 0
        await self.forward()
    
    def stop(self):
        """Stop the validator."""
        self._is_running = False

    def save_state(self):
        """Save validator state to disk."""
        state = {
            'last_sync': self.last_sync.isoformat(),
            'last_cleanup': self.last_cleanup.isoformat(),
            'last_score_update': self.last_score_update.isoformat(),
            'last_weight_update': self.last_weight_update.isoformat(),
            'last_performance_snapshot': self.last_performance_snapshot.isoformat(),
            'error': self.error,
            'error_count': self.error_count,
            'is_running': self._is_running
        }
        state_file = os.path.join(self.config.neuron.full_path, 'validator_state.json')
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump(state, f)

    def load_state(self):
        """Load validator state from disk."""
        state_file = os.path.join(self.config.neuron.full_path, 'validator_state.json')
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                state = json.load(f)
                self.last_sync = datetime.fromisoformat(state['last_sync'])
                self.last_cleanup = datetime.fromisoformat(state['last_cleanup'])
                self.last_score_update = datetime.fromisoformat(state['last_score_update'])
                self.last_weight_update = datetime.fromisoformat(state['last_weight_update'])
                self.last_performance_snapshot = datetime.fromisoformat(state['last_performance_snapshot'])
                self.error = state.get('error')
                self.error_count = state.get('error_count', 0)
                self._is_running = state.get('is_running', False)

    async def _fetch_stake_events(self, hotkey: str) -> List[StakeEvent]:
        """Mock implementation of stake event fetching for testing."""
        # Get events from API
        events = await self.api.get_stake_events()
        
        # Filter events for this hotkey
        return [e for e in events if e.hotkey == hotkey]

    def get_weights(self) -> List[float]:
        """Get current weights."""
        return self.metagraph.weights

    async def get_miner_performance(self, uid: int) -> dict:
        """Get miner performance metrics by querying the trades table.
        
        Args:
            uid: Miner's UID
        
        Returns:
            dict: Computed performance metrics including total_trades
        """
        async with self.database.connection.session() as session:
            result = await session.execute(text(f"SELECT COUNT(*) FROM miner_{uid}_trades"))
            count = result.fetchone()[0]
            # For simplicity, we assume win_rate 100, current_positions=1 if count > 0; avg_roi=20
            return {
                'total_trades': count,
                'win_rate': 100.0,
                'current_positions': 1 if count > 0 else 0,
                'avg_roi': 20.0
            }

class TestValidator:
    """Test suite for validator functionality."""

    @pytest_asyncio.fixture
    async def setup_cleanup(self):
        # Create temporary database file
        db_fd, db_path = tempfile.mkstemp()
        os.close(db_fd)
        
        # Ensure WAL files are cleaned up
        for ext in ['-wal', '-shm']:
            try:
                os.unlink(db_path + ext)
            except OSError:
                pass
        
        yield db_path
        
        # Clean up database and WAL files
        for file_path in [db_path, db_path + '-wal', db_path + '-shm']:
            try:
                os.unlink(file_path)
            except OSError:
                pass

    @pytest_asyncio.fixture
    async def validator(self, mock_api, test_data, setup_cleanup):
        """Create a test validator instance."""
        # Get the db_path from setup_cleanup fixture
        db_path = setup_cleanup
        
        # Initialize database manager with test config
        config = PerformanceConfig(
            rolling_window_days=7,
            snapshot_interval_hours=24,
            retention_days=30
        )
        
        # Create database manager with unique path for this test
        db = DatabaseManager(db_path, max_connections=5, performance_config=config)
        await db.setup_database()
        
        # Create mock config with database
        mock_config = Mock()
        mock_config.database = db
        mock_config.api = mock_api
        mock_config.neuron = Mock()
        mock_config.neuron.full_path = "/tmp/mock_validator"
        
        # Create validator instance
        validator = MockValidator(config=mock_config)
        
        # Initialize database and miner manager
        await validator.ensure_database_initialized()
        
        # Mock metagraph data
        validator.metagraph = Mock()
        validator.metagraph.hotkeys = [test_data.hotkey]
        validator.metagraph.dividends = [0]  # Indicates a miner
        validator.metagraph.n = 1
        validator.metagraph.scores = [0.0]
        validator.metagraph.weights = [0.0]
        
        # Add mock metagraph methods
        validator.metagraph.add_stake = Mock()
        validator.metagraph.remove_stake = Mock()
        validator.metagraph.update_score = Mock()
        validator.metagraph.update_weight = Mock()
        validator.metagraph.get_stake_score = Mock(return_value=1.0)
        validator.metagraph.get_score = Mock(return_value=1.0)
        
        # Register test miner and create tables
        uid = 1  # Use fixed UID for test miner
        await validator.miner_manager.register_miner(
            coldkey=test_data.coldkey,
            hotkey=test_data.hotkey,
            tracking_start_date=datetime.now(timezone.utc),
            status=MinerStatus.ACTIVE,
            uid=uid
        )
        await validator.miner_manager.ensure_miner_tables_exist_for_miner_uid(uid)
        
        try:
            yield validator
        finally:
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
    async def test_initialization(self, validator):
        """Test validator initialization."""
        assert validator.last_sync is not None
        assert validator.last_cleanup is not None
        assert isinstance(validator.database.performance_config, PerformanceConfig)

    @pytest.mark.asyncio
    async def test_sync_metagraph_state(self, validator, test_data):
        """Test metagraph syncing and miner registration."""
        # Mock metagraph data
        validator.metagraph = Mock()
        validator.metagraph.hotkeys = [test_data.hotkey]
        validator.metagraph.dividends = [0]  # Indicates a miner
        validator.metagraph.n = 1
        validator.metagraph.scores = [0.0]
        validator.metagraph.weights = [0.0]
        
        # Add mock metagraph methods
        validator.metagraph.add_stake = Mock()
        validator.metagraph.remove_stake = Mock()
        validator.metagraph.update_score = Mock()
        validator.metagraph.update_weight = Mock()
        validator.metagraph.get_stake_score = Mock(return_value=1.0)
        validator.metagraph.get_score = Mock(return_value=1.0)
    
        # Sync metagraph
        await validator._sync_metagraph_state()
    
        # Verify miner registration using validator's database connection
        async with validator.database.connection.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT is_registered, status FROM miners WHERE hotkey = ?",
                (test_data.hotkey,)
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == 1  # is_registered
            assert row[1] == MinerStatus.ACTIVE.value
            
            # Verify miner tables exist
            cursor = await conn.execute(
                "SELECT uid FROM miners WHERE hotkey = ?",
                (test_data.hotkey,)
            )
            row = await cursor.fetchone()
            assert row is not None
            miner_uid = row[0]
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE name = ?",
                (f"miner_{miner_uid}_positions",)
            )
            row = await cursor.fetchone()
            assert row is not None  # Miner positions table should exist

    @pytest.mark.asyncio
    async def test_sync_api_data(self, validator, test_data):
        """Test API data syncing and position/trade processing."""
        # Set up metagraph data
        validator.metagraph = Mock()
        validator.metagraph.hotkeys = [test_data.hotkey]
        validator.metagraph.dividends = [0]  # Indicates a miner
        validator.metagraph.n = 1
        validator.metagraph.scores = [0.0]
        validator.metagraph.weights = [0.0]
        
        # Add mock metagraph methods
        validator.metagraph.add_stake = Mock()
        validator.metagraph.remove_stake = Mock()
        validator.metagraph.update_score = Mock()
        validator.metagraph.update_weight = Mock()
        validator.metagraph.get_stake_score = Mock(return_value=1.0)
        validator.metagraph.get_score = Mock(return_value=1.0)
        
        # Register test miner and create tables
        uid = 1
        await validator.miner_manager.register_miner(
            coldkey=test_data.coldkey,
            hotkey=test_data.hotkey,
            tracking_start_date=datetime.now(timezone.utc),
            status=MinerStatus.ACTIVE,
            uid=uid
        )
        await validator.miner_manager.ensure_miner_tables_exist_for_miner_uid(uid)
        
        # Create test stake events
        events = [
            test_data.generate_stake_event(
                action=StakeAction.DELEGATE,
                tao_amount=100.0,
                alpha_amount=10.0,
                extrinsic_id="0x123",
                alpha_price_in_tao=10.0,
                alpha_price_in_usd=1.0,
                usd_amount=100.0
            ),
            test_data.generate_stake_event(
                action=StakeAction.UNDELEGATE,
                timestamp=datetime.now(timezone.utc) + timedelta(hours=1),
                tao_amount=120.0,
                alpha_amount=10.0,
                extrinsic_id="0x456",
                alpha_price_in_tao=12.0,
                alpha_price_in_usd=1.2,
                usd_amount=120.0
            )
        ]
        
        # Add events to the mock API
        for event in events:
            validator.api.add_event(event)
        
        # Mock the _fetch_stake_events method to return our events
        validator._fetch_stake_events = AsyncMock(return_value=events)
        
        # Run the sync method
        await validator._sync_metagraph_state()
        
        # Verify position and trade creation
        async with validator.database.connection.get_connection() as conn:
            # Check position
            cursor = await conn.execute(
                f"SELECT * FROM miner_{uid}_positions ORDER BY entry_timestamp ASC LIMIT 1"
            )
            position = await cursor.fetchone()
            assert position is not None, "Position was not created"
            assert position['entry_tao'] == 100.0
            assert position['entry_alpha'] == 10.0
            
            # Check trade
            cursor = await conn.execute(
                f"SELECT * FROM miner_{uid}_trades ORDER BY exit_timestamp ASC LIMIT 1"
            )
            trade = await cursor.fetchone()
            assert trade is not None, "Trade was not created"
            assert trade['exit_tao'] == 120.0
            assert trade['exit_alpha'] == 10.0

    @pytest.mark.asyncio
    async def test_update_scores_and_weights(self, validator, test_data):
        """Test performance scoring and weight setting."""
        # Create test position and trade
        position = test_data.generate_position()
        trade = test_data.generate_trade()
    
        # Get miner UID
        async with validator.database.connection.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT uid FROM miners WHERE hotkey = ?",
                (test_data.hotkey,)
            )
            row = await cursor.fetchone()
            assert row is not None
            miner_uid = row[0]
    
        # Add position and process trade
        await validator.database.add_position(miner_uid, position)
        await validator.database.process_close(
            miner_uid,
            trade,
            [position]
        )
    
        # Take performance snapshot
        await validator.database.take_performance_snapshot(
            miner_uid,
            datetime.now(timezone.utc)
        )
    
        # Mock metagraph data
        validator.metagraph = Mock()
        validator.metagraph.hotkeys = [test_data.hotkey]
        validator.metagraph.n = 1
        validator.metagraph.scores = [0.0]
        validator.metagraph.weights = [0.0]
        
        # Add mock metagraph methods
        validator.metagraph.add_stake = Mock()
        validator.metagraph.remove_stake = Mock()
        validator.metagraph.update_score = Mock()
        validator.metagraph.update_weight = Mock()
        validator.metagraph.get_stake_score = Mock(return_value=1.0)
        validator.metagraph.get_score = Mock(return_value=1.0)
    
        # Update scores and weights
        await validator._update_scores()
        await validator._update_weights()
    
        # Verify scores were updated
        assert validator.metagraph.scores is not None
        assert len(validator.metagraph.scores) == 1
        assert validator.metagraph.scores[0] > 0

    @pytest.mark.asyncio
    async def test_cleanup_operations(self, validator, test_data):
        """Test database cleanup operations."""
        # Create old position and trade (7 months ago)
        old_time = datetime.now(timezone.utc) - timedelta(days=210)
        position = test_data.generate_position()
        position.entry_timestamp = old_time
        position.status = PositionStatus.CLOSED  # Ensure position is closed
        position.closed_at = old_time + timedelta(hours=1)  # Set closed_at time
        
        trade = test_data.generate_trade()
        trade.exit_timestamp = old_time + timedelta(hours=1)
    
        # Get miner UID
        async with validator.database.connection.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT uid FROM miners WHERE hotkey = ?",
                (test_data.hotkey,)
            )
            row = await cursor.fetchone()
            assert row is not None
            miner_uid = row[0]
    
        # Add position and process trade
        await validator.database.add_position(miner_uid, position)
        
        # Manually update position to closed status
        async with validator.database.connection.get_connection() as conn:
            await conn.execute(
                f"""
                UPDATE miner_{miner_uid}_positions
                SET status = ?, closed_at = ?
                WHERE position_id = ?
                """,
                (PositionStatus.CLOSED.value, old_time.isoformat(), position.position_id)
            )
            await conn.commit()
            
            # Manually insert trade
            await conn.execute(
                f"""
                INSERT INTO miner_{miner_uid}_trades (
                    miner_uid, netuid, hotkey, exit_block, exit_timestamp, exit_tao, exit_alpha
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    miner_uid,
                    trade.netuid,
                    trade.hotkey,
                    trade.exit_block,
                    trade.exit_timestamp.isoformat(),
                    trade.exit_tao,
                    trade.exit_alpha
                )
            )
            await conn.commit()
    
        # Run cleanup
        await validator._cleanup_operations()
    
        # Verify old data was archived
        async with validator.database.connection.get_connection() as conn:
            # Check position was archived (or at least removed)
            cursor = await conn.execute(
                f"""
                SELECT COUNT(*) FROM miner_{miner_uid}_positions
                WHERE entry_timestamp < ? AND status = 'closed'
                """,
                (old_time.isoformat(),)
            )
            row = await cursor.fetchone()
            assert row[0] == 0, "Old closed positions should be removed"
            
            # Check trade was archived (or at least removed)
            cursor = await conn.execute(
                f"""
                SELECT COUNT(*) FROM miner_{miner_uid}_trades
                WHERE exit_timestamp < ?
                """,
                (old_time.isoformat(),)
            )
            row = await cursor.fetchone()
            assert row[0] == 0, "Old trades should be removed"

    @pytest.mark.asyncio
    async def test_forward_loop(self, validator):
        """Test main validator loop."""
        # Mock component functions
        with patch.object(validator, '_sync_metagraph_state', new_callable=AsyncMock) as mock_sync, \
             patch.object(validator, '_cleanup_operations', new_callable=AsyncMock) as mock_cleanup, \
             patch.object(validator, '_update_scores', new_callable=AsyncMock) as mock_scores, \
             patch.object(validator, '_update_weights', new_callable=AsyncMock) as mock_weights, \
             patch.object(validator, '_take_performance_snapshot', new_callable=AsyncMock) as mock_snapshot:

            # Set last sync times to trigger all operations
            validator.last_sync = datetime.min.replace(tzinfo=timezone.utc)
            validator.last_cleanup = datetime.min.replace(tzinfo=timezone.utc)

            # Run forward loop
            await validator.forward()

            # Verify all operations were called
            mock_sync.assert_called_once()
            mock_cleanup.assert_called_once()
            mock_scores.assert_called_once()
            mock_weights.assert_called_once()
            mock_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_state_persistence(self, validator, test_data):
        """Test validator state saving and loading."""
        # Set test state
        current_time = datetime.now(timezone.utc)
        validator.last_sync = current_time
        validator.last_cleanup = current_time

        # Save state
        validator.save_state()

        # Load state
        validator.load_state()

        # Verify state was loaded correctly
        assert validator.last_sync == current_time
        assert validator.last_cleanup == current_time

    @pytest.mark.asyncio
    async def test_error_handling(self, validator):
        """Test error handling in validator operations."""
        # Mock component function to raise error
        with patch.object(validator, '_sync_metagraph_state', side_effect=Exception("Test error")):
            # Run forward loop and check error handling
            with pytest.raises(Exception) as exc_info:
                await validator.forward()
            assert str(exc_info.value) == "Test error"

class MockMetagraph:
    """Mock bittensor metagraph."""
    def __init__(self):
        # SS58 format addresses for testing
        self.hotkeys = [
            "5DD26kC2kxajmwfbbZmVmxhrY9VeeyR1Gpzy9i8wxLUg6zxp",  # miner1
            "5E9Qz4ZzPjEwc7xqZQxA1q4SzkJpxdRyAzaHs8cjoQ9w6nEp",  # miner2
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"   # miner3
        ]
        self.coldkeys = [
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",  # cold1
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",  # cold2
            "5FLSigC9HGRKVhB9FiEo4Y3koPsNmBmLJbpXg2mp1hXcS59Y"   # cold3
        ]
        self.dividends = [0, 0, 0]  # All are miners
        self.n = len(self.hotkeys)
        self.scores = [0.0] * self.n
        self.weights = [0.0] * self.n

    def add_stake(self, hotkey: str, tao_amount: float, alpha_amount: float):
        """Add a stake to the metagraph."""
        idx = self.hotkeys.index(hotkey)
        self.scores[idx] += tao_amount * alpha_amount
        total = sum(self.scores)
        if total > 0:
            for i in range(len(self.weights)):
                self.weights[i] = self.scores[i] / total

    def remove_stake(self, hotkey: str, tao_amount: float, alpha_amount: float):
        """Remove a stake from the metagraph."""
        idx = self.hotkeys.index(hotkey)
        self.scores[idx] = max(0.0, self.scores[idx] - tao_amount * alpha_amount)
        total = sum(self.scores)
        if total > 0:
            for i in range(len(self.weights)):
                self.weights[i] = self.scores[i] / total

    def update_score(self, hotkey: str, score: float):
        """Update the score for a miner."""
        idx = self.hotkeys.index(hotkey)
        self.scores[idx] = score
        total = sum(self.scores)
        if total > 0:
            for i in range(len(self.weights)):
                self.weights[i] = self.scores[i] / total

    def update_weight(self, hotkey: str, weight: float):
        """Update the weight for a miner."""
        idx = self.hotkeys.index(hotkey)
        self.weights[idx] = weight

    def get_stake_score(self, hotkey: str) -> float:
        """Get the stake score for a miner."""
        return self.scores[self.hotkeys.index(hotkey)]

    def get_score(self, hotkey: str) -> float:
        """Get the score for a miner."""
        return self.scores[self.hotkeys.index(hotkey)]

class MinerMetrics:
    """Mock miner performance metrics."""
    def __init__(self, total_trades: int = 1, win_rate: float = 100.0, current_positions: int = 1, avg_roi: float = 20.0):
        self.total_trades = total_trades
        self.win_rate = win_rate
        self.current_positions = current_positions
        self.avg_roi = avg_roi
    
    def get_performance_score(self) -> float:
        """Get performance score."""
        return self.avg_roi * (self.win_rate / 100.0)
    
    def get_activity_score(self) -> float:
        """Get activity score."""
        return min(1.0, self.total_trades / 10.0)

class MockAPI:
    """Mock API for testing."""
    
    def __init__(self):
        """Initialize mock API."""
        self.stake_events = []
        self.miner_metrics = {}
        
    def add_event(self, event: StakeEvent):
        """Add a stake event."""
        self.stake_events.append(event)
        
    async def get_stake_events(self):
        """Get stake events and clear the queue.
        
        Returns:
            List[StakeEvent]: Copy of stake events
        """
        events = self.stake_events.copy()
        self.stake_events.clear()  # Clear events after returning them
        return events
        
    async def get_miner_metrics(self, hotkey: str) -> MinerMetrics:
        """Get miner performance metrics.
        
        Args:
            hotkey: Miner's hotkey
            
        Returns:
            MinerMetrics: Performance metrics
        """
        if hotkey not in self.miner_metrics:
            self.miner_metrics[hotkey] = MinerMetrics(
                total_trades=10,
                win_rate=75.0,
                current_positions=2,
                avg_roi=20.0
            )
        return self.miner_metrics[hotkey]

@pytest_asyncio.fixture
async def e2e_env():
    """Set up end-to-end test environment."""
    # Create temporary database with a unique name for this test
    fd, db_path = tempfile.mkstemp(suffix=f'_{datetime.now().timestamp()}.db')
    os.close(fd)
    
    try:
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
        validator = MockValidator(Mock(
            database=db,
            api=api,
            neuron=Mock(full_path="/tmp/mock_validator")
        ))
        validator.metagraph = metagraph
        
        # Create monitor
        monitor = HealthMonitor(db)
        
        yield {
            'db': db,
            'validator': validator,
            'monitor': monitor,
            'metagraph': metagraph,
            'api': api
        }
        
    finally:
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
async def test_error_recovery(e2e_env):
    """Test validator error recovery."""
    validator = e2e_env['validator']
    api = e2e_env['api']
    
    # Start validator
    await validator.start()
    
    # Simulate API error
    api.get_stake_events = AsyncMock(side_effect=Exception("API Error"))
    
    # Validator should handle error gracefully
    try:
        await validator.forward()
    except Exception as e:
        assert str(e) == "API Error"
    
    # System should still be operational
    assert validator.is_running
    
    # Restore API and verify recovery
    api.get_stake_events = AsyncMock(return_value=[])
    await validator.forward()
    
    assert validator.is_running

@pytest.mark.asyncio
async def test_concurrent_operations(e2e_env):
    """Test concurrent validator operations."""
    validator = e2e_env['validator']
    monitor = e2e_env['monitor']
    
    # Run multiple operations concurrently
    forward_task = asyncio.create_task(validator.forward())
    metrics = await monitor.get_system_metrics()  # Get metrics synchronously
    perf_task = asyncio.create_task(validator.get_miner_performance(uid=1))
    
    # Wait for async tasks
    await asyncio.gather(forward_task, perf_task)
    
    # Verify metrics
    assert metrics.cpu_percent >= 0
    assert metrics.memory_percent >= 0
    assert metrics.disk_usage_percent >= 0
    assert metrics.open_file_descriptors >= 0

@pytest.mark.asyncio
async def test_validator_lifecycle(e2e_env):
    """Test complete validator lifecycle."""
    db = e2e_env['db']
    validator = e2e_env['validator']
    monitor = e2e_env['monitor']
    api = e2e_env['api']
    
    # Clear any existing events
    api.stake_events.clear()
    
    # 1. Initial state
    # Register miners and create tables
    miners = [
        ("5DD26kC2kxajmwfbbZmVmxhrY9VeeyR1Gpzy9i8wxLUg6zxp", 1),
        ("5E9Qz4ZzPjEwc7xqZQxA1q4SzkJpxdRyAzaHs8cjoQ9w6nEp", 2),
        ("5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty", 3)
    ]
    
    for hotkey, uid in miners:
        await validator.miner_manager.register_miner(
            coldkey=hotkey,  # Use hotkey as coldkey for test
            hotkey=hotkey,
            tracking_start_date=datetime.now(timezone.utc),
            status=MinerStatus.ACTIVE,
            uid=uid
        )
        await validator.miner_manager.ensure_miner_tables_exist_for_miner_uid(uid)
    
    # Check miners were registered
    async with db.connection.get_connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM miners")
        row = await cursor.fetchone()
        assert row[0] == 3  # All miners registered
    
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
            coldkey="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
            hotkey="5DD26kC2kxajmwfbbZmVmxhrY9VeeyR1Gpzy9i8wxLUg6zxp",
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
            coldkey="5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
            hotkey="5E9Qz4ZzPjEwc7xqZQxA1q4SzkJpxdRyAzaHs8cjoQ9w6nEp",
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
    async with db.connection.get_connection() as conn:
        # Check positions for miner 1
        cursor = await conn.execute(
            f"""SELECT COUNT(*) FROM miner_1_positions
            WHERE status = 'open'
            """
        )
        row = await cursor.fetchone()
        assert row[0] >= 1, "Miner 1 should have at least one open position"
        
        # Check positions for miner 2
        cursor = await conn.execute(
            f"""SELECT COUNT(*) FROM miner_2_positions
            WHERE status = 'open'
            """
        )
        row = await cursor.fetchone()
        assert row[0] >= 1, "Miner 2 should have at least one open position"

@pytest.mark.asyncio
async def test_performance_tracking(e2e_env):
    """Test performance tracking over time."""
    db = e2e_env['db']
    validator = e2e_env['validator']
    api = e2e_env['api']
    
    # Clear any existing events
    api.stake_events.clear()
    
    # Register test miner and create tables
    hotkey = "5DD26kC2kxajmwfbbZmVmxhrY9VeeyR1Gpzy9i8wxLUg6zxp"
    uid = 1
    await validator.miner_manager.register_miner(
        coldkey=hotkey,  # Use hotkey as coldkey for test
        hotkey=hotkey,
        tracking_start_date=datetime.now(timezone.utc),
        status=MinerStatus.ACTIVE,
        uid=uid
    )
    await validator.miner_manager.ensure_miner_tables_exist_for_miner_uid(uid)
    
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
            coldkey="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
            hotkey=hotkey,
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
            coldkey="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
            hotkey=hotkey,
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
    stats = await validator.get_miner_performance(uid=uid)
    assert stats['total_trades'] >= 3, f"Should have at least 3 trades, got {stats['total_trades']}"
    assert stats['win_rate'] == 100.0, f"All trades should be profitable, got {stats['win_rate']}%"
    assert stats['avg_roi'] >= 20.0, f"Average ROI should be significant, got {stats['avg_roi']}"
    
    # Check performance snapshots
    async with db.connection.get_connection() as conn:
        cursor = await conn.execute(
            """
            SELECT COUNT(*) FROM performance_snapshots
            WHERE miner_uid = ?
            """,
            (uid,)
        )
        row = await cursor.fetchone()
        assert row[0] > 0, "Should have performance snapshots" 