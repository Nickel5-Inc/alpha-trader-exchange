"""
Data models for the Alpha Trade Exchange subnet.

This module contains all data models used across the system, organized by domain:
1. Core Models - Basic data structures used throughout the system
2. Database Models - Models for database entities and relationships
3. API Models - Models for API interactions and events
4. Performance Models - Models for tracking and scoring performance
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, List, Dict
import bittensor as bt

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase

# Base class for SQLAlchemy models
class Base(AsyncAttrs, DeclarativeBase):
    """Base class for SQLAlchemy models."""
    pass

# Core Models
class StakeAction(Enum):
    """Enum for stake action types"""
    DELEGATE = "DELEGATE"
    UNDELEGATE = "UNDELEGATE"
    STAKE_ADDED = "STAKE_ADDED"
    STAKE_REMOVED = "STAKE_REMOVED"

@dataclass
class StakeEvent:
    """Data class for storing stake/unstake events"""
    block_number: int
    timestamp: datetime
    netuid: int  # Subnet ID
    action: StakeAction
    coldkey: str  # nominator SS58 address
    hotkey: str  # delegate SS58 address
    tao_amount: float  # amount in TAO
    alpha_amount: float  # alpha amount
    extrinsic_id: str  # Unique identifier for the event
    alpha_price_in_tao: float  # Current alpha price in TAO
    alpha_price_in_usd: float  # Current alpha price in USD
    usd_amount: float  # USD value of the TAO amount

@dataclass
class ApiTrade:
    """Data class for storing processed trade information from API"""
    timestamp_enter: datetime
    timestamp_exit: datetime
    netuid: int
    coldkey: str
    hotkey: str  # Track which validator was used
    tao_in: float
    alpha_received: float
    tao_out: float
    alpha_returned: float
    roi_tao: float  # Absolute TAO gain/loss (not percentage)
    duration: timedelta
    extrinsic_id_enter: str  # Entry transaction ID
    extrinsic_id_exit: str   # Exit transaction ID

# Database Models
class PositionStatus(Enum):
    """Status of a position"""
    OPEN = "open"           # Position is fully open
    CLOSED = "closed"       # Position is fully closed
    PARTIAL = "partial"     # Position is partially closed

class MinerStatus(Enum):
    """Status of a miner in the network"""
    ACTIVE = "active"       # Miner is currently active
    INACTIVE = "inactive"   # Miner has not traded recently
    BLACKLISTED = "blacklisted"  # Miner has been blacklisted

@dataclass
class Position:
    """Data model for a position in a subnet."""
    position_id: Optional[int]  # None for new positions
    miner_uid: int  # Miner's UID
    netuid: int
    hotkey: str
    entry_block: int
    entry_timestamp: datetime
    entry_tao: float
    entry_alpha: float
    remaining_alpha: float  # Track how much of position remains
    status: PositionStatus
    extrinsic_id: str       # Unique blockchain transaction ID
    final_roi: Optional[float] = None  # Final TAO gain/loss (absolute value, not percentage)
    closed_at: Optional[datetime] = None  # When position was fully closed

    @property
    def entry_price(self) -> float:
        """Calculate entry price in TAO per alpha."""
        return self.entry_tao / self.entry_alpha if self.entry_alpha > 0 else 0

    @property
    def is_closed(self) -> bool:
        """Check if position is fully closed."""
        return self.status == PositionStatus.CLOSED

    @property
    def is_partial(self) -> bool:
        """Check if position is partially closed."""
        return self.status == PositionStatus.PARTIAL

@dataclass
class Trade:
    """Data model for a trade in a subnet."""
    trade_id: Optional[int]  # None for new trades
    miner_uid: int  # Miner's UID
    netuid: int
    hotkey: str
    exit_block: int
    exit_timestamp: datetime
    exit_tao: float
    exit_alpha: float
    extrinsic_id: str       # Unique blockchain transaction ID

    @property
    def exit_price(self) -> float:
        """Calculate exit price in TAO per alpha."""
        return self.exit_tao / self.exit_alpha if self.exit_alpha > 0 else 0

@dataclass
class PositionTrade:
    """Data model for a position-trade mapping (partial or full close)."""
    position_id: int
    trade_id: int
    alpha_amount: float  # Amount of alpha closed in this trade
    tao_amount: float   # Amount of tao received for this portion
    roi_tao: float      # Absolute TAO gain/loss (not percentage)
    duration: int       # in seconds
    min_blocks_met: bool = True  # Flag to indicate if minimum block requirement was met

    @classmethod
    def from_position_and_trade(
        cls,
        position: Position,
        trade: Trade,
        alpha_amount: float,
        tao_amount: float,
        min_blocks: int = 360  # Add minimum blocks parameter with default of 360
    ) -> 'PositionTrade':
        """Create a PositionTrade from a position and trade.
        
        Args:
            position: The position being closed
            trade: The trade closing the position
            alpha_amount: Amount of alpha being closed
            tao_amount: Amount of tao received for this portion
            min_blocks: Minimum blocks required between entry and exit (default: 360)
            
        Returns:
            PositionTrade: The mapping between position and trade
        """
        # Calculate duration
        duration = int((trade.exit_timestamp - position.entry_timestamp).total_seconds())
        
        # Check if minimum blocks requirement is met
        blocks_held = trade.exit_block - position.entry_block
        min_blocks_met = blocks_held >= min_blocks
        
        # Calculate absolute TAO gain/loss (not percentage)
        entry_tao_portion = position.entry_tao * (alpha_amount / position.entry_alpha) if position.entry_alpha > 0 else 0
        
        if min_blocks_met:
            # Direct TAO gain/loss calculation
            tao_gain_loss = tao_amount - entry_tao_portion
        else:
            # If minimum blocks not met, set gain/loss to 0 regardless of actual performance
            tao_gain_loss = 0.0
            bt.logging.info(f"Position {position.position_id} closed after only {blocks_held} blocks, setting TAO gain/loss to 0")
        
        return cls(
            position_id=position.position_id,
            trade_id=trade.trade_id,
            alpha_amount=alpha_amount,
            tao_amount=tao_amount,
            roi_tao=tao_gain_loss,  # Absolute TAO gain/loss
            duration=duration,
            min_blocks_met=min_blocks_met
        )

@dataclass
class Miner:
    """Data model for a miner in the network."""
    uid: int
    hotkey: str
    coldkey: str
    registration_date: datetime
    last_active: datetime
    status: MinerStatus
    tracking_start_date: datetime
    total_trades: int = 0
    total_volume_tao: float = 0.0
    total_volume_alpha: float = 0.0
    cumulative_roi: float = 0.0  # Total TAO gained/lost (not percentage)
    avg_trade_duration: int = 0  # in seconds
    win_rate: float = 0.0
    current_positions: int = 0

@dataclass
class PoolData:
    """Data model for subnet pool information."""
    netuid: int
    tao_reserve: float
    alpha_reserve: float
    alpha_supply: float
    emission_rate: float
    price: float  # TAO per Alpha

@dataclass
class PerformanceConfig:
    """Configuration for performance tracking and scoring.
    
    This class centralizes all configurable parameters for performance metrics,
    making it easy to modify tracking behavior without code changes.
    """
    # Time windows
    rolling_window_days: int = 7  # Length of rolling window for ROI calculation
    snapshot_interval_hours: int = 24  # How often to take performance snapshots
    retention_days: int = 90  # How long to keep historical data
    
    # Scoring weights
    roi_weight: float = 0.6  # Weight of ROI in final score
    volume_weight: float = 0.2  # Weight of trading volume
    win_rate_weight: float = 0.2  # Weight of win/loss ratio
    
    # Thresholds
    min_trades_for_scoring: int = 3  # Minimum trades needed in window for scoring
    min_volume_tao: float = 100.0  # Minimum TAO volume in window
    max_position_age_days: int = 30  # Maximum age for open positions
    
    # Time-weighted parameters
    time_decay_factor: float = 0.95  # Daily decay factor for older trades
    max_trade_gap_hours: int = 48  # Maximum gap between trades before penalty
    
    @property
    def rolling_window(self) -> timedelta:
        """Get rolling window as timedelta."""
        return timedelta(days=self.rolling_window_days)
    
    @property
    def snapshot_interval(self) -> timedelta:
        """Get snapshot interval as timedelta."""
        return timedelta(hours=self.snapshot_interval_hours)
    
    @property
    def retention_period(self) -> timedelta:
        """Get retention period as timedelta."""
        return timedelta(days=self.retention_days)
    
    @property
    def max_position_age(self) -> timedelta:
        """Get maximum position age as timedelta."""
        return timedelta(days=self.max_position_age_days)

@dataclass
class PerformanceSnapshot:
    """Point-in-time snapshot of miner performance metrics."""
    timestamp: datetime
    miner_uid: int
    window_start: datetime
    window_end: datetime
    
    # Trade metrics
    total_trades: int
    total_volume_tao: float
    total_volume_alpha: float
    
    # Performance metrics
    roi_simple: float  # Simple average of absolute TAO gain/loss
    roi_weighted: float  # Time-weighted absolute TAO gain/loss
    win_rate: float  # Percentage of profitable trades
    avg_trade_duration: int  # Average trade duration in seconds
    
    # Position metrics
    open_positions: int
    avg_position_age: int  # Average age of open positions in seconds
    
    # Scoring
    final_score: Optional[float] = None  # Calculated score if all criteria met

def process_close_event(
    positions: List[Position],
    trade: Trade,
    min_blocks: int = 360
) -> List[PositionTrade]:
    """Process a close event against a list of open positions using FIFO.
    
    Args:
        positions: List of positions sorted by entry time (FIFO order)
        trade: The trade/close event to process
        min_blocks: Minimum blocks required between entry and exit (default: 360)
        
    Returns:
        List[PositionTrade]: List of position-trade mappings created
    """
    remaining_alpha = trade.exit_alpha
    remaining_tao = trade.exit_tao
    position_trades = []
    
    for position in positions:
        if remaining_alpha <= 0:
            break
            
        # Calculate how much of this position to close
        alpha_to_close = min(position.remaining_alpha, remaining_alpha)
        tao_proportion = alpha_to_close / trade.exit_alpha
        tao_portion = tao_proportion * trade.exit_tao
        
        # Check if minimum blocks requirement is met
        blocks_held = trade.exit_block - position.entry_block
        min_blocks_met = blocks_held >= min_blocks
        
        # Create position trade record with min_blocks parameter
        position_trade = PositionTrade.from_position_and_trade(
            position=position,
            trade=trade,
            alpha_amount=alpha_to_close,
            tao_amount=tao_portion,
            min_blocks=min_blocks  # Pass the minimum blocks parameter
        )
        position_trades.append(position_trade)
        
        # Update position
        position.remaining_alpha -= alpha_to_close
        if position.remaining_alpha <= 0:
            position.status = PositionStatus.CLOSED
            position.closed_at = trade.exit_timestamp
            
            # Set final_roi to absolute TAO gain/loss
            if min_blocks_met:
                # Direct calculation for absolute TAO gain/loss
                entry_tao_portion = position.entry_tao * (alpha_to_close / position.entry_alpha) if position.entry_alpha > 0 else 0
                position.final_roi = tao_portion - entry_tao_portion
            else:
                # Set gain/loss to 0 for positions closed too early
                position.final_roi = 0.0
        else:
            position.status = PositionStatus.PARTIAL
        
        # Update remaining amounts
        remaining_alpha -= alpha_to_close
        remaining_tao -= tao_portion
    
    return position_trades

async def process_close(
    self, 
    miner_uid: int, 
    trade: Trade, 
    positions: List[Position],
    min_blocks: int = 360  # Add minimum blocks parameter with default of 360
) -> List[PositionTrade]:
    """Process a close event against a list of open positions using FIFO.
    
    Args:
        miner_uid: Miner's UID
        trade: The trade/close event to process
        positions: List of positions sorted by entry time (FIFO order)
        min_blocks: Minimum blocks required between entry and exit
        
    Returns:
        List[PositionTrade]: List of position-trade mappings created
    """
    if not positions:
        bt.logging.warning(f"No positions to close for trade {trade.extrinsic_id}")
        return []
        
    remaining_alpha = trade.exit_alpha
    remaining_tao = trade.exit_tao
    position_trades = []
    
    async with self.connection.get_connection() as conn:
        try:
            # Insert the trade first to get trade_id
            # Special SQL for handling trade insertion
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
            
            # Process each position
            for position in positions:
                if remaining_alpha <= 0:
                    break
                    
                # Calculate how much of this position to close
                alpha_to_close = min(position.remaining_alpha, remaining_alpha)
                # Use proportional allocation of TAO
                tao_proportion = alpha_to_close / trade.exit_alpha
                tao_portion = tao_proportion * trade.exit_tao
                
                # Check if minimum blocks requirement is met
                blocks_held = trade.exit_block - position.entry_block
                min_blocks_met = blocks_held >= min_blocks
                
                if not min_blocks_met:
                    bt.logging.info(
                        f"Position {position.position_id} closed after only {blocks_held} blocks, "
                        f"less than required {min_blocks}. Setting TAO gain/loss to 0."
                    )
                
                # Create position trade record with min_blocks parameter
                position_trade = PositionTrade.from_position_and_trade(
                    position=position,
                    trade=trade,
                    alpha_amount=alpha_to_close,
                    tao_amount=tao_portion,
                    min_blocks=min_blocks  # Pass the minimum blocks parameter
                )
                position_trades.append(position_trade)
                
                # Update position remaining_alpha
                position.remaining_alpha -= alpha_to_close
                
                # Update position status
                if position.remaining_alpha <= 0:
                    position.status = PositionStatus.CLOSED
                    position.closed_at = trade.exit_timestamp
                    
                    # Calculate final TAO gain/loss for the position if fully closed
                    # For positions closed before minimum blocks, this will still be 0
                    # due to the modified PositionTrade.from_position_and_trade method
                    async with self.connection.get_connection() as conn2:
                        cursor = await conn2.execute(
                            f"""
                            SELECT SUM(tao_amount) - SUM(alpha_amount * (SELECT entry_tao/entry_alpha FROM miner_{miner_uid}_positions WHERE position_id = {position.position_id}))
                            FROM miner_{miner_uid}_position_trades
                            WHERE position_id = ?
                            """,
                            (position.position_id,)
                        )
                        pt_info = await cursor.fetchone()
                        if pt_info and pt_info[0]:
                            # Set absolute TAO gain/loss
                            position.final_roi = pt_info[0]
                else:
                    position.status = PositionStatus.PARTIAL
                
                # Update the position in the database
                await conn.execute(
                    f"""
                    UPDATE miner_{miner_uid}_positions
                    SET remaining_alpha = ?,
                        status = ?,
                        closed_at = ?,
                        final_roi = ?
                    WHERE position_id = ?
                    """,
                    (
                        position.remaining_alpha,
                        position.status.value,
                        position.closed_at.isoformat() if position.closed_at else None,
                        position.final_roi,
                        position.position_id
                    )
                )
                
                # Insert position-trade mapping with min_blocks_met flag
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
                        tao_portion, position_trade.roi_tao, position_trade.duration, min_blocks_met
                    )
                )
                
                # Update remaining amounts
                remaining_alpha -= alpha_to_close
                remaining_tao -= tao_portion
            
            # Update miner stats if applicable
            await self._update_miner_stats(miner_uid, conn)
            
            # Commit all changes
            await conn.commit()
            return position_trades
            
        except Exception as e:
            bt.logging.error(f"Error processing close: {str(e)}")
            await conn.rollback()
            raise DatabaseError(f"Failed to process close: {str(e)}")