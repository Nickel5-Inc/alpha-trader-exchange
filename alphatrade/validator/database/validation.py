"""
Validation functions for database operations.

This module provides validation functions to ensure data integrity
and catch errors before they reach the database.
"""

from datetime import datetime
from typing import Optional, List
import math

from .exceptions import (
    InvalidAmountError,
    InvalidTimestampError,
    ValidationError,
    IntegrityError,
    ConnectionError
)
from .models import Position, Trade, StakeEvent, MinerStatus, PositionStatus, PositionTrade

def validate_amount(amount: float, name: str = "amount") -> None:
    """Validate a numeric amount (TAO or alpha).
    
    Args:
        amount (float): Amount to validate
        name (str): Name of the amount for error messages
        
    Raises:
        InvalidAmountError: If amount is invalid
    """
    if not isinstance(amount, (int, float)):
        raise InvalidAmountError(f"{name} must be numeric")
    if amount <= 0:
        raise InvalidAmountError(f"{name} must be positive")
    if not math.isfinite(amount):
        raise InvalidAmountError(f"{name} must be finite")

def validate_timestamp(
    timestamp: datetime,
    reference: Optional[datetime] = None,
    name: str = "timestamp"
) -> None:
    """Validate a timestamp.
    
    Args:
        timestamp (datetime): Timestamp to validate
        reference (Optional[datetime]): Reference timestamp for ordering
        name (str): Name of the timestamp for error messages
        
    Raises:
        InvalidTimestampError: If timestamp is invalid
    """
    if not isinstance(timestamp, datetime):
        raise InvalidTimestampError(f"{name} must be a datetime")
    if timestamp.tzinfo is None:
        raise InvalidTimestampError(f"{name} must be timezone-aware")
    if reference and timestamp < reference:
        raise InvalidTimestampError(
            f"{name} ({timestamp}) cannot be before reference ({reference})"
        )

def validate_position(position: Position) -> None:
    """Validate a position before database operations.
    
    Args:
        position (Position): Position to validate
        
    Raises:
        ValidationError: If position is invalid
    """
    if not isinstance(position, Position):
        raise ValidationError("Must be a Position instance")
        
    validate_amount(position.entry_tao, "entry_tao")
    validate_amount(position.entry_alpha, "entry_alpha")
    validate_amount(position.remaining_alpha, "remaining_alpha")
    
    if position.remaining_alpha > position.entry_alpha:
        raise ValidationError("remaining_alpha cannot exceed entry_alpha")
        
    validate_timestamp(position.entry_timestamp, name="entry_timestamp")

def validate_trade(trade: Trade) -> None:
    """Validate a trade before database operations.
    
    Args:
        trade (Trade): Trade to validate
        
    Raises:
        ValidationError: If trade is invalid
    """
    if not isinstance(trade, Trade):
        raise ValidationError("Must be a Trade instance")
        
    validate_amount(trade.exit_tao, "exit_tao")
    validate_amount(trade.exit_alpha, "exit_alpha")
    validate_timestamp(trade.exit_timestamp, name="exit_timestamp")

def validate_stake_event(event: StakeEvent) -> None:
    """Validate a stake event before processing.
    
    Args:
        event (StakeEvent): Event to validate
        
    Raises:
        ValidationError: If event is invalid
    """
    if not isinstance(event, StakeEvent):
        raise ValidationError("Must be a StakeEvent instance")
        
    validate_amount(event.tao_amount, "tao_amount")
    validate_amount(event.alpha_amount, "alpha_amount")
    validate_timestamp(event.timestamp, name="event_timestamp")

def validate_coldkey(coldkey: str) -> None:
    """Validate a miner's coldkey.
    
    Args:
        coldkey (str): Coldkey to validate in SS58 format
        
    Raises:
        ValidationError: If coldkey is invalid
    """
    if not isinstance(coldkey, str):
        raise ValidationError("coldkey must be a string")
    if not coldkey.startswith("5"):  # SS58 addresses start with 5
        raise ValidationError("coldkey must be in SS58 format (starts with 5)")
    if len(coldkey) != 48:  # SS58 addresses are 48 characters long
        raise ValidationError("coldkey must be 48 characters long")

def validate_miner_status(status: MinerStatus) -> None:
    """Validate a miner's status.
    
    Args:
        status (MinerStatus): Status to validate
        
    Raises:
        ValidationError: If status is invalid
    """
    if not isinstance(status, MinerStatus):
        raise ValidationError("status must be a MinerStatus enum")
    if status not in MinerStatus:
        raise ValidationError(f"Invalid status: {status}")

def validate_miner_registration(
    coldkey: str,
    hotkey: str,
    tracking_start_date: datetime,
    status: MinerStatus = MinerStatus.ACTIVE
) -> None:
    """Validate miner registration data.
    
    Args:
        coldkey (str): Miner's coldkey
        hotkey (str): Miner's hotkey
        tracking_start_date (datetime): When to start tracking
        status (MinerStatus): Initial miner status
        
    Raises:
        ValidationError: If registration data is invalid
    """
    validate_coldkey(coldkey)
    validate_hotkey(hotkey)
    validate_timestamp(tracking_start_date, name="tracking_start_date")
    validate_miner_status(status)

def validate_miner_uid(uid: int) -> None:
    """Validate a miner's UID.
    
    Args:
        uid (int): Miner's UID to validate
        
    Raises:
        ValidationError: If UID is invalid
    """
    if not isinstance(uid, int):
        raise ValidationError("miner_uid must be an integer")
    if uid <= 0:
        raise ValidationError("miner_uid must be positive")

def validate_position_id(position_id: int) -> None:
    """Validate a position ID.
    
    Args:
        position_id (int): Position ID to validate
        
    Raises:
        ValidationError: If position ID is invalid
    """
    if not isinstance(position_id, int):
        raise ValidationError("position_id must be an integer")
    if position_id <= 0:
        raise ValidationError("position_id must be positive")

def validate_trade_id(trade_id: int) -> None:
    """Validate a trade ID.
    
    Args:
        trade_id (int): Trade ID to validate
        
    Raises:
        ValidationError: If trade ID is invalid
    """
    if not isinstance(trade_id, int):
        raise ValidationError("trade_id must be an integer")
    if trade_id <= 0:
        raise ValidationError("trade_id must be positive")

def validate_timeframe(start_time: datetime, end_time: datetime) -> None:
    """Validate a timeframe for queries.
    
    Args:
        start_time (datetime): Start of timeframe
        end_time (datetime): End of timeframe
        
    Raises:
        ValidationError: If timeframe is invalid
    """
    if not isinstance(start_time, datetime):
        raise ValidationError("start_time must be a datetime")
    if not isinstance(end_time, datetime):
        raise ValidationError("end_time must be a datetime")
    if start_time >= end_time:
        raise ValidationError("start_time must be before end_time")
    validate_timestamp(start_time, name="start_time")
    validate_timestamp(end_time, name="end_time")

def validate_hotkey(hotkey: str) -> None:
    """Validate a validator's hotkey.
    
    Args:
        hotkey (str): Hotkey to validate in SS58 format
        
    Raises:
        ValidationError: If hotkey is invalid
    """
    if not isinstance(hotkey, str):
        raise ValidationError("hotkey must be a string")
    if not hotkey.startswith("5"):  # SS58 addresses start with 5
        raise ValidationError("hotkey must be in SS58 format (starts with 5)")
    if len(hotkey) != 48:  # SS58 addresses are 48 characters long
        raise ValidationError("hotkey must be 48 characters long")

def validate_position_trade_integrity(position: Position, trade: Trade, alpha_amount: float) -> None:
    """Validate the integrity of a position-trade relationship.
    
    Args:
        position: Position being closed
        trade: Trade closing the position
        alpha_amount: Amount of alpha being closed
        
    Raises:
        IntegrityError: If relationship is invalid
    """
    if position.netuid != trade.netuid:
        raise IntegrityError("Position and trade must be in same subnet")
    
    if alpha_amount > position.remaining_alpha:
        raise IntegrityError(
            f"Cannot close more alpha ({alpha_amount}) than remaining ({position.remaining_alpha})"
        )
    
    if trade.exit_timestamp < position.entry_timestamp:
        raise IntegrityError("Trade timestamp cannot be before position entry")

def validate_position_state_transition(
    position: Position,
    new_remaining_alpha: float,
    new_status: PositionStatus
) -> None:
    """Validate a position state transition.
    
    Args:
        position: Position being updated
        new_remaining_alpha: New remaining alpha value
        new_status: New position status
        
    Raises:
        IntegrityError: If state transition is invalid
    """
    if new_remaining_alpha < 0:
        raise IntegrityError("Remaining alpha cannot be negative")
        
    if new_remaining_alpha > position.entry_alpha:
        raise IntegrityError("Remaining alpha cannot exceed entry alpha")
        
    # Validate status transitions
    if position.status == PositionStatus.CLOSED:
        raise IntegrityError("Cannot modify closed position")
        
    if new_status == PositionStatus.OPEN and new_remaining_alpha < position.entry_alpha:
        raise IntegrityError("Open positions must have full remaining alpha")
        
    if new_status == PositionStatus.CLOSED and new_remaining_alpha > 0:
        raise IntegrityError("Closed positions must have zero remaining alpha")
        
    if new_status == PositionStatus.PARTIAL:
        if new_remaining_alpha == 0:
            raise IntegrityError("Partial positions must have remaining alpha")
        if new_remaining_alpha == position.entry_alpha:
            raise IntegrityError("Partial positions must have some alpha closed")

def validate_miner_stats_consistency(
    total_trades: int,
    total_volume_tao: float,
    total_volume_alpha: float,
    cumulative_roi: float,
    win_rate: float,
    current_positions: int
) -> None:
    """Validate consistency of miner statistics.
    
    Args:
        total_trades: Total number of trades
        total_volume_tao: Total TAO volume
        total_volume_alpha: Total alpha volume
        cumulative_roi: Average ROI across trades
        win_rate: Percentage of winning trades
        current_positions: Number of open positions
        
    Raises:
        IntegrityError: If statistics are inconsistent
    """
    if total_trades < 0:
        raise IntegrityError("Total trades cannot be negative")
        
    if total_volume_tao < 0 or total_volume_alpha < 0:
        raise IntegrityError("Volume cannot be negative")
        
    if not -100 <= cumulative_roi <= 1000:  # Reasonable ROI range
        raise IntegrityError("Cumulative ROI outside reasonable range")
        
    if not 0 <= win_rate <= 100:
        raise IntegrityError("Win rate must be between 0 and 100")
        
    if current_positions < 0:
        raise IntegrityError("Current positions cannot be negative")
        
    if total_trades == 0 and (total_volume_tao != 0 or total_volume_alpha != 0):
        raise IntegrityError("Zero trades must have zero volume")

def validate_trade_stats_consistency(
    trade: Trade,
    affected_positions: List[Position],
    position_trades: List[PositionTrade]
) -> None:
    """Validate consistency between trade, affected positions, and mappings.
    
    Args:
        trade: Trade being processed
        affected_positions: Positions affected by the trade
        position_trades: Position-trade mappings created
        
    Raises:
        IntegrityError: If relationships are inconsistent
    """
    # Validate total alpha matches
    total_alpha = sum(pt.alpha_amount for pt in position_trades)
    if abs(total_alpha - trade.exit_alpha) > 1e-8:  # Allow small float rounding error
        raise IntegrityError(
            f"Position-trade alpha sum ({total_alpha}) "
            f"doesn't match trade alpha ({trade.exit_alpha})"
        )
    
    # Validate total TAO matches
    total_tao = sum(pt.tao_amount for pt in position_trades)
    if abs(total_tao - trade.exit_tao) > 1e-8:
        raise IntegrityError(
            f"Position-trade TAO sum ({total_tao}) "
            f"doesn't match trade TAO ({trade.exit_tao})"
        )
    
    # Validate all positions are accounted for
    position_ids = {p.position_id for p in affected_positions}
    mapping_position_ids = {pt.position_id for pt in position_trades}
    if position_ids != mapping_position_ids:
        raise IntegrityError("Mismatch between affected positions and mappings")

async def validate_db_connection_state(conn) -> None:
    """Validate database connection state.
    
    Args:
        conn: Database connection to validate
        
    Raises:
        ConnectionError: If connection is invalid or closed
    """
    if conn is None:
        raise ConnectionError("Database connection is None")
    try:
        # Simple test query to verify connection
        cursor = await conn.execute("SELECT 1")
        await cursor.fetchone()
    except Exception as e:
        raise ConnectionError(f"Database connection is invalid: {str(e)}")

async def validate_referential_integrity(
    conn,
    miner_uid: int,
    position_id: Optional[int] = None,
    trade_id: Optional[int] = None
) -> None:
    """Validate referential integrity between tables.
    
    Args:
        conn: Database connection
        miner_uid (int): Miner's UID to validate
        position_id (Optional[int]): Position ID to validate
        trade_id (Optional[int]): Trade ID to validate
        
    Raises:
        IntegrityError: If referential integrity is violated
    """
    try:
        # Verify miner exists
        cursor = await conn.execute(
            "SELECT uid FROM miners WHERE uid = ?",
            (miner_uid,)
        )
        if not await cursor.fetchone():
            raise IntegrityError(f"Miner {miner_uid} does not exist")
            
        # Verify position if provided
        if position_id is not None:
            cursor = await conn.execute(
                f"SELECT position_id FROM miner_{miner_uid}_positions WHERE position_id = ?",
                (position_id,)
            )
            if not await cursor.fetchone():
                raise IntegrityError(f"Position {position_id} does not exist for miner {miner_uid}")
                
        # Verify trade if provided
        if trade_id is not None:
            cursor = await conn.execute(
                f"SELECT trade_id FROM miner_{miner_uid}_trades WHERE trade_id = ?",
                (trade_id,)
            )
            if not await cursor.fetchone():
                raise IntegrityError(f"Trade {trade_id} does not exist for miner {miner_uid}")
                
    except Exception as e:
        if isinstance(e, IntegrityError):
            raise
        raise IntegrityError(f"Failed to validate referential integrity: {str(e)}") 