"""
Validation functions for position tracking.

This module provides validation functions to ensure data integrity
and catch errors before they reach the database.
"""

from datetime import datetime
import math
from typing import List

from .models import Position, Trade, PositionTrade, PositionStatus
from alphatrade.validator.database.exceptions import (
    ValidationError, IntegrityError, InsufficientAlphaError
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
        
    if not isinstance(position.entry_tao, (int, float)) or position.entry_tao <= 0:
        raise ValidationError("entry_tao must be positive")
        
    if not isinstance(position.entry_alpha, (int, float)) or position.entry_alpha <= 0:
        raise ValidationError("entry_alpha must be positive")
        
    if not isinstance(position.remaining_alpha, (int, float)) or position.remaining_alpha < 0:
        raise ValidationError("remaining_alpha cannot be negative")
        
    if position.remaining_alpha > position.entry_alpha:
        raise ValidationError("remaining_alpha cannot exceed entry_alpha")
        
    if position.entry_timestamp.tzinfo is None:
        raise ValidationError("entry_timestamp must be timezone-aware")

def validate_trade(trade: Trade) -> None:
    """Validate a trade before database operations.
    
    Args:
        trade (Trade): Trade to validate
        
    Raises:
        ValidationError: If trade is invalid
    """
    if not isinstance(trade, Trade):
        raise ValidationError("Must be a Trade instance")
        
    if not isinstance(trade.exit_tao, (int, float)) or trade.exit_tao <= 0:
        raise ValidationError("exit_tao must be positive")
        
    if not isinstance(trade.exit_alpha, (int, float)) or trade.exit_alpha <= 0:
        raise ValidationError("exit_alpha must be positive")
        
    if trade.exit_timestamp.tzinfo is None:
        raise ValidationError("exit_timestamp must be timezone-aware")

def validate_position_trade_integrity(
    position: Position,
    trade: Trade,
    alpha_amount: float
) -> None:
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
