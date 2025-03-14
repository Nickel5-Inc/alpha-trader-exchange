"""
Data models for position tracking.

This module contains all data models used for tracking positions, trades,
and their relationships.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

class PositionStatus(Enum):
    """Status of a position"""
    OPEN = "open"           # Position is fully open
    CLOSED = "closed"       # Position is fully closed
    PARTIAL = "partial"     # Position is partially closed

@dataclass
class Position:
    """Data model for a position in a subnet."""
    position_id: Optional[int]  # None for new positions
    netuid: int
    hotkey: str
    entry_block: int
    entry_timestamp: datetime
    entry_tao: float
    entry_alpha: float
    remaining_alpha: float  # Track how much of position remains
    status: PositionStatus
    final_roi: Optional[float] = None  # Final ROI for closed positions
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
    """Data model for an exit trade."""
    trade_id: Optional[int]  # None for new trades
    netuid: int
    hotkey: str
    exit_block: int
    exit_timestamp: datetime
    exit_tao: float
    exit_alpha: float

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
    roi_tao: float
    duration: int       # in seconds

    @classmethod
    def from_position_and_trade(
        cls,
        position: Position,
        trade: Trade,
        alpha_amount: float,
        tao_amount: float
    ) -> 'PositionTrade':
        """Create a PositionTrade from a position and trade.
        
        Args:
            position: The position being closed
            trade: The trade closing the position
            alpha_amount: Amount of alpha being closed
            tao_amount: Amount of tao received for this portion
            
        Returns:
            PositionTrade: The mapping between position and trade
        """
        # Calculate ROI for this portion
        entry_tao_portion = position.entry_price * alpha_amount
        roi = ((tao_amount - entry_tao_portion) / entry_tao_portion) * 100 if entry_tao_portion > 0 else 0
        
        # Calculate duration
        duration = int((trade.exit_timestamp - position.entry_timestamp).total_seconds())
        
        return cls(
            position_id=position.position_id,
            trade_id=trade.trade_id,
            alpha_amount=alpha_amount,
            tao_amount=tao_amount,
            roi_tao=roi,
            duration=duration
        ) 