"""
Position tracking manager for the Alpha Trade Exchange subnet.

This module provides the PositionManager class for managing positions and trades.
"""

from datetime import datetime
from typing import List, Optional, Tuple, Dict, Any
from uuid import uuid4

from alphatrade.validator.database.manager import DatabaseManager
from alphatrade.validator.database.exceptions import DatabaseError, ValidationError
from .models import Position, Trade, PositionTrade, PositionStatus
from .queries import (
    get_position,
    get_positions_in_timeframe,
    get_position_trades,
    get_trade_positions,
    create_position,
    create_trade,
    get_position_stats
)

class PositionManager:
    """Manager class for position tracking.
    
    This class provides a high-level interface for managing positions and trades.
    """
    
    def __init__(self):
        """Initialize position manager."""
        self._db = DatabaseManager()  # Get singleton instance
    
    async def open_position(
        self,
        miner_uid: str,
        netuid: int,
        hotkey: str,
        entry_block: int,
        entry_timestamp: datetime,
        entry_tao: float,
        entry_alpha: float
    ) -> Position:
        """Open a new position.
        
        Args:
            miner_uid: Miner UID
            netuid: Subnet ID
            hotkey: Hotkey
            entry_block: Block number at entry
            entry_timestamp: Timestamp at entry
            entry_tao: TAO amount at entry
            entry_alpha: Alpha amount at entry
            
        Returns:
            Position: Created position
            
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If creation fails
        """
        position = Position(
            position_id=str(uuid4()),
            miner_uid=miner_uid,
            netuid=netuid,
            hotkey=hotkey,
            entry_block=entry_block,
            entry_timestamp=entry_timestamp,
            entry_tao=entry_tao,
            entry_alpha=entry_alpha,
            remaining_alpha=entry_alpha,
            status=PositionStatus.OPEN,
            final_roi=0.0,
            closed_at=None
        )
        
        async def create_func(session):
            return await create_position(session, position)
            
        return await self._db.connection._run_transaction(create_func)
    
    async def close_positions(
        self,
        miner_uid: str,
        netuid: int,
        hotkey: str,
        positions: List[Tuple[Position, float]],
        exit_block: int,
        exit_timestamp: datetime,
        exit_tao: float,
        exit_alpha: float
    ) -> Tuple[Trade, List[PositionTrade]]:
        """Close positions with a trade.
        
        Args:
            miner_uid: Miner UID
            netuid: Subnet ID
            hotkey: Hotkey
            positions: List of (position, alpha_amount) tuples
            exit_block: Block number at exit
            exit_timestamp: Timestamp at exit
            exit_tao: TAO amount at exit
            exit_alpha: Alpha amount at exit
            
        Returns:
            Tuple[Trade, List[PositionTrade]]: Created trade and position-trade mappings
            
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If creation fails
        """
        trade = Trade(
            trade_id=str(uuid4()),
            miner_uid=miner_uid,
            netuid=netuid,
            hotkey=hotkey,
            exit_block=exit_block,
            exit_timestamp=exit_timestamp,
            exit_tao=exit_tao,
            exit_alpha=exit_alpha
        )
        
        async def create_func(session):
            return await create_trade(session, trade, positions)
            
        return await self._db.connection._run_transaction(create_func)
    
    async def get_position(
        self,
        miner_uid: str,
        position_id: str
    ) -> Optional[Position]:
        """Get a position by its ID.
        
        Args:
            miner_uid: Miner UID
            position_id: Position ID
            
        Returns:
            Optional[Position]: Position if found, None otherwise
            
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If query fails
        """
        async def query_func(session):
            return await get_position(session, miner_uid, position_id)
            
        return await self._db.connection._run_read_query(query_func)
    
    async def get_positions_in_timeframe(
        self,
        miner_uid: str,
        start_time: datetime,
        end_time: datetime,
        netuid: Optional[int] = None,
        status: Optional[PositionStatus] = None
    ) -> List[Position]:
        """Get all positions within a timeframe.
        
        Args:
            miner_uid: Miner UID
            start_time: Start time
            end_time: End time
            netuid: Optional subnet ID filter
            status: Optional position status filter
            
        Returns:
            List[Position]: List of positions
            
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If query fails
        """
        async def query_func(session):
            return await get_positions_in_timeframe(
                session,
                miner_uid,
                start_time,
                end_time,
                netuid,
                status
            )
            
        return await self._db.connection._run_read_query(query_func)
    
    async def get_position_trades(
        self,
        miner_uid: str,
        position_id: str
    ) -> List[Tuple[Trade, PositionTrade]]:
        """Get all trades that closed a position.
        
        Args:
            miner_uid: Miner UID
            position_id: Position ID
            
        Returns:
            List[Tuple[Trade, PositionTrade]]: List of (trade, position_trade) tuples
            
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If query fails
        """
        async def query_func(session):
            return await get_position_trades(session, miner_uid, position_id)
            
        return await self._db.connection._run_read_query(query_func)
    
    async def get_trade_positions(
        self,
        miner_uid: str,
        trade_id: str
    ) -> List[Tuple[Position, PositionTrade]]:
        """Get all positions affected by a trade.
        
        Args:
            miner_uid: Miner UID
            trade_id: Trade ID
            
        Returns:
            List[Tuple[Position, PositionTrade]]: List of (position, position_trade) tuples
            
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If query fails
        """
        async def query_func(session):
            return await get_trade_positions(session, miner_uid, trade_id)
            
        return await self._db.connection._run_read_query(query_func)
    
    async def get_position_stats(
        self,
        miner_uid: str,
        netuid: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get position statistics.
        
        Args:
            miner_uid: Miner UID
            netuid: Optional subnet ID filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            dict: Statistics including:
                - total_positions: Total number of positions
                - open_positions: Number of open positions
                - closed_positions: Number of closed positions
                - partial_positions: Number of partially closed positions
                - total_alpha: Total alpha in positions
                - total_tao: Total TAO in positions
                - avg_roi: Average ROI across closed positions
                - avg_duration: Average position duration in seconds
                
        Raises:
            ValidationError: If inputs are invalid
            DatabaseError: If query fails
        """
        async def query_func(session):
            return await get_position_stats(
                session,
                miner_uid,
                netuid,
                start_time,
                end_time
            )
            
        return await self._db.connection._run_read_query(query_func) 
