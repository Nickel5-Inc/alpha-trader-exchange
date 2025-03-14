"""
SQLAlchemy query functions for position tracking.

This module provides query functions for managing positions and trades in the database.
"""

from datetime import datetime
from typing import List, Optional, Tuple
from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession

from alphatrade.validator.database.connection import DatabaseConnection
from alphatrade.validator.database.exceptions import DatabaseError, ValidationError
from .models import Position, Trade, PositionTrade, PositionStatus
from .validation import (
    validate_position,
    validate_trade,
    validate_position_trade_integrity,
    validate_position_state_transition,
    validate_trade_stats_consistency
)

async def get_position(
    session: AsyncSession,
    miner_uid: str,
    position_id: str
) -> Optional[Position]:
    """Get a position by its ID.
    
    Args:
        session: Database session
        miner_uid: Miner UID
        position_id: Position ID
        
    Returns:
        Optional[Position]: Position if found, None otherwise
        
    Raises:
        ValidationError: If inputs are invalid
        DatabaseError: If query fails
    """
    query = (
        select(Position)
        .where(
            and_(
                Position.miner_uid == miner_uid,
                Position.position_id == position_id
            )
        )
    )
    
    try:
        result = await session.execute(query)
        position = result.scalar_one_or_none()
        return position
    except Exception as e:
        raise DatabaseError(f"Failed to get position: {str(e)}")

async def get_positions_in_timeframe(
    session: AsyncSession,
    miner_uid: str,
    start_time: datetime,
    end_time: datetime,
    netuid: Optional[int] = None,
    status: Optional[PositionStatus] = None
) -> List[Position]:
    """Get all positions within a timeframe.
    
    Args:
        session: Database session
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
    filters = [
        Position.miner_uid == miner_uid,
        Position.entry_timestamp >= start_time,
        Position.entry_timestamp <= end_time
    ]
    
    if netuid is not None:
        filters.append(Position.netuid == netuid)
    if status is not None:
        filters.append(Position.status == status)
        
    query = select(Position).where(and_(*filters))
    
    try:
        result = await session.execute(query)
        positions = result.scalars().all()
        return positions
    except Exception as e:
        raise DatabaseError(f"Failed to get positions: {str(e)}")

async def get_position_trades(
    session: AsyncSession,
    miner_uid: str,
    position_id: str
) -> List[Tuple[Trade, PositionTrade]]:
    """Get all trades that closed a position.
    
    Args:
        session: Database session
        miner_uid: Miner UID
        position_id: Position ID
        
    Returns:
        List[Tuple[Trade, PositionTrade]]: List of (trade, position_trade) tuples
        
    Raises:
        ValidationError: If inputs are invalid
        DatabaseError: If query fails
    """
    query = (
        select(Trade, PositionTrade)
        .join(
            PositionTrade,
            and_(
                PositionTrade.trade_id == Trade.trade_id,
                PositionTrade.position_id == position_id
            )
        )
        .where(
            and_(
                Trade.miner_uid == miner_uid,
                PositionTrade.position_id == position_id
            )
        )
    )
    
    try:
        result = await session.execute(query)
        trades = result.all()
        return trades
    except Exception as e:
        raise DatabaseError(f"Failed to get position trades: {str(e)}")

async def get_trade_positions(
    session: AsyncSession,
    miner_uid: str,
    trade_id: str
) -> List[Tuple[Position, PositionTrade]]:
    """Get all positions affected by a trade.
    
    Args:
        session: Database session
        miner_uid: Miner UID
        trade_id: Trade ID
        
    Returns:
        List[Tuple[Position, PositionTrade]]: List of (position, position_trade) tuples
        
    Raises:
        ValidationError: If inputs are invalid
        DatabaseError: If query fails
    """
    query = (
        select(Position, PositionTrade)
        .join(
            PositionTrade,
            and_(
                PositionTrade.position_id == Position.position_id,
                PositionTrade.trade_id == trade_id
            )
        )
        .where(
            and_(
                Position.miner_uid == miner_uid,
                PositionTrade.trade_id == trade_id
            )
        )
    )
    
    try:
        result = await session.execute(query)
        positions = result.all()
        return positions
    except Exception as e:
        raise DatabaseError(f"Failed to get trade positions: {str(e)}")

async def create_position(
    session: AsyncSession,
    position: Position
) -> Position:
    """Create a new position.
    
    Args:
        session: Database session
        position: Position to create
        
    Returns:
        Position: Created position
        
    Raises:
        ValidationError: If position is invalid
        DatabaseError: If creation fails
    """
    validate_position(position)
    
    try:
        session.add(position)
        await session.flush()
        return position
    except Exception as e:
        raise DatabaseError(f"Failed to create position: {str(e)}")

async def create_trade(
    session: AsyncSession,
    trade: Trade,
    affected_positions: List[Tuple[Position, float]]
) -> Tuple[Trade, List[PositionTrade]]:
    """Create a new trade and update affected positions.
    
    Args:
        session: Database session
        trade: Trade to create
        affected_positions: List of (position, alpha_amount) tuples
        
    Returns:
        Tuple[Trade, List[PositionTrade]]: Created trade and position-trade mappings
        
    Raises:
        ValidationError: If trade or position updates are invalid
        DatabaseError: If creation fails
    """
    validate_trade(trade)
    
    position_trades = []
    for position, alpha_amount in affected_positions:
        validate_position_trade_integrity(position, trade, alpha_amount)
        
        # Create position-trade mapping
        position_trade = PositionTrade.from_position_and_trade(
            position, trade, alpha_amount
        )
        position_trades.append(position_trade)
        
        # Update position state
        position.remaining_alpha -= alpha_amount
        if position.remaining_alpha <= 0:
            position.status = PositionStatus.CLOSED
            position.closed_at = trade.exit_timestamp
        else:
            position.status = PositionStatus.PARTIAL
            
        validate_position_state_transition(position)
    
    validate_trade_stats_consistency(trade, affected_positions, position_trades)
    
    try:
        session.add(trade)
        session.add_all(position_trades)
        await session.flush()
        return trade, position_trades
    except Exception as e:
        raise DatabaseError(f"Failed to create trade: {str(e)}")

async def get_position_stats(
    session: AsyncSession,
    miner_uid: str,
    netuid: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> dict:
    """Get position statistics.
    
    Args:
        session: Database session
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
    filters = [Position.miner_uid == miner_uid]
    
    if netuid is not None:
        filters.append(Position.netuid == netuid)
    if start_time is not None:
        filters.append(Position.entry_timestamp >= start_time)
    if end_time is not None:
        filters.append(Position.entry_timestamp <= end_time)
        
    try:
        # Get position counts by status
        status_counts = (
            select(
                Position.status,
                func.count(Position.position_id)
            )
            .where(and_(*filters))
            .group_by(Position.status)
        )
        status_result = await session.execute(status_counts)
        status_stats = dict(status_result.all())
        
        # Get total alpha and TAO
        totals = (
            select(
                func.sum(Position.entry_alpha).label("total_alpha"),
                func.sum(Position.entry_tao).label("total_tao")
            )
            .where(and_(*filters))
        )
        totals_result = await session.execute(totals)
        totals_row = totals_result.one()
        
        # Get average ROI and duration for closed positions
        closed_filters = filters + [Position.status == PositionStatus.CLOSED]
        averages = (
            select(
                func.avg(Position.final_roi).label("avg_roi"),
                func.avg(
                    func.julianday(Position.closed_at) -
                    func.julianday(Position.entry_timestamp)
                ).label("avg_duration")
            )
            .where(and_(*closed_filters))
        )
        averages_result = await session.execute(averages)
        averages_row = averages_result.one()
        
        return {
            "total_positions": sum(status_stats.values()),
            "open_positions": status_stats.get(PositionStatus.OPEN, 0),
            "closed_positions": status_stats.get(PositionStatus.CLOSED, 0),
            "partial_positions": status_stats.get(PositionStatus.PARTIAL, 0),
            "total_alpha": float(totals_row.total_alpha or 0),
            "total_tao": float(totals_row.total_tao or 0),
            "avg_roi": float(averages_row.avg_roi or 0),
            "avg_duration": float(averages_row.avg_duration or 0) * 86400  # Convert days to seconds
        }
    except Exception as e:
        raise DatabaseError(f"Failed to get position stats: {str(e)}") 
