"""
Mock TaoStats API client for testing.

This module provides a mock implementation of the AlphaAPI class that returns
consistent test data without making actual API calls.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Set, Optional

from alphatrade.api.api import AlphaAPI
from alphatrade.database.models import StakeEvent, StakeAction, ApiTrade, PoolData

class MockAlphaAPI(AlphaAPI):
    """Mock implementation of AlphaAPI for testing."""
    
    def __init__(self):
        """Initialize mock API with test data."""
        self.events = []  # Will be populated with test events
        self.pool_data = {}  # Will be populated with test pool data
        self.current_block = 1000
        
    async def __aenter__(self):
        """No-op for mock."""
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """No-op for mock."""
        pass

    def add_test_events(self, events: List[StakeEvent]):
        """Add test events to the mock.
        
        Args:
            events (List[StakeEvent]): Events to add
        """
        self.events.extend(events)
        self.events.sort(key=lambda x: x.timestamp)

    def set_test_pool_data(self, pool_data: List[PoolData]):
        """Set test pool data.
        
        Args:
            pool_data (List[PoolData]): Pool data to use
        """
        self.pool_data = {p.netuid: p for p in pool_data}

    async def get_stake_events(
        self,
        since_block: int = 0,
        coldkey: Optional[str] = None
    ) -> List[StakeEvent]:
        """Get stake events.
        
        Args:
            since_block: Block number to get events since
            coldkey: Optional coldkey to filter events by
            
        Returns:
            List[StakeEvent]: List of stake events
        """
        events = [e for e in self.events if e.block_number > since_block]
        if coldkey:
            events = [e for e in events if e.coldkey == coldkey]
        return events

    async def get_active_positions_at(self, coldkey: str, timestamp: datetime) -> Set[int]:
        """Get test active positions at timestamp.
        
        Args:
            coldkey (str): Miner's coldkey (SS58 address)
            timestamp (datetime): Timestamp to check positions at
            
        Returns:
            Set[int]: Set of netuids with active positions
        """
        active = set()
        
        for event in self.events:
            if event.coldkey != coldkey or event.timestamp > timestamp:
                continue
                
            if event.action == StakeAction.DELEGATE:
                active.add(event.netuid)
            elif event.action == StakeAction.UNDELEGATE:
                active.discard(event.netuid)
                
        return active

    async def get_pool_data(self) -> List[PoolData]:
        """Return test pool data.
        
        Returns:
            List[PoolData]: Test pool data
        """
        return list(self.pool_data.values())

    def add_event(self, event: StakeEvent):
        """Add a new event."""
        self.events.append(event)
        self.current_block = max(self.current_block, event.block_number)

    async def get_miner_metrics(self, hotkey: str):
        """Get mock miner performance metrics for a given hotkey."""
        # Define a simple dummy metrics object with required methods
        class DummyMetrics:
            def get_performance_score(self):
                return 0.85
            def get_activity_score(self):
                return 1.0
        return DummyMetrics() 