"""
Validator neuron for the Alpha Trade Exchange subnet.

This module implements the main validator logic for tracking miner positions,
calculating performance metrics, and maintaining database state.
"""

from datetime import datetime, timezone, timedelta
import asyncio
import numpy as np
import bittensor as bt
import torch

from alphatrade.base.validator import BaseValidatorNeuron
from alphatrade.validator.database.models import StakeAction, Position, Trade, PositionStatus, MinerStatus
from alphatrade.validator.database.manager import DatabaseManager
from alphatrade.validator.database.miner import MinerManager
from alphatrade.validator.api.api import AlphaAPI
from alphatrade.validator.forward import forward
from alphatrade.validator.database.sync import SyncManager, create_sync_manager
from alphatrade.validator.scoring import AlphaTradeScorer

class Validator(BaseValidatorNeuron):
    """
    Validator neuron for the Alpha Trade Exchange subnet.
    
    This validator:
    1. Tracks miner positions and trades
    2. Calculates performance metrics
    3. Sets weights based on miner performance
    4. Maintains database state
    
    The validator operates in two main cycles:
    - forward(): Handles queries from miners (called by Bittensor framework)
    - main(): Performs maintenance operations (called by our custom scheduler)
    """

    def __init__(self, config=None):
        """Initialize the validator with required components and state."""
        # Initialize state trackers BEFORE calling super().__init__
        self.last_sync = datetime.min.replace(tzinfo=timezone.utc)
        self.last_cleanup = datetime.min.replace(tzinfo=timezone.utc)
        self.last_scoring = datetime.min.replace(tzinfo=timezone.utc)
        
        # Initialize parent class
        super(Validator, self).__init__(config=config)
        
        # Initialize database manager
        self.database = DatabaseManager()
        
        # Initialize miner manager
        self.miner_manager = MinerManager(self.database)
        
        # Initialize API client
        self.api = AlphaAPI()
        
        # Initialize sync manager (will be done in main loop)
        self.sync_manager = None

        # Convert numpy array to torch tensor first
        if hasattr(self, 'metagraph') and hasattr(self.metagraph, 'S'):
            self.weights = torch.zeros(self.metagraph.S.shape, dtype=torch.float32)
        else:
            bt.logging.warning("Metagraph.S not available, initializing empty weights")
            self.weights = torch.zeros(0)
        
        # Initialize scoring manager
        self.scorer = AlphaTradeScorer(db_path=self.database.db_path)
        
        # Ensure database is set up properly at startup
        self._setup_database()
        
        # Load saved state if exists
        self.load_state()
        bt.logging.info("Validator initialized successfully")    

    def _setup_database(self):
        """Initialize the database during validator startup.
        
        This method ensures that the database and all required tables exist.
        """
        try:
            # Create event loop or use existing one
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # Create a new event loop if no current loop exists
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Set up database and miner manager in sequence
            bt.logging.info("Setting up database...")
            
            # Run both setup tasks
            loop.run_until_complete(self._async_setup_database())
            
            bt.logging.info("Database setup complete")
        except Exception as e:
            bt.logging.error(f"Error setting up database: {str(e)}")
            
            # Wait a moment and try again with a fresh database connection
            try:
                bt.logging.info("Retrying database setup...")
                # Create new instances to ensure fresh connections
                self.database = DatabaseManager()
                self.miner_manager = MinerManager(self.database)
                
                # Create a new event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                # Try setup again
                loop.run_until_complete(self._async_setup_database())
                
                bt.logging.info("Database setup successful on retry")
            except Exception as retry_error:
                bt.logging.error(f"Database setup failed on retry: {str(retry_error)}")
                # Continue anyway - we'll try again in the main loop
    
    async def _async_setup_database(self):
        """Async setup method for database and miner manager initialization."""
        bt.logging.info("Starting database initialization sequence")
        
        try:
            # First, initialize the database (creates required tables)
            await self.database.setup_database()
            bt.logging.info("Core database tables initialized")
            
            # Then set up miner tables through the miner manager
            await self.miner_manager.ensure_database_initialized()
            bt.logging.info("Miner tables initialized")
        except Exception as e:
            bt.logging.error(f"Error during database initialization: {str(e)}")
            # Let the error propagate so the retry mechanism can handle it

    async def main(self):
        """
        Main validator maintenance loop.
        This runs periodically to handle state synchronization and scoring.
        """
        current_time = datetime.now(timezone.utc)
        bt.logging.info("Running main validator maintenance cycle")
        
        try:
            # First, ensure database is initialized
            await self.scorer.set_weights_based_on_performance(self, min_weight_threshold=0.0001)
            self.last_scoring = current_time
            if not hasattr(self.database, '_connection_initialized') or not self.database._connection_initialized:
                bt.logging.info("Database not initialized, initializing now...")
                await self.database.setup_database()
                
            if not hasattr(self.miner_manager, '_initialized') or not self.miner_manager._initialized:
                bt.logging.info("Miner manager not initialized, initializing now...")
                await self.miner_manager.ensure_database_initialized()
            
            # Initialize sync manager if needed
            if self.sync_manager is None:
                self.sync_manager = await create_sync_manager(self)
                bt.logging.info("SyncManager initialized")
            
            # Run unified sync if needed
            if await self.sync_manager.should_sync():
                await self.sync_manager.sync_all()
                self.last_sync = current_time
        
            await self.scorer.set_weights_based_on_performance(self, min_weight_threshold=0.0001)
            self.last_scoring = current_time
            
            # Run cleanup operations (if needed)
            if (current_time - self.last_cleanup) > timedelta(hours=24):
                await self._cleanup_operations()
                self.last_cleanup = current_time
            
            # Save current state
            self.save_state()
            
        except Exception as e:
            bt.logging.error(f"Error in main validator loop: {str(e)}")
            # Don't raise to keep the validator running

    async def forward(self):
        """
        Process queries from miners in the network.
        This method is called automatically by the Bittensor framework.
        """
        # Ensure database is initialized before processing queries
        if not hasattr(self.database, '_connection_initialized') or not self.database._connection_initialized:
            bt.logging.info("Database not initialized, initializing now before processing query...")
            await self.database.setup_database()
            
        if not hasattr(self.miner_manager, '_initialized') or not self.miner_manager._initialized:
            bt.logging.info("Miner manager not initialized, initializing now...")
            await self.miner_manager.ensure_database_initialized()
            
        # Call the imported forward function which handles query processing
        await forward(self)

    async def _cleanup_operations(self):
        """
        Perform database cleanup operations.
        This archives old data and optimizes database performance.
        """
        bt.logging.info("Running cleanup operations...")
        try:
            # Archive old positions (older than retention period)
            retention_days = 180  # 6 months
            archive_cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
            
            async with self.database.connection.get_connection() as conn:
                # Get all active miners
                cursor = await conn.execute("SELECT uid FROM miners")
                miners = await cursor.fetchall()
                
                # Archive old data for each miner
                for miner in miners:
                    miner_uid = miner[0]
                    
                    # Archive old closed positions
                    await conn.execute(
                        f"""
                        DELETE FROM miner_{miner_uid}_positions
                        WHERE entry_timestamp < ? AND status = 'closed'
                        """,
                        (archive_cutoff.isoformat(),)
                    )
                    
                    # Archive old trades
                    await conn.execute(
                        f"""
                        DELETE FROM miner_{miner_uid}_trades
                        WHERE exit_timestamp < ?
                        """,
                        (archive_cutoff.isoformat(),)
                    )
                    
                    # Archive old position-trade mappings
                    await conn.execute(
                        f"""
                        DELETE FROM miner_{miner_uid}_position_trades
                        WHERE position_id IN (
                            SELECT position_id FROM miner_{miner_uid}_positions
                            WHERE entry_timestamp < ? AND status = 'closed'
                        )
                        """,
                        (archive_cutoff.isoformat(),)
                    )
                    
                    # Archive old performance snapshots
                    await conn.execute(
                        """
                        DELETE FROM performance_snapshots
                        WHERE miner_uid = ? AND timestamp < ?
                        """,
                        (miner_uid, archive_cutoff.isoformat())
                    )
                    
                await conn.commit()
                bt.logging.info("Cleaned up old data successfully")
                
        except Exception as e:
            bt.logging.error(f"Error during cleanup: {str(e)}")
            raise

    def save_state(self):
        """Save validator state to disk."""
        bt.logging.info("Saving validator state")
        super().save_state()
        
        # Make sure these attributes exist before trying to save them
        if hasattr(self, 'last_sync') and hasattr(self, 'last_cleanup') and hasattr(self, 'last_scoring'):
            # Save additional state
            state = {
                'last_sync': self.last_sync.isoformat(),
                'last_cleanup': self.last_cleanup.isoformat(),
                'last_scoring': self.last_scoring.isoformat()
            }
            
            # Save to file as part of the NPZ file
            np.savez(
                self.config.neuron.full_path + "/validator_state.npz",
                **state
            )

    def load_state(self):
        """Load validator state from disk."""
        bt.logging.info("Loading validator state")
        
        try:
            # Load base state
            super().load_state()
            
            # Load additional state
            try:
                state = np.load(self.config.neuron.full_path + "/validator_state.npz")
                self.last_sync = datetime.fromisoformat(str(state['last_sync']))
                self.last_cleanup = datetime.fromisoformat(str(state['last_cleanup']))
                
                # Load last_scoring if it exists, otherwise use default
                if 'last_scoring' in state:
                    self.last_scoring = datetime.fromisoformat(str(state['last_scoring']))
                else:
                    self.last_scoring = datetime.min.replace(tzinfo=timezone.utc)
                    
                bt.logging.info("Loaded validator state successfully")
            except (FileNotFoundError, KeyError) as e:
                bt.logging.warning(f"Could not load validator state: {str(e)}")
                # Ensure we have default values if loading fails
                self.last_sync = datetime.min.replace(tzinfo=timezone.utc)
                self.last_cleanup = datetime.min.replace(tzinfo=timezone.utc)
                self.last_scoring = datetime.min.replace(tzinfo=timezone.utc)
        except Exception as e:
            bt.logging.error(f"Error loading validator state: {str(e)}")