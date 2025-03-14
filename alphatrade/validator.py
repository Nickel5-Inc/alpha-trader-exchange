from datetime import datetime, timezone, timedelta
import numpy as np
from alphatrade.database.models import StakeAction, PositionStatus, Position, Trade, MinerStatus
from alphatrade.database.exceptions import DatabaseError
import bittensor as bt

from alphatrade.base.validator import BaseValidatorNeuron
from alphatrade.database.manager import DatabaseManager
from alphatrade.database.miner import MinerManager
from alphatrade.api.api import AlphaAPI
from alphatrade.validator.forward import forward

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
        super(Validator, self).__init__(config=config)
        
        # Initialize database manager
        self.database = DatabaseManager()
        
        # Initialize miner manager
        self.miner_manager = MinerManager(self.database)
        
        # Initialize API client
        self.api = AlphaAPI()
        
        # Initialize state trackers
        self.last_metagraph_sync = datetime.min.replace(tzinfo=timezone.utc)
        self.last_api_sync = datetime.min.replace(tzinfo=timezone.utc)
        self.last_cleanup = datetime.min.replace(tzinfo=timezone.utc)
        
        # Load saved state if exists
        self.load_state()
        bt.logging.info("Validator initialized successfully")

    async def forward(self):
        """
        Process queries from miners in the network.
        This method is called automatically by the Bittensor framework.
        """
        # Call the imported forward function which handles query processing
        await forward(self)

    async def main(self):
        """
        Main validator maintenance loop.
        This runs periodically to handle state synchronization and scoring.
        """
        current_time = datetime.now(timezone.utc)
        bt.logging.info("Running main validator maintenance cycle")
        
        try:
            # 1. Sync metagraph and update state (if needed)
            if (current_time - self.last_metagraph_sync) > timedelta(minutes=5):
                await self._sync_metagraph_state()
                self.last_metagraph_sync = current_time
            
            # 2. Sync API data (if needed)
            if (current_time - self.last_api_sync) > timedelta(minutes=15):
                await self._sync_api_data()
                self.last_api_sync = current_time
            
            # 3. Update scores and weights
            await self._update_scores()
            await self._update_weights()
            
            # 4. Run cleanup operations (if needed)
            if (current_time - self.last_cleanup) > timedelta(hours=24):
                await self._cleanup_operations()
                self.last_cleanup = current_time
            
            # 5. Save current state
            self.save_state()
            
        except Exception as e:
            bt.logging.error(f"Error in main validator loop: {str(e)}")
            # Don't raise to keep the validator running

    async def _sync_metagraph_state(self):
        """
        Synchronize with the metagraph and update local state.
        This ensures our database reflects the current state of the network.
        """
        bt.logging.info("Syncing metagraph state...")
        try:
            # Get current registered miners from metagraph
            current_hotkeys = self._get_miner_hotkeys()
            bt.logging.info(f"Found {len(current_hotkeys)} miners in metagraph")
            
            # Process each miner individually to ensure table creation using the miner manager
            for hotkey in current_hotkeys:
                try:
                    uid = self.metagraph.hotkeys.index(hotkey) + 1  # Compute UID
                    bt.logging.info(f"Processing miner {hotkey} with UID {uid}")
                    
                    # Register miner using MinerManager to force table creation
                    await self.miner_manager.register_miner(
                        coldkey=hotkey,
                        hotkey=hotkey,
                        tracking_start_date=datetime.now(timezone.utc),
                        status=MinerStatus.ACTIVE,
                        uid=uid
                    )
                    
                    # Explicitly create tables for this miner
                    await self.miner_manager.ensure_miner_tables_exist_for_miner_uid(uid)
                    
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
                                async for row in cursor:
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
                    # Continue with other miners
            
            # Mark unregistered miners as inactive
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
            
            bt.logging.info(f"Updated registration status for {len(current_hotkeys)} miners")
            
        except Exception as e:
            bt.logging.error(f"Failed to sync metagraph state: {str(e)}")
            raise

    async def _sync_api_data(self):
        """
        Synchronize with external APIs to fetch the latest data.
        This ensures we have up-to-date information about stake events.
        """
        bt.logging.info("Syncing API data...")
        try:
            # Get all registered miners
            async with self.database.connection.get_connection() as conn:
                cursor = await conn.execute(
                    "SELECT uid, hotkey FROM miners WHERE is_registered = 1"
                )
                miners = await cursor.fetchall()
            
            # Fetch recent events for each miner
            for miner in miners:
                # Get events since last sync
                events = await self._fetch_stake_events(miner['hotkey'])
                
                # Process events
                for event in events:
                    if event.action == StakeAction.DELEGATE:
                        # Create new position
                        position = Position(
                            position_id=None,
                            miner_uid=miner['uid'],
                            netuid=event.netuid,
                            hotkey=event.hotkey,
                            entry_block=event.block_number,
                            entry_timestamp=event.timestamp,
                            entry_tao=event.tao_amount,
                            entry_alpha=event.alpha_amount,
                            remaining_alpha=event.alpha_amount,
                            status=PositionStatus.OPEN
                        )
                        await self.database.add_position(miner['uid'], position)
                    
                    elif event.action == StakeAction.UNDELEGATE:
                        # Get open positions
                        async with self.database.connection.get_connection() as conn:
                            cursor = await conn.execute(
                                f"""
                                SELECT * FROM miner_{miner['uid']}_positions
                                WHERE status = 'open' AND netuid = ?
                                ORDER BY entry_timestamp ASC
                                """,
                                (event.netuid,)
                            )
                            positions = []
                            async for row in cursor:
                                positions.append(Position(
                                    position_id=row['position_id'],
                                    miner_uid=miner['uid'],
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
                                    trade_id=None,
                                    miner_uid=miner['uid'],
                                    netuid=event.netuid,
                                    hotkey=event.hotkey,
                                    exit_block=event.block_number,
                                    exit_timestamp=event.timestamp,
                                    exit_tao=event.tao_amount,
                                    exit_alpha=event.alpha_amount
                                )
                                
                                # Close positions
                                await self.database.process_close(miner['uid'], trade, positions)
            
            bt.logging.info("API sync completed successfully")
        except Exception as e:
            bt.logging.error(f"Error syncing API data: {str(e)}")
            raise

    async def _update_scores(self):
        """
        Update miner scores based on performance metrics.
        This analyzes trading history and position performance to calculate scores.
        """
        bt.logging.info("Updating performance scores...")
        try:
            # Get performance snapshots for all active miners
            async with self.database.connection.get_connection() as conn:
                cursor = await conn.execute(
                    """
                    SELECT m.hotkey, ps.* 
                    FROM miners m
                    LEFT JOIN performance_snapshots ps ON ps.miner_uid = m.uid
                    WHERE m.is_registered = 1
                    AND ps.timestamp = (
                        SELECT MAX(timestamp) 
                        FROM performance_snapshots 
                        WHERE miner_uid = m.uid
                    )
                    """
                )
                snapshots = await cursor.fetchall()
            
            # Calculate scores
            scores = np.zeros(self.metagraph.n)
            for snapshot in snapshots:
                if snapshot['final_score'] is not None:
                    try:
                        uid = self.metagraph.hotkeys.index(snapshot['hotkey'])
                        scores[uid] = snapshot['final_score']
                    except ValueError:
                        bt.logging.warning(f"Hotkey {snapshot['hotkey']} not found in metagraph")
            
            # Update scores in the base class
            self.update_scores(scores, list(range(len(scores))))
            
            bt.logging.info("Performance scores updated successfully")
            
        except Exception as e:
            bt.logging.error(f"Error updating scores: {str(e)}")
            raise

    async def _update_weights(self):
        """
        Set miner weights based on calculated scores.
        This updates the weights on-chain to influence rewards.
        """
        bt.logging.info("Updating weights...")
        try:
            # Set weights if enough time has passed
            if self.should_set_weights():
                self.set_weights()
                bt.logging.info("Weights set successfully")
            else:
                bt.logging.info("Skipping weight setting (not yet time)")
                
        except Exception as e:
            bt.logging.error(f"Error setting weights: {str(e)}")
            raise

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

    async def _fetch_stake_events(self, coldkey: str):
        """
        Fetch stake events for a specific coldkey from the API.
        
        Args:
            coldkey: The coldkey to fetch events for
            
        Returns:
            List of StakeEvent objects
        """
        try:
            # Get events from our last sync time
            since_time = self.last_api_sync if self.last_api_sync > datetime.min.replace(tzinfo=timezone.utc) else None
            
            # Fetch events from API
            async with self.api as api_client:
                events = await api_client.get_stake_events(
                    coldkey=coldkey,
                    start_time=since_time
                )
                
            bt.logging.info(f"Fetched {len(events)} stake events for {coldkey}")
            return events
            
        except Exception as e:
            bt.logging.error(f"Error fetching stake events for {coldkey}: {str(e)}")
            return []  # Return empty list on error to avoid breaking the sync

    def _get_miner_hotkeys(self):
        """
        Get the set of hotkeys for all miners in the metagraph.
        
        Returns:
            set: Set of miner hotkeys
        """
        miner_uids = set([uid for uid, dividend in enumerate(self.metagraph.dividends) if dividend == 0])
        return set([hotkey for uid, hotkey in enumerate(self.metagraph.hotkeys) if uid in miner_uids])

    def save_state(self):
        """Save validator state to disk."""
        bt.logging.info("Saving validator state")
        super().save_state()
        
        # Save additional state
        state = {
            'last_metagraph_sync': self.last_metagraph_sync.isoformat(),
            'last_api_sync': self.last_api_sync.isoformat(),
            'last_cleanup': self.last_cleanup.isoformat()
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
                self.last_metagraph_sync = datetime.fromisoformat(str(state['last_metagraph_sync']))
                self.last_api_sync = datetime.fromisoformat(str(state['last_api_sync']))
                self.last_cleanup = datetime.fromisoformat(str(state['last_cleanup']))
                bt.logging.info("Loaded validator state successfully")
            except (FileNotFoundError, KeyError) as e:
                bt.logging.warning(f"Could not load validator state: {str(e)}")
        except Exception as e:
            bt.logging.error(f"Error loading validator state: {str(e)}") 