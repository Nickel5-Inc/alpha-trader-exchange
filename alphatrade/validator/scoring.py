import sqlite3
from datetime import datetime, timedelta, timezone, date
import pandas as pd
import numpy as np
import re
import bittensor as bt
import traceback
import torch

class AlphaTradeScorer:
    def __init__(self, db_path="alphatrade/database/alpha_trade.db"):
        """
        Initialize the scoring module with database connection
        
        Args:
            db_path (str): Path to the SQLite database
        """
        self.db_path = db_path
        
    # [other methods unchanged]
        
    async def calculate_miner_scores(self, database_connection, logger=None):
        """
        Calculate performance scores for all miners based on their position tables
        
        Args:
            database_connection: Async database connection
            logger: Optional logger instance
            
        Returns:
            dict: Dictionary mapping miner UIDs to their performance scores
        """
        miner_scores = {}
        current_time = datetime.now(timezone.utc)
        
        try:
            # Get all table names in the database
            try:
                async with database_connection.get_connection() as conn:
                    cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = await cursor.fetchall()
            except Exception as conn_error:
                bt.logging.error(f"Error with database connection: {str(conn_error)}")
                bt.logging.info("Falling back to direct SQLite connection")
                # Fallback to direct connection
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = cursor.fetchall()
                    return self._calculate_miner_scores_sync(conn, current_time)
                
            # Extract miner position tables
            miner_position_tables = []
            for table in tables:
                table_name = table[0]
                match = re.match(r'miner_(\d+)_positions', table_name)
                if match:
                    miner_uid = int(match.group(1))
                    miner_position_tables.append((miner_uid, table_name))
            
            bt.logging.info(f"Found {len(miner_position_tables)} miner position tables")
            
            # Process each miner's positions
            for miner_uid, table_name in miner_position_tables:
                bt.logging.info(f"Processing positions for miner {miner_uid}")
                
                # Query closed positions for this miner
                query = f"""
                SELECT 
                    position_id, 
                    entry_timestamp, 
                    closed_at as exit_timestamp, 
                    final_roi as roi
                FROM {table_name}
                WHERE status = 'closed' AND closed_at IS NOT NULL
                """
                
                try:
                    async with database_connection.get_connection() as conn:
                        cursor = await conn.execute(query)
                        positions = await cursor.fetchall()
                except Exception as query_error:
                    bt.logging.error(f"Error querying positions: {str(query_error)}")
                    continue
                
                if not positions:
                    bt.logging.info(f"No closed positions found for miner {miner_uid}")
                    # Assign a zero score for miners with no closed positions
                    miner_scores[miner_uid] = 0.0
                    continue
                
                bt.logging.info(f"Found {len(positions)} closed positions for miner {miner_uid}")
                
                # Process each position manually to handle datetime conversion issues
                daily_returns = {}
                for position in positions:
                    position_id, entry_timestamp_str, exit_timestamp_str, roi = position
                    
                    # Print raw timestamp from database for debugging
                    bt.logging.info(f"Position {position_id}: exit_timestamp={exit_timestamp_str}, roi={roi}")
                    
                    # Parse timestamp manually to avoid pandas issues
                    try:
                        # Remove microseconds and timezone for simplicity
                        if exit_timestamp_str and "." in exit_timestamp_str:
                            # Remove microseconds part
                            parts = exit_timestamp_str.split(".")
                            clean_timestamp = parts[0]
                            if "+" in parts[1]:
                                # Keep timezone
                                tz_part = "+" + parts[1].split("+")[1]
                                clean_timestamp += tz_part
                        else:
                            clean_timestamp = exit_timestamp_str
                            
                        # Remove timezone part if it exists
                        if "+" in clean_timestamp:
                            clean_timestamp = clean_timestamp.split("+")[0]
                            
                        # Parse the date part only
                        exit_date_str = clean_timestamp.split("T")[0]
                        exit_date = date.fromisoformat(exit_date_str)
                        
                        bt.logging.info(f"  Parsed date: {exit_date}")
                        
                        # Add to daily returns
                        if exit_date in daily_returns:
                            daily_returns[exit_date] += float(roi)
                        else:
                            daily_returns[exit_date] = float(roi)
                    except Exception as e:
                        bt.logging.error(f"Error parsing timestamp {exit_timestamp_str}: {str(e)}")
                        continue
                
                # Convert daily returns to list for further processing
                daily_data = []
                for exit_date, roi_sum in daily_returns.items():
                    days_difference = (current_time.date() - exit_date).days
                    return_decay = self.calculate_return_decay()
                    weight = self.calculate_day_weight(days_difference, return_decay)
                    weighted_return = roi_sum * weight
                    
                    daily_data.append({
                        'exit_date': exit_date,
                        'roi': roi_sum,
                        'days_difference': days_difference,
                        'weight': weight,
                        'weighted_return': weighted_return
                    })
                
                if not daily_data:
                    bt.logging.info(f"No valid daily returns for miner {miner_uid}")
                    miner_scores[miner_uid] = 0.0
                    continue
                
                # Display daily returns for debugging
                bt.logging.info("\nDaily returns:")
                for data in daily_data:
                    bt.logging.info(f"Date: {data['exit_date']} | ROI: {data['roi']:.6f} | " 
                                  f"Days diff: {data['days_difference']} | Weight: {data['weight']:.6f} | "
                                  f"Weighted: {data['weighted_return']:.6f}")
                
                # Calculate average weighted return
                total_weighted_return = sum(data['weighted_return'] for data in daily_data)
                avg_weighted_return = total_weighted_return / len(daily_data)
                
                bt.logging.info(f"Total weighted return: {total_weighted_return:.6f}")
                bt.logging.info(f"Average weighted return: {avg_weighted_return:.6f}")
                
                miner_scores[miner_uid] = avg_weighted_return
                bt.logging.info(f"Miner {miner_uid} score: {miner_scores[miner_uid]}")
            
            return miner_scores
                
        except Exception as e:
            bt.logging.error(f"Error calculating miner scores: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return {}

    def calculate_return_decay(self):
        """
        Calculate the decay factor for historical returns.
        
        Uses the exponential decay formula from AlphaTradeRewards:
        exp(ln(20)/14 days)
        
        Returns:
            float: The decay factor to apply to historical returns
        """
        # Using the same formula as in AlphaTradeRewards
        return np.exp(np.log(20) / 14)

    def calculate_day_weight(self, days_difference, return_decay):
        """
        Calculate weight for a specific day based on its age.
        
        Args:
            days_difference (int): Number of days from current date
            return_decay (float): The decay factor from calculate_return_decay
        
        Returns:
            float: Weight to apply to returns from this day
        """
        # Apply exponential decay based on days difference, as in AlphaTradeRewards
        lambda_param = 0.25
        return np.exp(-lambda_param * return_decay * days_difference)

    def _calculate_miner_scores_sync(self, connection, current_time):
        """
        Synchronous version of calculate_miner_scores as fallback
        
        Args:
            connection: SQLite connection
            current_time: Current datetime
            
        Returns:
            dict: Dictionary mapping miner UIDs to their performance scores
        """
        miner_scores = {}
        current_date = current_time.date()
        
        try:
            # Get all table names in the database
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            # Extract miner position tables
            miner_position_tables = []
            for table in tables:
                table_name = table[0]
                match = re.match(r'miner_(\d+)_positions', table_name)
                if match:
                    miner_uid = int(match.group(1))
                    miner_position_tables.append((miner_uid, table_name))
            
            bt.logging.info(f"Found {len(miner_position_tables)} miner position tables")
            
            # Process each miner's positions
            for miner_uid, table_name in miner_position_tables:
                bt.logging.info(f"Processing positions for miner {miner_uid}")
                
                # Query closed positions for this miner
                query = f"""
                SELECT 
                    position_id, 
                    entry_timestamp, 
                    closed_at as exit_timestamp, 
                    final_roi as roi
                FROM {table_name}
                WHERE status = 'closed' AND closed_at IS NOT NULL
                """
                
                cursor.execute(query)
                positions = cursor.fetchall()
                
                if not positions:
                    bt.logging.info(f"No closed positions found for miner {miner_uid}")
                    # Assign a zero score for miners with no closed positions
                    miner_scores[miner_uid] = 0.0
                    continue
                
                # Process each position manually to handle datetime conversion issues
                daily_returns = {}
                for position in positions:
                    position_id, entry_timestamp_str, exit_timestamp_str, roi = position
                    
                    # Log position info
                    bt.logging.info(f"Position {position_id}: exit_timestamp={exit_timestamp_str}, roi={roi}")
                    
                    # Parse timestamp manually to avoid pandas issues
                    try:
                        # Remove microseconds and timezone for simplicity
                        if exit_timestamp_str and "." in exit_timestamp_str:
                            parts = exit_timestamp_str.split(".")
                            clean_timestamp = parts[0]
                            if "+" in parts[1]:
                                tz_part = "+" + parts[1].split("+")[1]
                                clean_timestamp += tz_part
                        else:
                            clean_timestamp = exit_timestamp_str
                            
                        # Remove timezone part if it exists
                        if "+" in clean_timestamp:
                            clean_timestamp = clean_timestamp.split("+")[0]
                            
                        # Parse the date part only
                        exit_date_str = clean_timestamp.split("T")[0]
                        exit_date = date.fromisoformat(exit_date_str)
                        
                        bt.logging.info(f"  Parsed date: {exit_date}")
                        
                        # Add to daily returns
                        if exit_date in daily_returns:
                            daily_returns[exit_date] += float(roi)
                        else:
                            daily_returns[exit_date] = float(roi)
                    except Exception as e:
                        bt.logging.error(f"Error parsing timestamp {exit_timestamp_str}: {str(e)}")
                        continue
                
                # Convert daily returns to DataFrame for further processing
                daily_data = []
                for exit_date, roi_sum in daily_returns.items():
                    days_difference = (current_date - exit_date).days
                    return_decay = self.calculate_return_decay()
                    weight = self.calculate_day_weight(days_difference, return_decay)
                    weighted_return = roi_sum * weight
                    
                    daily_data.append({
                        'exit_date': exit_date,
                        'roi': roi_sum,
                        'days_difference': days_difference,
                        'weight': weight,
                        'weighted_return': weighted_return
                    })
                
                if not daily_data:
                    bt.logging.info(f"No valid daily returns for miner {miner_uid}")
                    miner_scores[miner_uid] = 0.0
                    continue
                
                # Display daily returns for debugging
                bt.logging.info("\nDaily returns:")
                for data in daily_data:
                    bt.logging.info(f"Date: {data['exit_date']} | ROI: {data['roi']:.6f} | " 
                                   f"Days diff: {data['days_difference']} | Weight: {data['weight']:.6f} | "
                                   f"Weighted: {data['weighted_return']:.6f}")
                
                # Calculate average weighted return
                total_weighted_return = sum(data['weighted_return'] for data in daily_data)
                avg_weighted_return = total_weighted_return / len(daily_data)
                
                bt.logging.info(f"Total weighted return: {total_weighted_return:.6f}")
                bt.logging.info(f"Average weighted return: {avg_weighted_return:.6f}")
                
                miner_scores[miner_uid] = avg_weighted_return
                bt.logging.info(f"Miner {miner_uid} score: {miner_scores[miner_uid]}")
            
            return miner_scores
                
        except Exception as e:
            bt.logging.error(f"Error calculating miner scores: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return {}
    
    async def set_weights_based_on_performance(self, validator, min_weight_threshold=0.1):
        """
        Calculate performance scores and set weights for miners in the validator
        
        Args:
            validator: The validator instance with metagraph and weights
            min_weight_threshold (float): Minimum weight to assign to any miner
            
        Returns:
            dict: Dictionary of normalized weights by UID
        """
        try:
            # Define epsilon for handling floating point precision issues
            EPSILON = 1e-6
            
            # Check if database connection is available
            db_connection = None
            if hasattr(validator, 'database') and hasattr(validator.database, 'connection'):
                db_connection = validator.database.connection
            
            # Calculate scores for all miners
            miner_scores = await self.calculate_miner_scores(db_connection)
            
            bt.logging.info("\n--- All Miner Performance Scores ---")
            # Sort miners by score, with negative scores at the bottom
            sorted_miners = sorted(miner_scores.items(), key=lambda x: x[1], reverse=True)
            
            # Print results in a table format
            bt.logging.info(f"{'Miner UID':<10} | {'Raw Score':<15} | {'Normalized Score':<15}")
            bt.logging.info("-" * 45)
            
            # First identify positive-scoring miners only
            positive_miners = []
            for uid, score in sorted_miners:
                if score > EPSILON:  # Only include miners with scores significantly greater than 0
                    positive_miners.append((uid, score))
            
            # Calculate normalization factors if positive miners exist
            max_score = 0
            min_score = 0
            if positive_miners:
                scores = [s for _, s in positive_miners]
                max_score = max(scores)
                min_score = min(scores)
            
            # Prepare normalized weights
            normalized_weights = {}
            
            # Process each miner score
            for uid, score in sorted_miners:
                # DIRECT APPROACH: Zero or negative raw scores ALWAYS get zero normalized score
                if score <= EPSILON:
                    normalized = 0.0
                else:
                    # Only positive scores get normalized between 0 and 1
                    if len(positive_miners) > 1 and max_score != min_score:
                        normalized = (score - min_score) / (max_score - min_score)
                    else:
                        # If only one positive miner or all have same score, they get 1.0
                        normalized = 1.0
                
                # Store normalized score
                normalized_weights[uid] = normalized
                
                # Log for visibility
                bt.logging.info(f"{uid:<10} | {score:<15.6f} | {normalized:<15.6f}")
            
            bt.logging.info("\nScoring complete!")
            
            # Check if validator has the necessary attributes to set weights
            if hasattr(validator, 'metagraph') and hasattr(validator, 'weights'):
                # Convert normalized_weights to tensor format
                for uid in normalized_weights:
                    if hasattr(validator.metagraph, 'uids') and uid in validator.metagraph.uids:
                        validator.weights[uid] = normalized_weights[uid]
                
                bt.logging.info(f"Updated weights tensor for {len(normalized_weights)} miners based on performance")
                
                # The key addition: Actually set the weights on the blockchain
                if hasattr(validator, 'subtensor') and hasattr(validator, 'wallet'):
                    # Check stake (like in NextPlace)
                    uid = validator.metagraph.hotkeys.index(validator.wallet.hotkey.ss58_address)
                    stake = float(validator.metagraph.S[uid])
                    
                    #if stake < 1000.0:  # You can adjust this minimum stake requirement
                    #    bt.logging.warning(f"Insufficient stake ({stake} TAO). Failed in setting weights.")
                    #    return normalized_weights
                    
                    try:
                        # Convert to appropriate tensor format if needed
                        if not isinstance(validator.weights, torch.Tensor):
                            weights_tensor = torch.tensor(validator.weights, dtype=torch.float32)
                        else:
                            weights_tensor = validator.weights
                        
                        # Set weights on the blockchain
                        result = validator.subtensor.set_weights(
                            netuid=validator.config.netuid,
                            wallet=validator.wallet,
                            uids=validator.metagraph.uids,
                            weights=weights_tensor,
                            wait_for_inclusion=True,
                            version_key=1,  # Replace with your version key if you have one
                            wait_for_finalization=False,
                        )
                        
                        # Check result
                        success = result[0] if isinstance(result, tuple) and len(result) >= 1 else False
                        
                        if success:
                            bt.logging.info("✅ Successfully set weights on the blockchain.")
                        else:
                            bt.logging.warning(f"❗Failed to set weights on the blockchain. Result: {result}")
                    
                    except Exception as e:
                        bt.logging.error(f"Error setting weights on the blockchain: {str(e)}")
                        bt.logging.error(traceback.format_exc())
                else:
                    bt.logging.warning("Could not set weights on blockchain: missing subtensor or wallet attribute")
                
                # Save weights to disk if method exists
                if hasattr(validator, 'save_state') and callable(validator.save_state):
                    validator.save_state()
            else:
                bt.logging.warning("Could not update weights: validator missing metagraph or weights attributes")
            
            return normalized_weights
                
        except Exception as e:
            bt.logging.error(f"Error setting weights based on performance: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return {}