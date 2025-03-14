import numpy as np
from typing import List, Dict, Any, Tuple
import bittensor as bt
import sqlite3
import pandas as pd
from datetime import datetime

class AlphaTradeRewards:
    BLOCKS_PER_DAY = 7200
    
    def __init__(self, db_path="alphatrade/database/alpha_trade.db"):
        """
        Initialize the rewards system
        
        Args:
            db_path (str): Path to the SQLite database
        """
        self.db_path = db_path
        
    def _get_connection(self):
        """Create and return a database connection"""
        return sqlite3.connect(self.db_path)
    
    def get_trades_by_blocks(self, current_block: int, days_back: int = 14, hotkey: str = None) -> pd.DataFrame:
        """
        Fetch trades within the specified block range
        
        Args:
            current_block (int): The current block number
            days_back (int): Number of days to look back
            hotkey (str): Optional hotkey to filter trades
            
        Returns:
            pd.DataFrame: DataFrame containing trades within the block range
        """
        start_block = current_block - (self.BLOCKS_PER_DAY * days_back)
        
        query = """
        SELECT * FROM closed_trades 
        WHERE exit_block >= ? AND exit_block <= ?
        """
        params = [start_block, current_block]
        
        if hotkey:
            query += " AND hotkey = ?"
            params.append(hotkey)
            
        query += " ORDER BY exit_block DESC"
        
        with self._get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
            
        # Add day number column (0 is most recent day)
        df['days_ago'] = (current_block - df['exit_block']) // self.BLOCKS_PER_DAY
        
        return df
    
    def calculate_daily_returns(self, trades_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate total ROI per day from trades
        """
        if trades_df.empty:
            return pd.DataFrame()
            
        daily_returns = trades_df.groupby('days_ago')['roi'].agg([
            ('total_roi', 'sum'),
            ('num_trades', 'count')
        ]).reset_index()
        
        return daily_returns
    
    def calculate_return_decay(self) -> float:
        """
        Calculate return decay based on the formula: exp(ln(20)/14 days)
        """
        return np.exp(np.log(20) / 14)
    
    def calculate_day_weight(self, days_ago: int, return_decay: float) -> float:
        """
        Calculate day weight using exponential decay
        """
        lambda_param = 1.0
        return np.exp(-lambda_param * return_decay * days_ago)
    
    def calculate_weighted_returns(
        self, 
        current_block: int,
        hotkey: str = None,
        days_back: int = 14
    ) -> Tuple[float, pd.DataFrame]:
        """
        Calculate weighted daily returns giving more importance to recent results
        """
        # Get trades for the specified block range
        trades_df = self.get_trades_by_blocks(current_block, days_back, hotkey)
        
        # Calculate daily returns
        daily_returns = self.calculate_daily_returns(trades_df)
        
        # If no trades, return 0 score
        if daily_returns.empty:
            return 0.0, pd.DataFrame()
        
        # Calculate weights for each day
        return_decay = self.calculate_return_decay()
        
        daily_returns['weight'] = daily_returns['days_ago'].apply(
            lambda x: self.calculate_day_weight(x, return_decay)
        )
        
        # Calculate weighted returns
        daily_returns['weighted_return'] = (
            daily_returns['total_roi'] * 
            daily_returns['weight']
        )
        
        # Calculate total weighted return score
        total_weighted_return = daily_returns['weighted_return'].sum()
        
        return total_weighted_return, daily_returns
    
    def calculate_miner_reward(
        self,
        hotkey: str,
        current_block: int,
        days_back: int = 14,
    ) -> float:
        """
        Calculate reward for a specific miner based on their trading performance
        
        Args:
            hotkey (str): Miner's hotkey
            current_block (int): Current block number
            days_back (int): Number of days to analyze
            
        Returns:
            float: Calculated reward value
        """
        try:
            # Calculate weighted returns for this miner
            weighted_score, daily_breakdown = self.calculate_weighted_returns(
                current_block=current_block,
                hotkey=hotkey,
                days_back=days_back
            )
            
            if weighted_score == 0:
                bt.logging.info(f"No trades found for hotkey: {hotkey}")
                return 0.0
            
            # Convert score to reward value (normalized between 0 and 1)
            reward_value = np.clip(weighted_score / 100, 0, 1)
            
            bt.logging.info(
                f"Calculated reward for hotkey {hotkey}: {reward_value} "
                f"(based on weighted score: {weighted_score})"
            )
            
            return reward_value
            
        except Exception as e:
            bt.logging.error(f"Error calculating reward for hotkey {hotkey}: {str(e)}")
            return 0.0

    def get_rewards(
        self,
        queries: List[Dict[str, Any]],
        responses: List[float],
        current_block: int,
    ) -> np.ndarray:
        """
        Returns an array of rewards for multiple miners based on their trading performance
        
        Args:
            queries (List[Dict]): List of query dictionaries containing miner info
            responses (List[float]): List of responses (can be used for additional validation)
            current_block (int): Current block number
            
        Returns:
            np.ndarray: Array of rewards for each miner
        """
        rewards = []
        
        for query, response in zip(queries, responses):
            try:
                # Extract miner information from query
                hotkey = query.get('hotkey')
                days_back = query.get('days_back', 14)
                
                if not hotkey:
                    bt.logging.warning(f"Missing hotkey in query: {query}")
                    rewards.append(0.0)
                    continue
                
                # Calculate reward for this miner
                reward = self.calculate_miner_reward(
                    hotkey=hotkey,
                    current_block=current_block,
                    days_back=days_back
                )
                
                rewards.append(reward)
                
            except Exception as e:
                bt.logging.error(f"Error processing reward for query {query}: {str(e)}")
                rewards.append(0.0)
        
        return np.array(rewards)