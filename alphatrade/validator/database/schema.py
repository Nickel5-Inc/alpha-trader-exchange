"""
SQL schema definitions for the Alpha Trade Exchange subnet database.

Database Design Overview
-----------------------
The database uses a three-table design per miner to handle complex position tracking and FIFO accounting.
This design was chosen to provide accurate tracking of position performance while maintaining data integrity
and query efficiency.

Core Tables
----------
1. Miners Table (global):
   - Tracks all miners registered in the subnet
   - Stores aggregate statistics and miner status
   - Contains registration and tracking start dates
   - Example: A miner registers with coldkey "0x123..." and starts being tracked from block 1000

2. Per-Miner Tables
   Each miner gets three dedicated tables prefixed with their UID:

   a) Positions Table (miner_{uid}_positions):
      - Records entry points into a subnet (stake events)
      - Tracks initial TAO staked and alpha received
      - Maintains remaining_alpha for partial closes
      - Status transitions: open -> partial -> closed
      - Stores extrinsic_id for unique identification
      - Example: Miner stakes 100 TAO, receives 10 alpha, position_id=1, extrinsic_id="0xabc123"

   b) Trades Table (miner_{uid}_trades):
      - Records exit points from a subnet (unstake events)
      - One trade can close multiple positions
      - Tracks total TAO received and alpha returned
      - Stores extrinsic_id for unique identification
      - Example: Miner unstakes 5 alpha for 60 TAO, trade_id=1, extrinsic_id="0xdef456"

   c) Position-Trade Mappings (miner_{uid}_position_trades):
      - Links positions to the trades that close them
      - Stores performance metrics for each position-trade pair
      - Enables accurate FIFO accounting
      - Example: trade_id=1 closes 5 alpha from position_id=1, ROI=20%

FIFO Accounting Example
---------------------
Consider this sequence:
1. Miner opens two positions:
   - Position 1: 20 alpha @ 15 TAO/alpha (300 TAO total), extrinsic_id="0xabc123"
   - Position 2: 20 alpha @ 23 TAO/alpha (460 TAO total), extrinsic_id="0xdef456"

2. Miner unstakes 30 alpha for 750 TAO with extrinsic_id="0xghi789":
   - First 20 alpha comes from Position 1 (oldest)
   - Next 10 alpha comes from Position 2 (newest)
   - Position 1 is fully closed
   - Position 2 is partially closed (10 alpha remains)

3. Database records:
   Positions:
   - P1: status='closed', remaining_alpha=0, extrinsic_id="0xabc123"
   - P2: status='partial', remaining_alpha=10, extrinsic_id="0xdef456"

   Trade:
   - T1: exit_alpha=30, exit_tao=750, extrinsic_id="0xghi789"

   Position-Trade Mappings:
   - P1-T1: alpha_amount=20, tao_amount=500, roi=66.67%
   - P2-T1: alpha_amount=10, tao_amount=250, roi=8.70%

Benefits of This Design
---------------------
1. Accurate Performance Tracking:
   - Precise ROI calculations per position portion
   - Validator-specific performance metrics
   - Timing information preserved

2. Complex Scenario Support:
   - Partial position closes
   - Multiple positions closed in one trade
   - FIFO accounting compliance

3. Data Integrity:
   - Foreign key constraints ensure consistency
   - Status transitions are clearly tracked
   - Complete audit trail maintained
   - Unique extrinsic IDs prevent duplicate entries

4. Query Efficiency:
   - Indexed lookups for common queries
   - Efficient aggregation of statistics
   - Fast position and trade retrieval

Note on Indexing
--------------
Each table includes carefully chosen indexes to optimize common queries:
- Status-based position lookups
- Chronological ordering for FIFO
- Validator (hotkey) performance analysis
- Subnet-specific queries
- Extrinsic ID lookups for uniqueness checks

The indexes balance query performance with storage overhead.
"""

# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# SQL Statements for table creation
CREATE_MINER_TABLE = """
CREATE TABLE IF NOT EXISTS miners (
    uid INTEGER PRIMARY KEY,
    hotkey TEXT UNIQUE NOT NULL,
    coldkey TEXT NOT NULL UNIQUE,
    registration_date TIMESTAMP NOT NULL,  -- When the miner registered with the subnet
    last_active TIMESTAMP NOT NULL,        -- Last time a position or trade was recorded
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'blacklisted')),
    total_trades INTEGER DEFAULT 0,        -- Total number of exit trades
    total_volume_tao REAL DEFAULT 0.0,     -- Total TAO volume (sum of entry positions)
    total_volume_alpha REAL DEFAULT 0.0,   -- Total alpha volume (sum of entry positions)
    cumulative_roi REAL DEFAULT 0.0,       -- Average ROI across all closed positions
    avg_trade_duration INTEGER DEFAULT 0,   -- Average duration of closed positions in seconds
    win_rate REAL DEFAULT 0.0,             -- Percentage of trades with positive ROI
    current_positions INTEGER DEFAULT 0,    -- Number of open or partially closed positions
    tracking_start_date TIMESTAMP NOT NULL, -- When we started tracking this miner
    is_registered BOOLEAN DEFAULT TRUE     -- Whether the miner is currently registered in the network
)
"""

# Template for creating miner-specific position tables
CREATE_MINER_POSITIONS_TABLE = """
CREATE TABLE IF NOT EXISTS positions_{hotkey} (
    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
    netuid INTEGER NOT NULL,              -- Subnet ID where position was opened
    coldkey TEXT NOT NULL,                -- Nominator coldkey for this position
    entry_block INTEGER NOT NULL,         -- Block number when position was opened
    entry_timestamp TIMESTAMP NOT NULL,   -- When the position was opened
    entry_tao REAL NOT NULL,             -- Amount of TAO staked
    entry_alpha REAL NOT NULL,           -- Amount of alpha received initially
    remaining_alpha REAL NOT NULL,        -- Amount of alpha not yet closed (for partial closes)
    status TEXT NOT NULL,                -- open/partial/closed
    extrinsic_id TEXT UNIQUE NOT NULL,    -- Unique blockchain transaction ID
    final_roi REAL DEFAULT NULL,         -- Final ROI for closed positions
    closed_at TIMESTAMP DEFAULT NULL,    -- When the position was fully closed
    CONSTRAINT status_check CHECK (status IN ('open', 'partial', 'closed')),
    CONSTRAINT roi_check CHECK (
        (status != 'closed' AND final_roi IS NULL AND closed_at IS NULL) OR
        (status = 'closed' AND final_roi IS NOT NULL AND closed_at IS NOT NULL)
    )
)
"""

# Template for creating miner-specific trade tables
CREATE_MINER_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS trades_{hotkey} (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    netuid INTEGER NOT NULL,              -- Subnet ID where trade occurred
    coldkey TEXT NOT NULL,                -- Nominator coldkey for this trade
    exit_block INTEGER NOT NULL,          -- Block number when trade executed
    exit_timestamp TIMESTAMP NOT NULL,    -- When the trade occurred
    exit_tao REAL NOT NULL,              -- Total TAO received in this trade
    exit_alpha REAL NOT NULL,            -- Total alpha returned in this trade
    extrinsic_id TEXT UNIQUE NOT NULL     -- Unique blockchain transaction ID
)
"""

# Template for creating position-trade mapping table
CREATE_MINER_POSITION_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS position_trades_{hotkey} (
    position_id INTEGER NOT NULL,         -- Reference to the position being closed
    trade_id INTEGER NOT NULL,           -- Reference to the closing trade
    alpha_amount REAL NOT NULL,          -- Amount of alpha closed from this position in this trade
    tao_amount REAL NOT NULL,            -- Portion of trade's TAO allocated to this position close
    roi_tao REAL NOT NULL,              -- ROI for this specific position portion
    duration INTEGER NOT NULL,           -- Time between position entry and this trade
    PRIMARY KEY (position_id, trade_id),
    FOREIGN KEY (position_id) REFERENCES positions_{hotkey}(position_id),
    FOREIGN KEY (trade_id) REFERENCES trades_{hotkey}(trade_id)
)
"""

# Indexes for optimizing common queries

# Position indexes
CREATE_MINER_POSITION_INDEXES = [
    # Find positions by status (e.g., all open positions)
    "CREATE INDEX IF NOT EXISTS idx_pos_{hotkey}_status ON positions_{hotkey}(status)",
    
    # Sort positions by entry time for FIFO processing
    "CREATE INDEX IF NOT EXISTS idx_pos_{hotkey}_entry_time ON positions_{hotkey}(entry_timestamp)",
    
    # Find positions in a specific subnet
    "CREATE INDEX IF NOT EXISTS idx_pos_{hotkey}_netuid ON positions_{hotkey}(netuid)",
    
    # Find positions by coldkey
    "CREATE INDEX IF NOT EXISTS idx_pos_{hotkey}_coldkey ON positions_{hotkey}(coldkey)",
    
    # Find positions by extrinsic_id
    "CREATE INDEX IF NOT EXISTS idx_pos_{hotkey}_extrinsic_id ON positions_{hotkey}(extrinsic_id)"
]

# Trade indexes
CREATE_MINER_TRADE_INDEXES = [
    # Sort trades by exit time for chronological processing
    "CREATE INDEX IF NOT EXISTS idx_trade_{hotkey}_exit_time ON trades_{hotkey}(exit_timestamp)",
    
    # Find trades in a specific subnet
    "CREATE INDEX IF NOT EXISTS idx_trade_{hotkey}_netuid ON trades_{hotkey}(netuid)",
    
    # Find trades by coldkey
    "CREATE INDEX IF NOT EXISTS idx_trade_{hotkey}_coldkey ON trades_{hotkey}(coldkey)",
    
    # Find trades by extrinsic_id
    "CREATE INDEX IF NOT EXISTS idx_trade_{hotkey}_extrinsic_id ON trades_{hotkey}(extrinsic_id)"
]

# Position-Trade mapping indexes
CREATE_MINER_POSITION_TRADE_INDEXES = [
    # Find all trades that closed a position
    "CREATE INDEX IF NOT EXISTS idx_pt_{hotkey}_position ON position_trades_{hotkey}(position_id)",
    
    # Find all positions closed in a trade
    "CREATE INDEX IF NOT EXISTS idx_pt_{hotkey}_trade ON position_trades_{hotkey}(trade_id)"
]

# Performance tracking tables
CREATE_PERFORMANCE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS performance_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    miner_uid INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_trades INTEGER NOT NULL,
    total_volume_tao REAL NOT NULL,
    total_volume_alpha REAL NOT NULL,
    roi_simple REAL NOT NULL,
    roi_weighted REAL NOT NULL,
    win_rate REAL NOT NULL,
    avg_trade_duration INTEGER NOT NULL,
    open_positions INTEGER NOT NULL,
    avg_position_age INTEGER NOT NULL,
    final_score REAL,
    FOREIGN KEY (miner_uid) REFERENCES miners(uid),
    CONSTRAINT window_check CHECK (window_start < window_end)
)
"""

# Performance snapshot indexes
CREATE_PERFORMANCE_SNAPSHOT_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_snapshots_miner_time 
    ON performance_snapshots(miner_uid, timestamp)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_snapshots_window 
    ON performance_snapshots(window_start, window_end)
    """
]

# Archive tables for historical data
CREATE_ARCHIVE_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS archived_position_summaries (
        archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
        miner_uid INTEGER NOT NULL,
        archive_date TIMESTAMP NOT NULL,
        positions_archived INTEGER NOT NULL,
        total_alpha REAL NOT NULL,
        total_tao REAL NOT NULL,
        avg_roi REAL NOT NULL,
        avg_duration INTEGER NOT NULL,
        FOREIGN KEY (miner_uid) REFERENCES miners(uid)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archived_trade_summaries (
        archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
        miner_uid INTEGER NOT NULL,
        archive_date TIMESTAMP NOT NULL,
        trades_archived INTEGER NOT NULL,
        total_alpha REAL NOT NULL,
        total_tao REAL NOT NULL,
        FOREIGN KEY (miner_uid) REFERENCES miners(uid)
    )
    """
]

CREATE_ARCHIVE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pos_archive_date ON archived_position_summaries(archive_date)",
    "CREATE INDEX IF NOT EXISTS idx_pos_archive_miner ON archived_position_summaries(miner_uid)",
    "CREATE INDEX IF NOT EXISTS idx_trade_archive_date ON archived_trade_summaries(archive_date)",
    "CREATE INDEX IF NOT EXISTS idx_trade_archive_miner ON archived_trade_summaries(miner_uid)"
]

# Template for creating miner-specific archive tables
CREATE_MINER_ARCHIVED_POSITIONS_TABLE = """
CREATE TABLE IF NOT EXISTS miner_{uid}_archived_positions (
    position_id INTEGER PRIMARY KEY,
    netuid INTEGER NOT NULL,
    hotkey TEXT NOT NULL,
    entry_block INTEGER NOT NULL,
    entry_timestamp DATETIME NOT NULL,
    entry_tao REAL NOT NULL,
    entry_alpha REAL NOT NULL,
    remaining_alpha REAL NOT NULL,
    status TEXT NOT NULL,
    extrinsic_id TEXT UNIQUE NOT NULL,
    CHECK (status IN ('open', 'partial', 'closed')),
    CHECK (entry_tao > 0),
    CHECK (entry_alpha > 0),
    CHECK (remaining_alpha >= 0)
)
"""

CREATE_MINER_ARCHIVED_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS miner_{uid}_archived_trades (
    trade_id INTEGER PRIMARY KEY,
    netuid INTEGER NOT NULL,
    hotkey TEXT NOT NULL,
    exit_block INTEGER NOT NULL,
    exit_timestamp DATETIME NOT NULL,
    exit_tao REAL NOT NULL,
    exit_alpha REAL NOT NULL,
    extrinsic_id TEXT UNIQUE NOT NULL,
    CHECK (exit_tao > 0),
    CHECK (exit_alpha > 0)
)
"""

CREATE_MINER_ARCHIVED_POSITION_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS miner_{uid}_archived_position_trades (
    position_id INTEGER NOT NULL,
    trade_id INTEGER NOT NULL,
    alpha_amount REAL NOT NULL,
    tao_amount REAL NOT NULL,
    roi_tao REAL NOT NULL,
    duration INTEGER NOT NULL,
    PRIMARY KEY (position_id, trade_id),
    CHECK (alpha_amount > 0),
    CHECK (tao_amount > 0),
    CHECK (duration >= 0)
)
"""