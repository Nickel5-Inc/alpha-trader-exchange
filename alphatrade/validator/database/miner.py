import asyncio
import bittensor as bt
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, text

from .manager import DatabaseManager
from .models import Miner, MinerStatus
from .exceptions import DatabaseError, ValidationError
from .validation import validate_miner_registration, validate_miner_status
from .schema import (
    CREATE_MINER_POSITIONS_TABLE,
    CREATE_MINER_TRADES_TABLE,
    CREATE_MINER_POSITION_TRADES_TABLE,
    CREATE_MINER_POSITION_INDEXES,
    CREATE_MINER_TRADE_INDEXES,
    CREATE_MINER_POSITION_TRADE_INDEXES
)

class MinerManager:
    """Manager class for miner-related database operations."""
    
    def __init__(self, db: Optional[DatabaseManager] = None):
        """Initialize miner manager.
        
        Args:
            db: Optional database manager instance. If not provided,
                gets singleton instance.
        """
        self._db = db or DatabaseManager()  # Get singleton instance if not provided
        self._initialized = False
    
    async def ensure_database_initialized(self):
        """Ensure database is initialized."""
        bt.logging.info("Ensuring database is initialized for miner manager")
        try:
            if not self._initialized:
                if not hasattr(self._db, '_connection_initialized') or not self._db._connection_initialized:
                    await self._db.setup_database()
                
                # Define CREATE_MINER_TABLE explicitly if it's not found in schema
                miners_table_sql = """
                CREATE TABLE IF NOT EXISTS miners (
                    uid INTEGER PRIMARY KEY,
                    hotkey TEXT UNIQUE NOT NULL,
                    coldkey TEXT NOT NULL UNIQUE,
                    registration_date TIMESTAMP NOT NULL,
                    last_active TIMESTAMP NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'blacklisted')),
                    total_trades INTEGER DEFAULT 0,
                    total_volume_tao REAL DEFAULT 0.0,
                    total_volume_alpha REAL DEFAULT 0.0,
                    cumulative_roi REAL DEFAULT 0.0,
                    avg_trade_duration INTEGER DEFAULT 0,
                    win_rate REAL DEFAULT 0.0,
                    current_positions INTEGER DEFAULT 0,
                    tracking_start_date TIMESTAMP NOT NULL,
                    is_registered BOOLEAN DEFAULT TRUE
                )
                """
                
                # Ensure miners table exists
                async with self._db.connection.get_connection() as conn:
                    try:
                        # Use the imported table creation SQL if available
                        try:
                            await conn.execute(CREATE_MINER_TABLE)
                        except NameError:
                            # Fall back to our explicit definition if import failed
                            bt.logging.warning("CREATE_MINER_TABLE not found in schema, using fallback definition")
                            await conn.execute(miners_table_sql)
                    except Exception as e:
                        bt.logging.error(f"Error creating miners table: {str(e)}")
                        # Try with explicit SQL as a last resort
                        await conn.execute(miners_table_sql)
                    
                    # Check if the miners table has data
                    try:
                        result = await conn.execute("SELECT COUNT(*) FROM miners")
                        count = await result.fetchone()
                        bt.logging.info(f"Found {count[0]} existing miners in database")
                    except Exception as e:
                        bt.logging.warning(f"Error checking miners count: {str(e)}")
                    
                    await conn.commit()
                
                await self.ensure_miner_tables_exist()
                self._initialized = True
                bt.logging.info("Miner manager database initialization complete")
        except Exception as e:
            bt.logging.error(f"Error initializing miner manager database: {str(e)}")
            raise

    async def ensure_miner_tables_exist(self):
        """Ensure all required tables exist for all miners.
        
        This method checks if required tables exist for all miners in the database,
        and creates them if they don't.
        
        Raises:
            DatabaseError: If table creation fails
        """
        bt.logging.info("Ensuring miner tables exist")
        try:
            async with self._db.connection.get_connection() as conn:
                # First, check if miners table exists
                try:
                    result = await conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='miners'"
                    )
                    table_exists = await result.fetchone()
                    
                    if not table_exists:
                        bt.logging.info("Creating miners table")
                        # Define miners table creation SQL inline as fallback
                        miners_table_sql = """
                        CREATE TABLE IF NOT EXISTS miners (
                            uid INTEGER PRIMARY KEY,
                            hotkey TEXT UNIQUE NOT NULL,
                            coldkey TEXT NOT NULL UNIQUE,
                            registration_date TIMESTAMP NOT NULL,
                            last_active TIMESTAMP NOT NULL,
                            status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'blacklisted')),
                            total_trades INTEGER DEFAULT 0,
                            total_volume_tao REAL DEFAULT 0.0,
                            total_volume_alpha REAL DEFAULT 0.0,
                            cumulative_roi REAL DEFAULT 0.0,
                            avg_trade_duration INTEGER DEFAULT 0,
                            win_rate REAL DEFAULT 0.0,
                            current_positions INTEGER DEFAULT 0,
                            tracking_start_date TIMESTAMP NOT NULL,
                            is_registered BOOLEAN DEFAULT TRUE
                        )
                        """
                        await conn.execute(miners_table_sql)
                        await conn.commit()
                except Exception as e:
                    bt.logging.error(f"Error checking/creating miners table: {str(e)}")
                    # Create miners table anyway just to be sure
                    miners_table_sql = """
                    CREATE TABLE IF NOT EXISTS miners (
                        uid INTEGER PRIMARY KEY,
                        hotkey TEXT UNIQUE NOT NULL,
                        coldkey TEXT NOT NULL UNIQUE,
                        registration_date TIMESTAMP NOT NULL,
                        last_active TIMESTAMP NOT NULL,
                        status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'blacklisted')),
                        total_trades INTEGER DEFAULT 0,
                        total_volume_tao REAL DEFAULT 0.0,
                        total_volume_alpha REAL DEFAULT 0.0,
                        cumulative_roi REAL DEFAULT 0.0,
                        avg_trade_duration INTEGER DEFAULT 0,
                        win_rate REAL DEFAULT 0.0,
                        current_positions INTEGER DEFAULT 0,
                        tracking_start_date TIMESTAMP NOT NULL,
                        is_registered BOOLEAN DEFAULT TRUE
                    )
                    """
                    await conn.execute(miners_table_sql)
                    await conn.commit()
                
                # Now check if there are any miners to create tables for
                try:
                    result = await conn.execute("SELECT uid FROM miners")
                    miners = await result.fetchall()
                    
                    if miners:
                        bt.logging.info(f"Found {len(miners)} miners, creating tables")
                        for miner in miners:
                            await self.ensure_miner_tables_exist_for_miner_uid(miner[0])
                    else:
                        bt.logging.info("No miners found in database")
                except Exception as e:
                    bt.logging.error(f"Error checking miners: {str(e)}")
        except Exception as e:
            bt.logging.error(f"Error ensuring miner tables exist: {str(e)}")
            raise DatabaseError(f"Failed to ensure miner tables exist: {str(e)}")
    
    async def ensure_miner_tables_exist_for_miner_uid(self, miner_uid: int) -> None:
        """Ensure all required tables exist for a miner using the miner UID as identifier.
        
        Args:
            miner_uid: Miner's UID
                
        Raises:
            DatabaseError: If table creation fails
        """
        bt.logging.info(f"Setting up tables for miner with UID {miner_uid}")
        try:
            # Use raw connection for DDL statements
            async with self._db.connection.get_connection() as conn:
                # Check if tables already exist - use exact pattern matching
                result = await conn.execute(
                    """
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE ?
                    """,
                    (f"miner_{miner_uid}_%",)  # This will match tables like miner_2_positions but not miner_20_positions
                )
                existing_tables = await result.fetchall()
                
                # Convert result to list of table names
                if existing_tables:
                    # Use exact pattern matching filter to ensure we only get tables for this specific miner
                    exact_match_pattern = f"miner_{miner_uid}_"
                    existing_table_names = []
                    for table in existing_tables:
                        table_name = table[0]
                        # Only include tables that start with the exact pattern (no partial matches)
                        # This prevents miner_20_positions from matching for miner_2
                        parts = table_name.split('_', 2)
                        if len(parts) >= 3 and parts[0] == 'miner' and parts[1] == str(miner_uid):
                            existing_table_names.append(table_name)
                else:
                    existing_table_names = []
                
                # Log existing tables
                if existing_table_names:
                    bt.logging.info(f"Found existing tables for miner {miner_uid}: {existing_table_names}")
                
                # Create positions table if needed
                positions_table = f"miner_{miner_uid}_positions"
                if positions_table not in existing_table_names:
                    bt.logging.info(f"Creating positions table for miner {miner_uid}")
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {positions_table} (
                            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            miner_uid INTEGER NOT NULL,
                            netuid INTEGER NOT NULL,
                            hotkey TEXT NOT NULL,
                            entry_block INTEGER NOT NULL,
                            entry_timestamp TIMESTAMP NOT NULL,
                            entry_tao REAL NOT NULL,
                            entry_alpha REAL NOT NULL,
                            remaining_alpha REAL NOT NULL,
                            status TEXT NOT NULL CHECK (status IN ('open', 'partial', 'closed')),
                            final_roi REAL,
                            closed_at TIMESTAMP,
                            extrinsic_id TEXT UNIQUE
                        )
                    """)
                
                # Create trades table if needed
                trades_table = f"miner_{miner_uid}_trades"
                if trades_table not in existing_table_names:
                    bt.logging.info(f"Creating trades table for miner {miner_uid}")
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {trades_table} (
                            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            miner_uid INTEGER NOT NULL,
                            netuid INTEGER NOT NULL,
                            hotkey TEXT NOT NULL,
                            exit_block INTEGER NOT NULL,
                            exit_timestamp TIMESTAMP NOT NULL,
                            exit_tao REAL NOT NULL,
                            exit_alpha REAL NOT NULL,
                            extrinsic_id TEXT UNIQUE
                        )
                    """)
                
                # Create position_trades table if needed
                position_trades_table = f"miner_{miner_uid}_position_trades"
                if position_trades_table not in existing_table_names:
                    bt.logging.info(f"Creating position_trades table for miner {miner_uid}")
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {position_trades_table} (
                            position_id INTEGER NOT NULL,
                            trade_id INTEGER NOT NULL,
                            alpha_amount REAL NOT NULL,
                            tao_amount REAL NOT NULL,
                            roi_tao REAL NOT NULL,
                            duration INTEGER NOT NULL,
                            min_blocks_met BOOLEAN DEFAULT TRUE,
                            PRIMARY KEY (position_id, trade_id),
                            FOREIGN KEY (position_id) REFERENCES {positions_table}(position_id),
                            FOREIGN KEY (trade_id) REFERENCES {trades_table}(trade_id)
                        )
                    """)
                else:
                    # Check if min_blocks_met column exists in position_trades table
                    result = await conn.execute(
                        f"""
                        PRAGMA table_info({position_trades_table})
                        """
                    )
                    columns = await result.fetchall()
                    column_names = [col[1] for col in columns]
                    
                    # Add min_blocks_met column if it doesn't exist
                    if 'min_blocks_met' not in column_names:
                        bt.logging.info(f"Adding min_blocks_met column to {position_trades_table}")
                        await conn.execute(f"""
                            ALTER TABLE {position_trades_table}
                            ADD COLUMN min_blocks_met BOOLEAN DEFAULT TRUE
                        """)
                
                # Create indexes - check if indexes exist first
                result = await conn.execute(
                    """
                    SELECT name FROM sqlite_master 
                    WHERE type='index' AND (
                        tbl_name = ? OR 
                        tbl_name = ? OR 
                        tbl_name = ?
                    )
                    """,
                    (positions_table, trades_table, position_trades_table)
                )
                existing_indexes = await result.fetchall()
                existing_index_names = [idx[0] for idx in existing_indexes]
                
                # Create position indexes
                for index_name, index_sql in [
                    (f"idx_pos_{miner_uid}_status", f"CREATE INDEX IF NOT EXISTS idx_pos_{miner_uid}_status ON {positions_table}(status)"),
                    (f"idx_pos_{miner_uid}_entry_time", f"CREATE INDEX IF NOT EXISTS idx_pos_{miner_uid}_entry_time ON {positions_table}(entry_timestamp)"),
                    (f"idx_pos_{miner_uid}_extrinsic_id", f"CREATE INDEX IF NOT EXISTS idx_pos_{miner_uid}_extrinsic_id ON {positions_table}(extrinsic_id)"),
                    (f"idx_pos_{miner_uid}_entry_block", f"CREATE INDEX IF NOT EXISTS idx_pos_{miner_uid}_entry_block ON {positions_table}(entry_block)")
                ]:
                    if index_name not in existing_index_names:
                        bt.logging.info(f"Creating index {index_name}")
                        await conn.execute(index_sql)
                
                # Create trade indexes
                for index_name, index_sql in [
                    (f"idx_trade_{miner_uid}_exit_time", f"CREATE INDEX IF NOT EXISTS idx_trade_{miner_uid}_exit_time ON {trades_table}(exit_timestamp)"),
                    (f"idx_trade_{miner_uid}_extrinsic_id", f"CREATE INDEX IF NOT EXISTS idx_trade_{miner_uid}_extrinsic_id ON {trades_table}(extrinsic_id)"),
                    (f"idx_trade_{miner_uid}_exit_block", f"CREATE INDEX IF NOT EXISTS idx_trade_{miner_uid}_exit_block ON {trades_table}(exit_block)")
                ]:
                    if index_name not in existing_index_names:
                        bt.logging.info(f"Creating index {index_name}")
                        await conn.execute(index_sql)
                
                # Create position-trade indexes
                for index_name, index_sql in [
                    (f"idx_pt_{miner_uid}_position", f"CREATE INDEX IF NOT EXISTS idx_pt_{miner_uid}_position ON {position_trades_table}(position_id)"),
                    (f"idx_pt_{miner_uid}_trade", f"CREATE INDEX IF NOT EXISTS idx_pt_{miner_uid}_trade ON {position_trades_table}(trade_id)"),
                    (f"idx_pt_{miner_uid}_min_blocks", f"CREATE INDEX IF NOT EXISTS idx_pt_{miner_uid}_min_blocks ON {position_trades_table}(min_blocks_met)")
                ]:
                    if index_name not in existing_index_names:
                        bt.logging.info(f"Creating index {index_name}")
                        await conn.execute(index_sql)
                
                # Commit all changes
                await conn.commit()
                
                bt.logging.info(f"Successfully set up tables for miner {miner_uid}")
                
        except Exception as e:
            bt.logging.error(f"Failed to ensure miner tables exist for miner {miner_uid}: {str(e)}")
            raise DatabaseError(f"Failed to ensure miner tables exist: {str(e)}")

    async def register_miner(
        self,
        coldkey: str,
        hotkey: str = None,
        tracking_start_date: datetime = None,
        status: MinerStatus = MinerStatus.ACTIVE,
        uid: Optional[int] = None
    ) -> int:
        """Register a new miner or update existing one.
        
        Args:
            coldkey: Miner's coldkey
            hotkey: Optional hotkey (defaults to coldkey)
            tracking_start_date: When to start tracking (defaults to now)
            status: Initial miner status
            uid: Optional UID to use
            
        Returns:
            int: Miner's UID
            
        Raises:
            ValidationError: If registration data is invalid
            DatabaseError: If registration fails
        """
        await self.ensure_database_initialized()
        
        # Use coldkey as hotkey if not provided
        hotkey = hotkey or coldkey
        
        # Use current time if no start date provided
        if tracking_start_date is None:
            tracking_start_date = datetime.now(timezone.utc)
            
        # Validate inputs
        validate_miner_registration(coldkey, hotkey, tracking_start_date, status)
        
        async with self._db.connection.get_connection() as conn:
            try:
                # First check if a miner with specified coldkey/hotkey exists
                result = await conn.execute(
                    "SELECT uid FROM miners WHERE coldkey = ? OR hotkey = ?",
                    (coldkey, hotkey)
                )
                existing_by_key = await result.fetchone()
                
                # Also check if a miner with specified UID exists (if UID was provided)
                existing_by_uid = None
                if uid is not None:
                    result = await conn.execute(
                        "SELECT uid FROM miners WHERE uid = ?",
                        (uid,)
                    )
                    existing_by_uid = await result.fetchone()
                
                # Handle the case where UID is specified but already exists with different coldkey
                if existing_by_uid and not existing_by_key:
                    # A miner with this UID exists but has different coldkey/hotkey
                    # This is a conflicting situation - we'll log this and use the existing miner's UID
                    bt.logging.warning(f"Attempted to register miner {coldkey} with UID {uid}, but UID already exists with different coldkey. Using existing miner's UID.")
                    # Return the existing UID so tables aren't recreated with a conflicting UID
                    return existing_by_uid[0]
                
                if existing_by_key:
                    # Update existing miner
                    existing_uid = existing_by_key[0]
                    
                    # If UID was provided but doesn't match existing, log warning
                    if uid is not None and existing_uid != uid:
                        bt.logging.warning(f"Miner {coldkey} already exists with UID {existing_uid}, ignoring requested UID {uid}")
                    
                    await conn.execute(
                        """
                        UPDATE miners 
                        SET coldkey = ?,
                            hotkey = ?,
                            status = ?,
                            is_registered = 1,
                            last_active = ?
                        WHERE uid = ?
                        """,
                        (
                            coldkey,
                            hotkey,
                            status.value,
                            datetime.now(timezone.utc).isoformat(),
                            existing_uid
                        )
                    )
                    new_uid = existing_uid
                else:
                    # Create new miner
                    # Now the tables will be created with a UID that doesn't conflict
                    await conn.execute(
                        """
                        INSERT INTO miners (
                            uid,
                            coldkey,
                            hotkey,
                            registration_date,
                            last_active,
                            status,
                            tracking_start_date,
                            is_registered,
                            total_trades,
                            total_volume_tao,
                            total_volume_alpha,
                            cumulative_roi,
                            avg_trade_duration,
                            win_rate,
                            current_positions
                        ) VALUES (
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            1,
                            0,
                            0.0,
                            0.0,
                            0.0,
                            0,
                            0.0,
                            0
                        )
                        """,
                        (
                            uid,
                            coldkey,
                            hotkey,
                            datetime.now(timezone.utc).isoformat(),
                            datetime.now(timezone.utc).isoformat(),
                            status.value,
                            tracking_start_date.isoformat()
                        )
                    )
                    
                    # Get the new miner's UID
                    if uid is not None:
                        new_uid = uid
                    else:
                        cursor = await conn.execute("SELECT last_insert_rowid()")
                        new_uid = (await cursor.fetchone())[0]
                
                # Commit changes
                await conn.commit()
                
            except Exception as e:
                bt.logging.error(f"Error registering miner: {str(e)}")
                await conn.rollback()
                raise DatabaseError(f"Failed to register miner: {str(e)}")
                
        # Now that we have new_uid, ensure miner tables exist
        await self.ensure_miner_tables_exist_for_miner_uid(new_uid)
        return new_uid

    async def _create_miner_tables(self, session: AsyncSession, miner_uid: int) -> None:
        """Create tables for a new miner.
        
        Args:
            session: Database session
            miner_uid: Miner's UID
        """
        # Create position table
        await session.execute(
            text(f"""
            CREATE TABLE IF NOT EXISTS miner_{miner_uid}_positions (
                position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                miner_uid INTEGER NOT NULL,
                netuid INTEGER NOT NULL,
                hotkey TEXT NOT NULL,
                entry_block INTEGER NOT NULL,
                entry_timestamp TIMESTAMP NOT NULL,
                entry_tao REAL NOT NULL,
                entry_alpha REAL NOT NULL,
                remaining_alpha REAL NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('open', 'partial', 'closed')),
                final_roi REAL,
                closed_at TIMESTAMP,
                extrinsic_id TEXT UNIQUE
            )
            """)
        )
        
        # Create trade table
        await session.execute(
            text(f"""
            CREATE TABLE IF NOT EXISTS miner_{miner_uid}_trades (
                trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                miner_uid INTEGER NOT NULL,
                netuid INTEGER NOT NULL,
                hotkey TEXT NOT NULL,
                exit_block INTEGER NOT NULL,
                exit_timestamp TIMESTAMP NOT NULL,
                exit_tao REAL NOT NULL,
                exit_alpha REAL NOT NULL,
                extrinsic_id TEXT UNIQUE
            )
            """)
        )
        
        # Create position-trade mapping table
        await session.execute(
            text(f"""
            CREATE TABLE IF NOT EXISTS miner_{miner_uid}_position_trades (
                position_id INTEGER NOT NULL,
                trade_id INTEGER NOT NULL,
                alpha_amount REAL NOT NULL,
                tao_amount REAL NOT NULL,
                roi_tao REAL NOT NULL,
                duration INTEGER NOT NULL,
                PRIMARY KEY (position_id, trade_id),
                FOREIGN KEY (position_id) REFERENCES miner_{miner_uid}_positions(position_id),
                FOREIGN KEY (trade_id) REFERENCES miner_{miner_uid}_trades(trade_id)
            )
            """)
        )
        
        # Create indexes
        await session.execute(
            text(f"""
            CREATE INDEX IF NOT EXISTS idx_pos_{miner_uid}_status 
            ON miner_{miner_uid}_positions(status)
            """)
        )
        await session.execute(
            text(f"""
            CREATE INDEX IF NOT EXISTS idx_pos_{miner_uid}_entry_time 
            ON miner_{miner_uid}_positions(entry_timestamp)
            """)
        )
        await session.execute(
            text(f"""
            CREATE INDEX IF NOT EXISTS idx_trade_{miner_uid}_exit_time 
            ON miner_{miner_uid}_trades(exit_timestamp)
            """)
        )
        
        # Create indexes for extrinsic_id
        await session.execute(
            text(f"""
            CREATE INDEX IF NOT EXISTS idx_pos_{miner_uid}_extrinsic_id 
            ON miner_{miner_uid}_positions(extrinsic_id)
            """)
        )
        await session.execute(
            text(f"""
            CREATE INDEX IF NOT EXISTS idx_trade_{miner_uid}_extrinsic_id 
            ON miner_{miner_uid}_trades(extrinsic_id)
            """)
        )

    async def create_miner_tables(self, miner_uid: int) -> None:
        """Create tables for a new miner.
        
        Args:
            miner_uid: Miner's UID
            
        Raises:
            DatabaseError: If table creation fails
        """
        await self.ensure_miner_tables_exist_for_miner_uid(miner_uid)

    async def get_miner(self, coldkey: str) -> Optional[Miner]:
        """Get miner by coldkey.
        
        Args:
            coldkey: Miner's coldkey
            
        Returns:
            Optional[Miner]: Miner if found, None otherwise
            
        Raises:
            DatabaseError: If query fails
        """
        async with self._db.connection.session() as session:
            try:
                result = await session.execute(
                    text("""
                    SELECT * FROM miners 
                    WHERE coldkey = :coldkey
                    """),
                    {"coldkey": coldkey}
                )
                row = result.fetchone()
                if row:
                    # Convert datetime strings to datetime objects
                    registration_date = datetime.fromisoformat(str(row[3])) if row[3] else None
                    last_active = datetime.fromisoformat(str(row[4])) if row[4] else None
                    tracking_start_date = datetime.fromisoformat(str(row[6])) if row[6] else None
                    
                    return Miner(
                        uid=row[0],  # uid
                        coldkey=row[1],  # coldkey
                        hotkey=row[2],  # hotkey
                        registration_date=registration_date,  # registration_date
                        last_active=last_active,  # last_active
                        status=MinerStatus(row[5]),  # status
                        tracking_start_date=tracking_start_date,  # tracking_start_date
                        total_trades=row[7],  # total_trades
                        total_volume_tao=row[8],  # total_volume_tao
                        total_volume_alpha=row[9],  # total_volume_alpha
                        cumulative_roi=row[10],  # cumulative_roi
                        avg_trade_duration=row[11],  # avg_trade_duration
                        win_rate=row[12],  # win_rate
                        current_positions=row[13]  # current_positions
                    )
                return None
            except Exception as e:
                raise DatabaseError(f"Failed to get miner: {str(e)}")

    async def get_miner_by_uid(self, uid: int) -> Optional[Miner]:
        """Get miner by UID.
        
        Args:
            uid: Miner's UID
            
        Returns:
            Optional[Miner]: Miner if found, None otherwise
            
        Raises:
            DatabaseError: If query fails
        """
        async with self._db.connection.session() as session:
            try:
                result = await session.execute(
                    text("""
                    SELECT * FROM miners 
                    WHERE uid = :uid
                    """),
                    {"uid": uid}
                )
                row = result.fetchone()
                if row:
                    # Convert datetime strings to datetime objects
                    registration_date = datetime.fromisoformat(str(row[3])) if row[3] else None
                    last_active = datetime.fromisoformat(str(row[4])) if row[4] else None
                    tracking_start_date = datetime.fromisoformat(str(row[6])) if row[6] else None
                    
                    return Miner(
                        uid=row[0],  # uid
                        coldkey=row[1],  # coldkey
                        hotkey=row[2],  # hotkey
                        registration_date=registration_date,  # registration_date
                        last_active=last_active,  # last_active
                        status=MinerStatus(row[5]),  # status
                        tracking_start_date=tracking_start_date,  # tracking_start_date
                        total_trades=row[7],  # total_trades
                        total_volume_tao=row[8],  # total_volume_tao
                        total_volume_alpha=row[9],  # total_volume_alpha
                        cumulative_roi=row[10],  # cumulative_roi
                        avg_trade_duration=row[11],  # avg_trade_duration
                        win_rate=row[12],  # win_rate
                        current_positions=row[13]  # current_positions
                    )
                return None
            except Exception as e:
                raise DatabaseError(f"Failed to get miner: {str(e)}")

    async def get_miner_by_hotkey(self, hotkey: str) -> Optional[Miner]:
        """Get miner by hotkey.
        
        Args:
            hotkey: Miner's hotkey
            
        Returns:
            Optional[Miner]: Miner if found, None otherwise
            
        Raises:
            DatabaseError: If query fails
        """
        async with self._db.connection.session() as session:
            try:
                result = await session.execute(
                    text("""
                    SELECT * FROM miners 
                    WHERE hotkey = :hotkey
                    """),
                    {"hotkey": hotkey}
                )
                row = result.fetchone()
                if row:
                    # Convert datetime strings to datetime objects
                    registration_date = datetime.fromisoformat(str(row[3])) if row[3] else None
                    last_active = datetime.fromisoformat(str(row[4])) if row[4] else None
                    tracking_start_date = datetime.fromisoformat(str(row[6])) if row[6] else None
                    
                    return Miner(
                        uid=row[0],  # uid
                        coldkey=row[1],  # coldkey
                        hotkey=row[2],  # hotkey
                        registration_date=registration_date,  # registration_date
                        last_active=last_active,  # last_active
                        status=MinerStatus(row[5]),  # status
                        tracking_start_date=tracking_start_date,  # tracking_start_date
                        total_trades=row[7],  # total_trades
                        total_volume_tao=row[8],  # total_volume_tao
                        total_volume_alpha=row[9],  # total_volume_alpha
                        cumulative_roi=row[10],  # cumulative_roi
                        avg_trade_duration=row[11],  # avg_trade_duration
                        win_rate=row[12],  # win_rate
                        current_positions=row[13]  # current_positions
                    )
                return None
            except Exception as e:
                raise DatabaseError(f"Failed to get miner: {str(e)}")

    async def update_miner_status(
        self,
        uid: int,
        status: MinerStatus
    ) -> bool:
        """Update miner status.
        
        Args:
            uid: Miner's UID
            status: New status
            
        Returns:
            bool: True if updated, False if miner not found
            
        Raises:
            ValidationError: If status is invalid
            DatabaseError: If update fails
        """
        validate_miner_status(status)
        
        async def txn(session: AsyncSession) -> bool:
            result = await session.execute(
                select(Miner).where(Miner.uid == uid)
            )
            miner = result.scalar_one_or_none()
            
            if miner:
                miner.status = status
                miner.last_active = datetime.now(timezone.utc)
                return True
            return False
            
        return await self._db.connection._run_transaction(txn)