"""
Environment variable utilities for the Alpha Trade Exchange subnet.

This module provides utilities for loading and accessing environment variables,
particularly API keys and other sensitive configuration that should not be
hardcoded or committed to version control.

Usage:
    from alphatrade.utils.env import load_env, get_env

    # Load environment variables from .env file
    load_env()

    # Get an environment variable with a default value
    api_key = get_env('TAOSTATS_API_KEY', 'default_key')
"""

import os
import logging
import bittensor as bt
from pathlib import Path
from typing import Optional, Dict, Any

# Global cache for loaded environment variables
_ENV_CACHE: Dict[str, str] = {}
_ENV_LOADED = False

def load_env(env_file: Optional[str] = None) -> bool:
    """Load environment variables from a .env file.
    
    Args:
        env_file: Path to .env file. If None, looks in current directory
                 and parent directories up to the project root.
                 
    Returns:
        bool: True if .env file was found and loaded, False otherwise.
    """
    global _ENV_LOADED, _ENV_CACHE
    
    if _ENV_LOADED:
        return True
        
    # If no env_file is specified, look for .env in current and parent directories
    if env_file is None:
        # Start with current directory
        current_dir = Path.cwd()
        
        # Look for .env in current directory and parents up to root
        while current_dir != current_dir.parent:
            env_path = current_dir / '.env'
            if env_path.exists():
                env_file = str(env_path)
                break
            current_dir = current_dir.parent
    
    # If we found or were given an env_file, load it
    if env_file and Path(env_file).exists():
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key-value pairs
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Remove quotes if present
                        if (value.startswith('"') and value.endswith('"')) or \
                           (value.startswith("'") and value.endswith("'")):
                            value = value[1:-1]
                            
                        # Set environment variable if not already set
                        if key not in os.environ:
                            os.environ[key] = value
                            _ENV_CACHE[key] = value
                            
            bt.logging.info(f"Loaded environment variables from {env_file}")
            _ENV_LOADED = True
            return True
            
        except Exception as e:
            bt.logging.error(f"Error loading .env file: {str(e)}")
            return False
    else:
        bt.logging.warning("No .env file found")
        return False

def get_env(key: str, default: Any = None) -> str:
    """Get an environment variable.
    
    Args:
        key: Environment variable name
        default: Default value if not found
        
    Returns:
        str: Environment variable value or default
    """
    # Try to load .env file if not already loaded
    if not _ENV_LOADED:
        load_env()
        
    # First check actual environment variables
    value = os.environ.get(key)
    
    # Then check our cache
    if value is None:
        value = _ENV_CACHE.get(key)
        
    # Finally return default if still not found
    if value is None:
        if default is not None:
            return default
        else:
            bt.logging.warning(f"Environment variable {key} not found and no default provided")
            return ""
            
    return value

def set_env(key: str, value: str) -> None:
    """Set an environment variable in memory (does not modify .env file).
    
    Args:
        key: Environment variable name
        value: Value to set
    """
    os.environ[key] = value
    _ENV_CACHE[key] = value 