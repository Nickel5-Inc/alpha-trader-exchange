"""
API client package for the Alpha Trade Exchange subnet.

This package provides clients for interacting with external APIs
such as TaoStats for fetching alpha token trading data and metrics.
"""

from alphatrade.utils.env import load_env

# Load environment variables when the module is imported
load_env()

# Import API clients
from .api import AlphaAPI

__all__ = ['AlphaAPI']
