"""
Downloader module for map tile downloader.
Handles downloading tiles with various strategies and IP rotation.
"""

from .strategies import DownloadStrategy, RateLimitStrategy, TimeBasedStrategy
from .pool import IPPool
from .core import TileDownloader

__all__ = ['TileDownloader', 'DownloadStrategy', 'RateLimitStrategy', 'TimeBasedStrategy', 'IPPool']
