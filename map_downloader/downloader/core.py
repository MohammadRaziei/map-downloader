"""
Core downloader functionality for map tiles.
"""
import os
import logging
import requests
from typing import Dict, List, Optional, Tuple, Union, Any
from pathlib import Path
from urllib.parse import urlparse

from .strategies import DownloadStrategy, create_strategy
from .pool import IPPool

logger = logging.getLogger(__name__)

class TileDownloader:
    """Handles downloading of map tiles with various strategies."""
    
    def __init__(self, config: Dict[str, Any], output_dir: str):
        """Initialize the TileDownloader.
        
        Args:
            config: Configuration dictionary
            output_dir: Directory to save downloaded tiles
        """
        self.config = config
        self.output_dir = output_dir
        self.strategies: List[DownloadStrategy] = []
        self.ip_pool: Optional[IPPool] = None
        self.session = requests.Session()
        self._initialize()
    
    def _initialize(self):
        """Initialize the downloader with configured strategies and IP pool."""
        # Set up download strategies
        for strategy_config in self.config.get('download_strategies', []):
            strategy = create_strategy(strategy_config)
            if strategy:
                self.strategies.append(strategy)
        
        # Set up IP pool if enabled
        if self.config.get('ip_pool', {}).get('enabled', False):
            self.ip_pool = IPPool(self.config['ip_pool'])
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Initialized TileDownloader with {len(self.strategies)} strategies")
    
    def download_tile(self, url: str, x: int, y: int, z: int, retries: int = 3) -> Optional[bytes]:
        """Download a single map tile.
        
        Args:
            url: URL template for the tile
            x: X coordinate of the tile
            y: Y coordinate of the tile
            z: Zoom level
            retries: Number of retry attempts
            
        Returns:
            Tile data as bytes if successful, None otherwise
        """
        # Format URL with tile coordinates
        tile_url = url.format(x=x, y=y, z=z)
        
        for attempt in range(retries):
            try:
                # Apply download strategies before making the request
                for strategy in self.strategies:
                    strategy.before_download()
                
                # Prepare request parameters
                headers = self.config.get('headers', {})
                proxies = None
                
                # Get proxy if IP pool is enabled
                if self.ip_pool:
                    ip_address = self.ip_pool.get_next_address()
                    if ip_address:
                        proxies = self.ip_pool.get_proxy_dict(ip_address)
                
                # Make the request
                logger.debug(f"Downloading tile: {tile_url}")
                response = self.session.get(
                    tile_url,
                    headers=headers,
                    proxies=proxies,
                    timeout=30
                )
                response.raise_for_status()
                
                # Mark success in IP pool if used
                if self.ip_pool and ip_address:
                    self.ip_pool.mark_success(ip_address)
                
                # Apply download strategies after successful download
                for strategy in self.strategies:
                    strategy.after_download(success=True)
                
                return response.content
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download tile {tile_url} (attempt {attempt + 1}/{retries}): {e}")
                
                # Mark failure in IP pool if used
                if self.ip_pool and ip_address:
                    self.ip_pool.mark_failure(ip_address)
                
                # Apply download strategies after failed download
                for strategy in self.strategies:
                    strategy.after_download(success=False)
                
                # If we've exhausted retries, re-raise the exception
                if attempt == retries - 1:
                    logger.error(f"All {retries} attempts failed for tile {tile_url}")
                    raise
                
                # Wait before retrying
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def save_tile(self, tile_data: bytes, x: int, y: int, z: int, format: str = 'png') -> str:
        """Save a downloaded tile to disk.
        
        Args:
            tile_data: Raw tile data
            x: X coordinate
            y: Y coordinate
            z: Zoom level
            format: Image format (e.g., 'png', 'jpg')
            
        Returns:
            Path to the saved tile
        """
        # Create directory structure: z/x/y.format
        tile_dir = os.path.join(self.output_dir, str(z), str(x))
        os.makedirs(tile_dir, exist_ok=True)
        
        # Save the tile
        tile_path = os.path.join(tile_dir, f"{y}.{format}")
        with open(tile_path, 'wb') as f:
            f.write(tile_data)
        
        logger.debug(f"Saved tile to {tile_path}")
        return tile_path
    
    def download_tile_range(self, url_template: str, bounds: Dict[str, float], 
                          zoom_levels: List[int], format: str = 'png') -> List[str]:
        """Download a range of map tiles.
        
        Args:
            url_template: URL template with {x}, {y}, {z} placeholders
            bounds: Dictionary with min_lat, min_lon, max_lat, max_lon
            zoom_levels: List of zoom levels to download
            format: Image format for saved tiles
            
        Returns:
            List of paths to downloaded tiles
        """
        downloaded_files = []
        
        for z in zoom_levels:
            # Convert lat/lon to tile coordinates
            min_x, min_y = self.deg2tile(bounds['min_lat'], bounds['min_lon'], z)
            max_x, max_y = self.deg2tile(bounds['max_lat'], bounds['max_lon'], z)
            
            # Ensure min/max are in the right order
            min_x, max_x = min(min_x, max_x), max(min_x, max_x)
            min_y, max_y = min(min_y, max_y), max(min_y, max_y)
            
            logger.info(f"Downloading zoom level {z}: x={min_x}-{max_x}, y={min_y}-{max_y}")
            
            # Download all tiles in the range
            for x in range(min_x, max_x + 1):
                for y in range(min_y, max_y + 1):
                    try:
                        tile_data = self.download_tile(url_template, x, y, z)
                        if tile_data:
                            tile_path = self.save_tile(tile_data, x, y, z, format)
                            downloaded_files.append(tile_path)
                    except Exception as e:
                        logger.error(f"Error downloading tile z={z}, x={x}, y={y}: {e}")
                        continue
        
        return downloaded_files
    
    @staticmethod
    def deg2tile(lat_deg: float, lon_deg: float, zoom: int) -> Tuple[int, int]:
        """Convert lat/lon to tile coordinates.
        
        Args:
            lat_deg: Latitude in degrees
            lon_deg: Longitude in degrees
            zoom: Zoom level
            
        Returns:
            Tuple of (x, y) tile coordinates
        """
        import math
        
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        x = int((lon_deg + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        
        return x, y
