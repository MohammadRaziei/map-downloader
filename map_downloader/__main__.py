"""
Main entry point for the map tile downloader application.
"""
import os
import sys
import time
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.absolute()))

from map_downloader.config import Config
from map_downloader.downloader import TileDownloader, DownloadStrategy
from map_downloader.mbtiles import MBTilesGenerator
from map_downloader.storage import create_storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('map_downloader.log')
    ]
)

logger = logging.getLogger(__name__)

class MapTileDownloader:
    """Main application class for downloading map tiles."""
    
    def __init__(self, config_path: str):
        """Initialize the map tile downloader.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.storage_backends = self._initialize_storage_backends()
        self.downloader = self._initialize_downloader()
    
    def _load_config(self) -> Config:
        """Load and validate the configuration."""
        try:
            config = Config.from_yaml(self.config_path)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def _initialize_storage_backends(self) -> List[Any]:
        """Initialize storage backends from configuration."""
        backends = []
        for dest in self.config.destinations:
            try:
                backend = create_storage({
                    'type': dest.type,
                    'path': dest.path,
                    'endpoint': getattr(dest, 'endpoint', ''),
                    'access_key': getattr(dest, 'access_key', ''),
                    'secret_key': getattr(dest, 'secret_key', ''),
                    'bucket_name': getattr(dest, 'bucket_name', ''),
                    'secure': getattr(dest, 'secure', True),
                    'region': getattr(dest, 'region', None)
                })
                backends.append(backend)
                logger.info(f"Initialized {dest.type} storage backend")
            except Exception as e:
                logger.error(f"Failed to initialize {dest.type} storage backend: {e}")
                raise
        
        return backends
    
    def _initialize_downloader(self) -> TileDownloader:
        """Initialize the tile downloader with configuration."""
        return TileDownloader(
            config={
                'download_strategies': [
                    {'type': s.type, **s.params} 
                    for s in self.config.download_strategies
                ],
                'ip_pool': {
                    'enabled': self.config.ip_pool.enabled,
                    'provider': self.config.ip_pool.provider,
                    'credentials': self.config.ip_pool.credentials,
                    'rotation_interval': self.config.ip_pool.rotation_interval,
                    'max_failures': self.config.ip_pool.max_failures
                },
                'headers': {}
            },
            output_dir=self.config.temp_download_dir
        )
    
    def _create_mbtiles(self, source_dir: str, output_path: str) -> bool:
        """Create an MBTiles file from downloaded tiles."""
        try:
            with MBTilesGenerator(
                output_path=output_path,
                config={
                    'name': self.config.mbtiles.name,
                    'description': self.config.mbtiles.description,
                    'version': self.config.mbtiles.version,
                    'type': self.config.mbtiles.type,
                    'format': self.config.mbtiles.format,
                    'min_zoom': self.config.mbtiles.min_zoom,
                    'max_zoom': self.config.mbtiles.max_zoom,
                    'bounds': self.config.mbtiles.bounds,
                    'attribution': self.config.mbtiles.attribution
                }
            ) as mbtiles:
                for zoom in os.listdir(source_dir):
                    zoom_path = os.path.join(source_dir, zoom)
                    if not os.path.isdir(zoom_path) or not zoom.isdigit():
                        continue
                    
                    mbtiles.add_tiles_from_directory(
                        directory=zoom_path,
                        zoom=int(zoom),
                        format=self.config.mbtiles.format
                    )
                
                # Optimize the MBTiles file
                mbtiles.optimize()
            
            logger.info(f"Created MBTiles file: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error creating MBTiles file: {e}")
            return False
    
    def _save_to_destinations(self, source_path: str, dest_path: str) -> bool:
        """Save a file to all configured storage destinations."""
        success = True
        
        # Read the source file
        try:
            with open(source_path, 'rb') as f:
                data = f.read()
        except Exception as e:
            logger.error(f"Error reading source file {source_path}: {e}")
            return False
        
        # Save to each destination
        for backend in self.storage_backends:
            try:
                if not backend.save(data, dest_path):
                    logger.error(f"Failed to save to {backend.__class__.__name__}: {dest_path}")
                    success = False
                else:
                    logger.info(f"Saved to {backend.__class__.__name__}: {dest_path}")
            except Exception as e:
                logger.error(f"Error saving to {backend.__class__.__name__}: {e}")
                success = False
        
        return success
    
    def run(self):
        """Run the map tile downloader."""
        logger.info("Starting map tile downloader")
        
        # Create temp directory if it doesn't exist
        os.makedirs(self.config.temp_download_dir, exist_ok=True)
        
        # Process each source
        for source in self.config.sources:
            logger.info(f"Processing source: {source.name}")
            
            # Create a directory for this source
            source_dir = os.path.join(self.config.temp_download_dir, source.name)
            os.makedirs(source_dir, exist_ok=True)
            
            try:
                # Download tiles
                self.downloader.download_tile_range(
                    url_template=source.url_template,
                    bounds={
                        'min_lat': source.bounds.min_lat,
                        'min_lon': source.bounds.min_lon,
                        'max_lat': source.bounds.max_lat,
                        'max_lon': source.bounds.max_lon
                    },
                    zoom_levels=source.zoom_levels,
                    format='png'  # Default format, can be made configurable
                )
                
                # Create MBTiles if configured
                if self.config.output_format == 'mbtiles' and self.config.mbtiles:
                    mbtiles_path = os.path.join(
                        self.config.temp_download_dir,
                        f"{source.name}.mbtiles"
                    )
                    
                    if self._create_mbtiles(source_dir, mbtiles_path):
                        # Save MBTiles to destinations
                        dest_path = f"{source.name}.mbtiles"
                        self._save_to_destinations(mbtiles_path, dest_path)
                
                # Save individual tiles to destinations if configured
                if self.config.output_format == 'files':
                    # This would involve walking the source_dir and saving each file
                    # to the destinations. Implementation omitted for brevity.
                    pass
                
                logger.info(f"Completed processing source: {source.name}")
                
            except Exception as e:
                logger.error(f"Error processing source {source.name}: {e}", exc_info=True)
                continue
        
        logger.info("Map tile downloader finished")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Download map tiles from various sources.')
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to configuration file (default: config/config.yaml)'
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    try:
        args = parse_args()
        app = MapTileDownloader(args.config)
        app.run()
        return 0
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
