"""
MBTiles generator for map tiles.
Converts downloaded map tiles into MBTiles format.
"""
import os
import sqlite3
import json
import logging
from typing import Dict, List, Optional, Tuple, Union, Any
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class MBTilesGenerator:
    """Handles generation of MBTiles files from downloaded map tiles."""
    
    def __init__(self, output_path: str, config: Dict[str, Any]):
        """Initialize the MBTiles generator.
        
        Args:
            output_path: Path to save the MBTiles file
            config: Configuration dictionary with MBTiles settings
        """
        self.output_path = output_path
        self.config = config
        self.connection = None
        self.cursor = None
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        # Set default metadata
        self.metadata = {
            'name': config.get('name', 'map_tiles'),
            'description': config.get('description', 'Map tiles'),
            'version': config.get('version', '1.0'),
            'type': config.get('type', 'baselayer'),
            'format': config.get('format', 'png'),
            'bounds': config.get('bounds', '-180.0,-85.0511,180.0,85.0511'),
            'attribution': config.get('attribution', ''),
            'minzoom': config.get('min_zoom', 0),
            'maxzoom': config.get('max_zoom', 22)
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.create_mbtiles()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def create_mbtiles(self):
        """Create a new MBTiles file with the appropriate schema."""
        # Remove existing file if it exists
        if os.path.exists(self.output_path):
            os.remove(self.output_path)
        
        # Create SQLite database
        self.connection = sqlite3.connect(self.output_path)
        self.cursor = self.connection.cursor()
        
        # Create tables
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            name text,
            value text
        )
        """)
        
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS tiles (
            zoom_level integer,
            tile_column integer,
            tile_row integer,
            tile_data blob,
            PRIMARY KEY (zoom_level, tile_column, tile_row)
        )
        """)
        
        # Add index for faster lookups
        self.cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS tile_index 
        ON tiles (zoom_level, tile_column, tile_row)
        """)
        
        # Add metadata
        for key, value in self.metadata.items():
            self.cursor.execute(
                "INSERT INTO metadata (name, value) VALUES (?, ?)",
                (key, str(value))
            )
        
        # Add creation time
        self.cursor.execute(
            "INSERT INTO metadata (name, value) VALUES (?, ?)",
            ('generator', 'Map Tile Downloader')
        )
        
        self.connection.commit()
        logger.info(f"Created new MBTiles file: {self.output_path}")
    
    def add_tile(self, zoom: int, x: int, y: int, tile_data: bytes):
        """Add a tile to the MBTiles file.
        
        Args:
            zoom: Zoom level
            x: X coordinate (tile column)
            y: Y coordinate (tile row, in TMS format)
            tile_data: Raw tile data
        """
        if not self.connection:
            raise RuntimeError("MBTiles file not open. Call create_mbtiles() first.")
        
        # Flip Y coordinate (TMS to XYZ)
        y_flipped = (2 ** zoom - 1) - y
        
        try:
            self.cursor.execute(
                "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                (zoom, x, y_flipped, sqlite3.Binary(tile_data))
            )
            self.connection.commit()
            logger.debug(f"Added tile z={zoom}, x={x}, y={y} to MBTiles")
        except sqlite3.Error as e:
            logger.error(f"Error adding tile z={zoom}, x={x}, y={y}: {e}")
            self.connection.rollback()
            raise
    
    def add_tiles_from_directory(self, directory: str, zoom: int, format: str = 'png'):
        """Add all tiles from a directory to the MBTiles file.
        
        Args:
            directory: Directory containing tiles in z/x/y.format structure
            zoom: Zoom level of the tiles
            format: Image format of the tiles (e.g., 'png', 'jpg')
        """
        if not os.path.exists(directory):
            logger.warning(f"Directory not found: {directory}")
            return
        
        # Get all x directories
        x_dirs = [d for d in os.listdir(directory) 
                 if os.path.isdir(os.path.join(directory, d)) and d.isdigit()]
        
        for x_dir in x_dirs:
            x = int(x_dir)
            x_path = os.path.join(directory, x_dir)
            
            # Get all y files
            y_files = [f for f in os.listdir(x_path) 
                      if os.path.isfile(os.path.join(x_path, f)) and 
                      f.endswith(f'.{format}')]
            
            for y_file in y_files:
                y = int(os.path.splitext(y_file)[0])
                tile_path = os.path.join(x_path, y_file)
                
                try:
                    with open(tile_path, 'rb') as f:
                        tile_data = f.read()
                    
                    self.add_tile(zoom, x, y, tile_data)
                    logger.debug(f"Added tile from {tile_path}")
                except Exception as e:
                    logger.error(f"Error adding tile from {tile_path}: {e}")
                    continue
        
        self.connection.commit()
        logger.info(f"Added all tiles from {directory} to MBTiles")
    
    def optimize(self):
        """Optimize the MBTiles file by vacuuming the database."""
        if not self.connection:
            raise RuntimeError("MBTiles file not open. Call create_mbtiles() first.")
        
        try:
            logger.info("Optimizing MBTiles file...")
            self.cursor.execute("VACUUM")
            self.connection.commit()
            logger.info("Optimization complete")
        except sqlite3.Error as e:
            logger.error(f"Error optimizing MBTiles file: {e}")
            raise
    
    def close(self):
        """Close the MBTiles file."""
        if self.connection:
            self.connection.commit()
            self.connection.close()
            self.connection = None
            self.cursor = None
            logger.info(f"Closed MBTiles file: {self.output_path}")
    
    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'MBTilesGenerator':
        """Create an MBTilesGenerator from a configuration dictionary."""
        output_path = config.get('output_path', 'output/tiles.mbtiles')
        return cls(output_path, config)
