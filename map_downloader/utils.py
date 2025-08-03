""
Utility functions for the map tile downloader.
"""
import os
import shutil
import logging
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

def cleanup_old_files(directory: str, days: int = 7, exclude: Optional[List[str]] = None) -> int:
    """Remove files older than the specified number of days.
    
    Args:
        directory: Directory to clean up
        days: Number of days to keep files
        exclude: List of file patterns to exclude from deletion
        
    Returns:
        Number of files removed
    """
    if exclude is None:
        exclude = []
    
    if not os.path.exists(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return 0
    
    cutoff_time = datetime.now() - timedelta(days=days)
    removed_count = 0
    
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            # Skip excluded files
            if any(name.endswith(ext) for ext in exclude):
                continue
                
            file_path = os.path.join(root, name)
            try:
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_mtime < cutoff_time:
                    os.remove(file_path)
                    removed_count += 1
                    logger.debug(f"Removed old file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing file {file_path}: {e}")
        
        # Remove empty directories
        for name in dirs:
            dir_path = os.path.join(root, name)
            try:
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    logger.debug(f"Removed empty directory: {dir_path}")
            except Exception as e:
                logger.error(f"Error removing directory {dir_path}: {e}")
    
    logger.info(f"Cleaned up {removed_count} files older than {days} days from {directory}")
    return removed_count

def validate_config(config_path: str) -> Tuple[bool, List[str]]:
    """Validate the configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Check if file exists
    if not os.path.exists(config_path):
        return False, [f"Configuration file not found: {config_path}"]
    
    # Try to load YAML
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return False, [f"Invalid YAML in config file: {e}"]
    
    # Check required top-level sections
    required_sections = ['global', 'sources', 'output']
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")
    
    # Check global settings
    if 'global' in config:
        global_cfg = config['global']
        required_global = ['temp_download_dir']
        for key in required_global:
            if key not in global_cfg:
                errors.append(f"Missing required global setting: {key}")
    
    # Check sources
    if 'sources' in config and not isinstance(config['sources'], list):
        errors.append("'sources' must be a list")
    elif 'sources' in config:
        for i, source in enumerate(config['sources']):
            if not isinstance(source, dict):
                errors.append(f"Source {i} is not a valid configuration object")
                continue
                
            required_source = ['name', 'type', 'url_template', 'zoom_levels', 'bounds']
            for key in required_source:
                if key not in source:
                    errors.append(f"Source {i} missing required field: {key}")
            
            # Check bounds
            if 'bounds' in source and isinstance(source['bounds'], dict):
                required_bounds = ['min_lat', 'min_lon', 'max_lat', 'max_lon']
                for bound in required_bounds:
                    if bound not in source['bounds']:
                        errors.append(f"Source {i} bounds missing required field: {bound}")
    
    # Check output destinations
    if 'output' in config and 'destinations' in config['output']:
        if not isinstance(config['output']['destinations'], list):
            errors.append("'output.destinations' must be a list")
        else:
            for i, dest in enumerate(config['output']['destinations']):
                if not isinstance(dest, dict) or 'type' not in dest:
                    errors.append(f"Destination {i} is missing required 'type' field")
                    continue
                
                # Check required fields based on type
                if dest['type'] == 'local' and 'path' not in dest:
                    errors.append(f"Local destination {i} is missing required 'path' field")
                elif dest['type'] == 'minio':
                    required_minio = ['endpoint', 'access_key', 'secret_key', 'bucket_name']
                    for key in required_minio:
                        if key not in dest:
                            errors.append(f"MinIO destination {i} is missing required field: {key}")
    
    return len(errors) == 0, errors

def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None) -> None:
    """Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file. If None, logs only to console.
    """
    log_level = getattr(logging, log_level.upper(), logging.INFO)
    
    handlers = [logging.StreamHandler()]
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def get_temp_dir(base_dir: str, create: bool = True) -> str:
    """Get a temporary directory path and optionally create it.
    
    Args:
        base_dir: Base directory for temporary files
        create: Whether to create the directory if it doesn't exist
        
    Returns:
        Path to the temporary directory
    """
    temp_dir = os.path.join(base_dir, 'temp')
    if create:
        os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

def get_output_filename(source_name: str, format: str = 'mbtiles') -> str:
    """Generate an output filename based on source name and format.
    
    Args:
        source_name: Name of the source
        format: Output format (e.g., 'mbtiles', 'zip')
        
    Returns:
        Generated filename
    """
    from slugify import slugify
    
    # Create a slug from the source name
    slug = slugify(source_name, lowercase=True)
    
    # Add timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    return f"{slug}_{timestamp}.{format}"
