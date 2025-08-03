"""
Configuration loader for the map tile downloader.
Handles loading and validating the YAML configuration.
"""
import os
import yaml
from typing import Dict, Any, Optional
from dataclass   es import dataclass, field
from typing import List

@dataclass
class DownloadStrategyConfig:
    name: str
    type: str
    params: Dict[str, Any] = field(default_factory=dict)

@dataclass
class IPPoolConfig:
    enabled: bool = False
    provider: str = ""
    credentials: Dict[str, str] = field(default_factory=dict)
    rotation_interval: int = 60
    max_failures: int = 3

@dataclass
class Bounds:
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

@dataclass
class SourceConfig:
    name: str
    type: str
    url_template: str
    headers: Dict[str, str]
    zoom_levels: List[int]
    bounds: Bounds

@dataclass
class OutputDestination:
    type: str  # 'local' or 'minio'
    path: str = ""
    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""
    bucket_name: str = ""
    secure: bool = True

@dataclass
class MBTilesConfig:
    name: str
    description: str
    attribution: str
    version: str
    format: str
    min_zoom: int
    max_zoom: int
    bounds: str
    type: str

@dataclass
class Config:
    log_level: str
    temp_download_dir: str
    cleanup_temp_files: bool
    cleanup_after_days: int
    max_retries: int
    retry_delay: int
    download_strategies: List[DownloadStrategyConfig]
    ip_pool: IPPoolConfig
    sources: List[SourceConfig]
    output_format: str
    compress: bool
    destinations: List[OutputDestination]
    mbtiles: Optional[MBTilesConfig] = None

    @classmethod
    def from_yaml(cls, config_path: str) -> 'Config':
        """Load configuration from a YAML file."""
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Create output directories if they don't exist
        os.makedirs(config_data['global']['temp_download_dir'], exist_ok=True)
        
        # Parse download strategies
        strategies = [
            DownloadStrategyConfig(
                name=s['name'],
                type=s['type'],
                params={k: v for k, v in s.items() if k not in ['name', 'type']}
            )
            for s in config_data.get('download_strategies', [])
        ]
        
        # Parse IP pool config
        ip_pool_data = config_data.get('ip_pool', {})
        ip_pool = IPPoolConfig(
            enabled=ip_pool_data.get('enabled', False),
            provider=ip_pool_data.get('provider', ''),
            credentials=ip_pool_data.get('credentials', {}),
            rotation_interval=ip_pool_data.get('rotation_interval', 60),
            max_failures=ip_pool_data.get('max_failures', 3)
        )
        
        # Parse sources
        sources = []
        for source in config_data.get('sources', []):
            bounds = source.get('bounds', {})
            sources.append(SourceConfig(
                name=source['name'],
                type=source['type'],
                url_template=source['url_template'],
                headers=source.get('headers', {}),
                zoom_levels=source.get('zoom_levels', []),
                bounds=Bounds(
                    min_lat=bounds.get('min_lat', 0),
                    min_lon=bounds.get('min_lon', 0),
                    max_lat=bounds.get('max_lat', 0),
                    max_lon=bounds.get('max_lon', 0)
                )
            ))
        
        # Parse output destinations
        destinations = []
        for dest in config_data.get('output', {}).get('destinations', []):
            destinations.append(OutputDestination(
                type=dest['type'],
                path=dest.get('path', ''),
                endpoint=dest.get('endpoint', ''),
                access_key=dest.get('access_key', ''),
                secret_key=dest.get('secret_key', ''),
                bucket_name=dest.get('bucket_name', ''),
                secure=dest.get('secure', True)
            ))
            
            # Create local output directory if it doesn't exist
            if dest['type'] == 'local' and 'path' in dest:
                os.makedirs(dest['path'], exist_ok=True)
        
        # Parse MBTiles config if needed
        mbtiles_data = config_data.get('mbtiles')
        mbtiles = None
        if mbtiles_data:
            mbtiles = MBTilesConfig(
                name=mbtiles_data.get('name', 'map_tiles'),
                description=mbtiles_data.get('description', ''),
                attribution=mbtiles_data.get('attribution', ''),
                version=mbtiles_data.get('version', '1.0'),
                format=mbtiles_data.get('format', 'png'),
                min_zoom=mbtiles_data.get('min_zoom', 0),
                max_zoom=mbtiles_data.get('max_zoom', 22),
                bounds=mbtiles_data.get('bounds', '-180.0,-85.0511,180.0,85.0511'),
                type=mbtiles_data.get('type', 'baselayer')
            )
        
        return cls(
            log_level=config_data['global'].get('log_level', 'INFO'),
            temp_download_dir=config_data['global'].get('temp_download_dir', '/tmp/map_downloader'),
            cleanup_temp_files=config_data['global'].get('cleanup_temp_files', True),
            cleanup_after_days=config_data['global'].get('cleanup_after_days', 7),
            max_retries=config_data['global'].get('max_retries', 3),
            retry_delay=config_data['global'].get('retry_delay', 5),
            download_strategies=strategies,
            ip_pool=ip_pool,
            sources=sources,
            output_format=config_data.get('output', {}).get('format', 'files'),
            compress=config_data.get('output', {}).get('compress', False),
            destinations=destinations,
            mbtiles=mbtiles
        )
