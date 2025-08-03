"""
IP Pool management for rotating IP addresses and handling proxies.
"""
import logging
import random
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class IPAddress:
    """Represents an IP address in the pool."""
    address: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    last_used: float = 0.0
    failures: int = 0
    is_active: bool = True
    
    @property
    def proxy_url(self) -> str:
        """Get the proxy URL for this IP address."""
        if self.username and self.password:
            return f"http://{self.username}:{self.password}@{self.address}:{self.port}"
        return f"http://{self.address}:{self.port}"


class IPPool:
    """Manages a pool of IP addresses for rotation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the IP pool.
        
        Args:
            config: Configuration dictionary with IP pool settings
        """
        self.addresses: List[IPAddress] = []
        self.config = config
        self._initialize_pool()
        self.rotation_interval = config.get('rotation_interval', 60)  # seconds
        self.max_failures = config.get('max_failures', 3)
        self.last_rotation = 0.0
        logger.info(f"Initialized IP pool with {len(self.addresses)} addresses")
    
    def _initialize_pool(self):
        """Initialize the IP pool from configuration."""
        # This would typically load IPs from a file, database, or API
        # For now, we'll use a placeholder
        if self.config.get('enabled', False):
            logger.warning("Custom IP pool configuration not implemented. Using default settings.")
            # In a real implementation, you would load IPs from config
            # Example:
            # self.addresses.append(IPAddress("proxy1.example.com", 8080, "user", "pass"))
    
    def add_address(self, address: str, port: int, username: Optional[str] = None, 
                   password: Optional[str] = None):
        """Add a new IP address to the pool."""
        self.addresses.append(IPAddress(
            address=address,
            port=port,
            username=username,
            password=password
        ))
        logger.debug(f"Added new IP address: {address}:{port}")
    
    def get_next_address(self) -> Optional[IPAddress]:
        """Get the next available IP address, considering rotation and failures."""
        if not self.addresses:
            logger.warning("No IP addresses available in the pool")
            return None
        
        # Filter out inactive addresses
        active_addrs = [ip for ip in self.addresses if ip.is_active]
        if not active_addrs:
            logger.error("No active IP addresses available")
            return None
        
        # Sort by last_used to implement round-robin
        active_addrs.sort(key=lambda x: x.last_used)
        
        # Check if we need to rotate due to time
        current_time = time.time()
        if (current_time - self.last_rotation) > self.rotation_interval:
            self.last_rotation = current_time
            logger.info("Rotating IP addresses due to rotation interval")
            # Move the first address to the end
            active_addrs = active_addrs[1:] + active_addrs[:1]
        
        # Get the next available address
        selected = active_addrs[0]
        selected.last_used = current_time
        
        return selected
    
    def mark_failure(self, ip_address: IPAddress):
        """Mark an IP address as failed."""
        ip_address.failures += 1
        logger.warning(f"Marked IP {ip_address.address} as failed. Failures: {ip_address.failures}")
        
        if ip_address.failures >= self.max_failures:
            ip_address.is_active = False
            logger.error(f"IP {ip_address.address} disabled after {self.max_failures} failures")
    
    def mark_success(self, ip_address: IPAddress):
        """Mark an IP address as successful."""
        if ip_address.failures > 0:
            ip_address.failures = 0
            logger.info(f"IP {ip_address.address} marked as successful, resetting failure count")
    
    def get_proxy_dict(self, ip_address: IPAddress) -> Dict[str, str]:
        """Get a requests-compatible proxy dictionary for the given IP address."""
        return {
            'http': ip_address.proxy_url,
            'https': ip_address.proxy_url
        }
    
    def get_random_address(self) -> Optional[IPAddress]:
        """Get a random active IP address from the pool."""
        active_addrs = [ip for ip in self.addresses if ip.is_active]
        if not active_addrs:
            return None
        return random.choice(active_addrs)
    
    def get_least_used_address(self) -> Optional[IPAddress]:
        """Get the least recently used active IP address."""
        active_addrs = [ip for ip in self.addresses if ip.is_active]
        if not active_addrs:
            return None
        return min(active_addrs, key=lambda x: x.last_used)
    
    def rotate(self):
        """Manually trigger IP rotation. """
        self.last_rotation = 0  # Force rotation on next get_next_address call
        logger.info("Manual IP rotation triggered")
