"""
Download strategy implementations for the map tile downloader.
"""
import time
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

class DownloadStrategy(ABC):
    """Base class for all download strategies."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._initialize()
    
    def _initialize(self):
        """Initialize the strategy with configuration."""
        pass
    
    @abstractmethod
    def before_download(self):
        """Called before each download."""
        pass
    
    @abstractmethod
    def after_download(self, success: bool):
        """Called after each download."""
        pass


class RateLimitStrategy(DownloadStrategy):
    """Rate limiting download strategy.
    
    Limits the number of requests per second to avoid hitting rate limits.
    """
    
    def _initialize(self):
        self.requests_per_second = self.config.get('requests_per_second', 5)
        self.min_interval = 1.0 / self.requests_per_second
        self.last_request_time = 0
        logger.info(f"Initialized RateLimitStrategy with {self.requests_per_second} requests per second")
    
    def before_download(self):
        """Ensure we don't exceed the rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def after_download(self, success: bool):
        """No action needed after download for rate limiting."""
        pass


class TimeBasedStrategy(DownloadStrategy):
    """Time-based download strategy.
    
    Downloads in batches with pauses in between to avoid detection.
    """
    
    def _initialize(self):
        self.run_minutes = self.config.get('run_minutes', 5)
        self.pause_minutes = self.config.get('pause_minutes', 1)
        self.batch_size = self.config.get('batch_size', 100)
        self.download_count = 0
        self.batch_start_time = time.time()
        logger.info(f"Initialized TimeBasedStrategy: {self.run_minutes} min run, "
                   f"{self.pause_minutes} min pause, batch size {self.batch_size}")
    
    def before_download(self):
        """Check if we need to take a break between batches."""
        current_time = time.time()
        elapsed = current_time - self.batch_start_time
        
        # Check if we've exceeded the run time
        if elapsed > (self.run_minutes * 60):
            logger.info(f"Batch limit reached. Pausing for {self.pause_minutes} minutes...")
            time.sleep(self.pause_minutes * 60)
            self.batch_start_time = time.time()
            self.download_count = 0
        
        # Check if we've reached the batch size
        if self.download_count >= self.batch_size:
            logger.info(f"Batch size of {self.batch_size} reached. Starting new batch...")
            self.download_count = 0
            self.batch_start_time = time.time()
    
    def after_download(self, success: bool):
        """Increment the download counter."""
        self.download_count += 1


class ExponentialBackoffStrategy(DownloadStrategy):
    """Exponential backoff strategy for handling failures.
    
    Increases delay between retries exponentially when failures occur.
    """
    
    def _initialize(self):
        self.base_delay = self.config.get('base_delay', 1.0)  # seconds
        self.max_delay = self.config.get('max_delay', 60.0)   # seconds
        self.factor = self.config.get('factor', 2.0)
        self.current_delay = self.base_delay
        logger.info(f"Initialized ExponentialBackoffStrategy with base_delay={self.base_delay}, "
                   f"max_delay={self.max_delay}, factor={self.factor}")
    
    def before_download(self):
        """No action needed before download."""
        pass
    
    def after_download(self, success: bool):
        """Adjust delay based on download success/failure."""
        if success:
            self.current_delay = self.base_delay
        else:
            self.current_delay = min(self.current_delay * self.factor, self.max_delay)
            logger.warning(f"Download failed. Next retry in {self.current_delay:.1f} seconds")
            time.sleep(self.current_delay)


def create_strategy(strategy_config: Dict[str, Any]) -> DownloadStrategy:
    """Factory function to create the appropriate download strategy."""
    strategy_type = strategy_config.get('type', '').lower()
    
    if strategy_type == 'rate_limit':
        return RateLimitStrategy(strategy_config)
    elif strategy_type == 'time_based':
        return TimeBasedStrategy(strategy_config)
    elif strategy_type == 'exponential_backoff':
        return ExponentialBackoffStrategy(strategy_config)
    else:
        logger.warning(f"Unknown strategy type: {strategy_type}. Using default RateLimitStrategy")
        return RateLimitStrategy({'requests_per_second': 5})
