"""
OwnLens - ML Module: Logging

Logging utilities for ML module.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Setup logging for ML module.
    
    Args:
        level: Logging level
        format_string: Custom format string
    
    Returns:
        Logger instance
    """
    if format_string is None:
        format_string = (
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger('ownlens.ml')
    logger.setLevel(level)
    
    return logger

