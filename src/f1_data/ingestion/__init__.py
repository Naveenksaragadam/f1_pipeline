#src/f1_data/ingestion/__init__.py
"""
F1 Data Ingestion Module

Provides production-grade data ingestion capabilities for Formula 1 data
from the Ergast API via Jolpica.

Main Components:
- F1DataIngestor: Core ingestion engine with retry logic and rate limiting
- HTTP client: Session management with automatic retries
- Configuration: Environment-based settings with validation
"""
import logging
from .ingestor import F1DataIngestor

logger = logging.getLogger(__name__)

__all__ = ['F1DataIngestor']