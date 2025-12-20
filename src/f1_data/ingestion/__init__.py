import logging
from .ingestor import F1DataIngestor

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Quick test to see if it works
    ingestor = F1DataIngestor()
    logger.info(f"\nSession created: {ingestor.session}")
    season = int(input("\nenter the Season: "))

    ingestor.run_full_extraction(season=season, batch_id='Backfill',force_refresh=True)