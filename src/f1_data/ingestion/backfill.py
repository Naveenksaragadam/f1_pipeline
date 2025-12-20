# src/backfill.py
import logging
from datetime import datetime
from f1_data.ingestion.ingestor import F1DataIngestor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_manual_backfill():
    ingestor = F1DataIngestor()
    
    # SEMANTIC BATCH ID (human-readable)
    batch_id = "backfill_historical_v1"
    
    # Backfill range
    start_year = 1950
    end_year = 2023  # Exclude current season
    current_year = datetime.now().year
    
    for season in range(start_year, end_year + 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"Season {season} | Batch: {batch_id}")
        logger.info(f"{'='*60}\n")
        
        # Historical data: Skip if exists (idempotent)
        force_refresh = (season == current_year)
        
        try:
            ingestor.run_full_extraction(
                season=season,
                batch_id=batch_id,
                force_refresh=force_refresh
            )
        except Exception as e:
            logger.error(f"❌ Failed season {season}: {e}")
            # Continue to next season (don't fail entire backfill)
            continue
    
    logger.info(f"\n✅ Backfill Complete!")

if __name__ == "__main__":
    run_manual_backfill()