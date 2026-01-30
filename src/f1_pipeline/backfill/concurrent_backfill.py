import argparse
import logging
import sys
from datetime import datetime
from typing import List

# INTERNAL IMPORT
try:
    from src.f1_pipeline.ingestion.ingestor import F1DataIngestor
except ImportError:
    from f1_pipeline.ingestion.ingestor import F1DataIngestor
    
# Configure Logging for the Backfill Script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("F1Backfill")

def generate_season_list(start_year: int, end_year: int) -> List[int]:
    """Generates a list of seasons (years) to process."""
    return list(range(start_year, end_year + 1))

def main():
    parser = argparse.ArgumentParser(description="Run concurrent backfill for F1 Ergast API.")
    
    parser.add_argument(
        "--start_season", 
        type=int, 
        required=True, 
        help="Starting season year (e.g., 1950)"
    )
    parser.add_argument(
        "--end_season", 
        type=int, 
        required=True, 
        help="Ending season year (e.g., 2023)"
    )
    parser.add_argument(
        "--force", 
        action="store_true", 
        help="Force refresh data even if it exists in MinIO"
    )
    parser.add_argument(
        "--validate", 
        action="store_true", 
        default=True,
        help="Validate MinIO connection on startup"
    )

    args = parser.parse_args()

    # validate inputs
    current_year = datetime.now().year
    if args.start_season < 1950 or args.end_season > current_year:
        logger.error(f"Invalid season range. F1 data exists from 1950 to {current_year}.")
        sys.exit(1)

    if args.start_season > args.end_season:
        logger.error("Start season cannot be after end season.")
        sys.exit(1)

    # Initialize Ingestor
    try:
        ingestor = F1DataIngestor(validate_connection=args.validate)
    except Exception as e:
        logger.critical(f"Failed to initialize ingestor: {e}")
        sys.exit(1)

    seasons_to_process = generate_season_list(args.start_season, args.end_season)
    
    logger.info(f"{'='*50}")
    logger.info(f"Starting Backfill: {args.start_season} to {args.end_season}")
    logger.info(f"Total Seasons: {len(seasons_to_process)}")
    logger.info(f"Force Refresh: {args.force}")
    logger.info(f"{'='*50}")

    total_start_time = datetime.now()
    failed_seasons = []

    for season in seasons_to_process:
        # Generate a unique batch ID for this specific run
        batch_id = datetime.now().strftime("%Y%m%dT%H%M%S")
        
        logger.info(f"Processing Season: {season} (Batch ID: {batch_id})")
        
        try:
            # Trigger the full extraction logic from your class
            summary = ingestor.run_full_extraction(
                season=season,
                batch_id=batch_id,
                force_refresh=args.force
            )
            
            if summary.get("status") != "SUCCESS":
                logger.warning(f"⚠️ Season {season} completed with status: {summary.get('status')}")
            
        except KeyboardInterrupt:
            logger.warning("Backfill interrupted by user.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Critical failure for Season {season}: {e}")
            failed_seasons.append(season)
            # We continue to the next season instead of crashing the whole backfill
            continue

    total_duration = (datetime.now() - total_start_time).total_seconds()
    
    logger.info(f"\n{'='*50}")
    logger.info("BACKFILL COMPLETE")
    logger.info(f"Total Duration: {total_duration:.2f}s")
    
    if failed_seasons:
        logger.error(f"The following seasons failed: {failed_seasons}")
        sys.exit(1)
    else:
        logger.info("All seasons processed successfully.")

if __name__ == "__main__":
    main()