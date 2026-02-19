# src/f1_pipeline/ingestion/backfill.py
"""
Manual backfill script for historical F1 data ingestion.

This script handles bulk historical data extraction with proper error handling
and recovery mechanisms. Use this for initial data loading or re-processing.
"""

import logging
import sys
from datetime import datetime
from typing import List, Dict, Any
from f1_pipeline.ingestion.ingestor import F1DataIngestor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_manual_backfill(
    start_year: int = 1950,
    end_year: int | None = None,
    batch_id: str | None = None,
    skip_on_error: bool = True,
) -> Dict[str, Any]:
    """
    Execute manual backfill for historical seasons.

    Args:
        start_year: First season to process (inclusive)
        end_year: Last season to process (inclusive). Defaults to previous year.
        batch_id: Optional custom batch identifier. Defaults to timestamp-based ID.
        skip_on_error: If True, continue processing remaining seasons on error

    Returns:
        Dictionary containing backfill summary statistics
    """
    # Determine end year (exclude current season by default)
    current_year = datetime.now().year
    if end_year is None:
        end_year = current_year - 1

    # Validate year range
    if start_year > end_year:
        logger.error(f"❌ Invalid year range: {start_year} > {end_year}")
        sys.exit(1)

    if end_year >= current_year:
        logger.warning(
            f"⚠️  End year ({end_year}) includes current season. "
            f"Consider using force_refresh=True for {current_year}"
        )

    # Generate batch ID if not provided
    if batch_id is None:
        batch_id = f"backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Initialize ingestor
    try:
        ingestor = F1DataIngestor(validate_connection=True)
    except Exception as e:
        logger.error(f"❌ Failed to initialize ingestor: {e}")
        sys.exit(1)

    # Track results
    total_seasons = end_year - start_year + 1
    failed_seasons: List[int] = []
    successful_seasons: List[int] = []

    logger.info(
        f"\n{'=' * 70}\n"
        f"🚀 BACKFILL STARTED\n"
        f"   Year Range: {start_year} → {end_year}\n"
        f"   Total Seasons: {total_seasons}\n"
        f"   Batch ID: {batch_id}\n"
        f"   Skip on Error: {skip_on_error}\n"
        f"{'=' * 70}\n"
    )

    backfill_start = datetime.now()

    # Process each season
    for season in range(start_year, end_year + 1):
        logger.info(f"\n{'=' * 70}")
        logger.info(f"Season {season} ({season - start_year + 1}/{total_seasons})")
        logger.info(f"{'=' * 70}\n")

        # Determine if force refresh is needed
        # Historical data: idempotent (skip existing)
        # Current year: force refresh (data may change)
        force_refresh = season == current_year

        try:
            summary = ingestor.run_full_extraction(
                season=season, batch_id=batch_id, force_refresh=force_refresh
            )

            if summary["status"] == "SUCCESS":
                successful_seasons.append(season)
                logger.info(f"✅ Season {season} completed successfully")
            else:
                logger.warning(f"⚠️  Season {season} completed with errors")
                if not skip_on_error:
                    failed_seasons.append(season)
                    break

        except Exception as e:
            logger.error(f"❌ Season {season} failed: {e}", exc_info=True)
            failed_seasons.append(season)

            if not skip_on_error:
                logger.error("Stopping backfill due to error (skip_on_error=False)")
                break
            else:
                logger.warning("Continuing to next season (skip_on_error=True)")
                continue

    # Generate final summary
    backfill_duration = (datetime.now() - backfill_start).total_seconds()

    summary = {
        "total_seasons": total_seasons,
        "successful_seasons": len(successful_seasons),
        "failed_seasons": len(failed_seasons),
        "failed_season_list": failed_seasons,
        "duration_seconds": round(backfill_duration, 2),
        "batch_id": batch_id,
    }

    logger.info(
        f"\n{'=' * 70}\n"
        f"{'✅ BACKFILL COMPLETE' if not failed_seasons else '⚠️  BACKFILL COMPLETED WITH ERRORS'}\n"
        f"   Total Duration: {backfill_duration:.2f}s\n"
        f"   Successful: {len(successful_seasons)}/{total_seasons}\n"
        f"   Failed: {len(failed_seasons)}/{total_seasons}\n"
    )

    if failed_seasons:
        logger.warning(f"   Failed Seasons: {failed_seasons}\n")
        logger.warning(
            f"   To retry failed seasons, run:\n"
            f"   python -m f1_pipeline.ingestion.backfill --years {','.join(map(str, failed_seasons))}"
        )

    logger.info(f"{'=' * 70}\n")

    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Backfill historical F1 data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill all historical data (1950 to previous year)
  python backfill.py
  
  # Backfill specific year range
  python backfill.py --start 2015 --end 2023
  
  # Backfill with custom batch ID
  python backfill.py --batch-id "initial_load_v1"
  
  # Stop on first error
  python backfill.py --no-skip-on-error
        """,
    )

    parser.add_argument(
        "--start",
        type=int,
        default=1950,
        help="First season to process (default: 1950)",
    )

    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="Last season to process (default: previous year)",
    )

    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Custom batch identifier (default: auto-generated)",
    )

    parser.add_argument(
        "--no-skip-on-error",
        action="store_true",
        help="Stop processing on first error (default: continue)",
    )

    args = parser.parse_args()

    try:
        summary = run_manual_backfill(
            start_year=args.start,
            end_year=args.end,
            batch_id=args.batch_id,
            skip_on_error=not args.no_skip_on_error,
        )

        # Exit with error code if any seasons failed
        if summary["failed_seasons"] > 0:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Backfill interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n❌ Backfill failed: {e}", exc_info=True)
        sys.exit(1)
