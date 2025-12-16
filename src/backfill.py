import logging
from f1_data.ingestion.ingestor import F1DataIngestor

# Setup logging to see what's happening
logging.basicConfig(level=logging.INFO)

def run_backfill():
    ingestor = F1DataIngestor()
    
    # HARDCODED BATCH ID ensures idempotency (skips if already exists)
    FIXED_BATCH_ID = "backfill_2023_v1" 
    
    # -----------------------------------------
    # TEST CONFIGURATION: JUST ONE SEASON
    # -----------------------------------------
    season_to_test = 2023
    
    print(f"🏎️  Starting Backfill for Season {season_to_test}...")
    print(f"📦 Batch ID: {FIXED_BATCH_ID}")

    # calling the run_full_extraction, but we need to ensure it uses our FIXED_BATCH_ID
    # Since run_full_extraction generates its own ID, we should manually call the parts 
    # OR refactor run_full_extraction. 
    
    # FOR THIS TEST: Let's manually call the heavy logic to control the ID.
    
    # 1. Reference Data
    print(f"\n--- Step 1: Reference Data ---")
    for endpoint in ["constructors", "drivers", "races"]:
        ingestor.ingest_endpoint(endpoint, batch_id=FIXED_BATCH_ID, season=season_to_test)

    # 2. Get the Schedule (to know rounds)
    # We fetch it again briefly to get the list of rounds
    schedule_url = f"{ingestor.base_url}/{season_to_test}.json"
    schedule_data = ingestor.fetch_page(schedule_url, limit=100, offset=0)
    races_list = schedule_data["MRData"]["RaceTable"]["Races"]
    
    total_rounds = len(races_list)
    print(f"\n--- Step 2: Found {total_rounds} Rounds. Starting Race Loop... ---")

    # 3. Loop Rounds
    for race in races_list:
        round_num = race["round"]
        print(f"   > Processing Round {round_num}/{total_rounds}...")
        
        # Race Data
        for endpoint in ["results", "qualifying", "laps", "pitstops", "sprint"]:
            ingestor.ingest_endpoint(endpoint, batch_id=FIXED_BATCH_ID, season=season_to_test, round=round_num)
        
        # Standings
        for endpoint in ["driverstandings", "constructorstandings"]:
            ingestor.ingest_endpoint(endpoint, batch_id=FIXED_BATCH_ID, season=season_to_test, round=round_num)

    print(f"\n✅ Backfill for {season_to_test} Complete!")

if __name__ == "__main__":
    run_backfill()