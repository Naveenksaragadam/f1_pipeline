import logging
from f1_data.ingestion.ingestor import F1DataIngestor

# Setup logging to see the output
logging.basicConfig(level=logging.INFO)

def main():
    print("🏎️  Starting Test Ingestion...")
    
    # Initialize the Ingestor
    ingestor = F1DataIngestor()

    # Test 1: Ingest Season-Level Data (Constructors) for 2024
    print("\n--- Testing Season Data (Constructors) ---")
    ingestor.ingest_endpoint("constructors", batch_id="test_batch_001", season=2024)

    # Test 2: Ingest Race-Level Data (Results) for Round 1 of 2024
    print("\n--- Testing Race Data (Results, Round 1) ---")
    ingestor.ingest_endpoint("results", batch_id="test_batch_001", season=2024, round_num="1")

    print("\n✅ Test Complete! Check MinIO Bronze bucket.")

if __name__ == "__main__":
    main()