# src/F1_PPIPELINE/ingestion/ingestor.py
import json
from pathlib import Path
from typing import Dict

from .config import BASE_URL, DEFAULT_LIMIT
from .http_client import create_session


class F1DataIngestor:
    def __init__(self, base_url: str = BASE_URL, session=None) -> None:
        self.base_url = base_url
        self.session = session or create_session()
    
    def _get_output_path(self, season: int, page: int) -> Path:
        """Helper to determine where to save the file."""
        project_root = Path(__file__).resolve().parents[1]
        save_dir = (
            project_root
            / "data"
            / "raw"
            / f"season={season}"
        )
        save_dir.mkdir(parents=True,exist_ok=True)
        return save_dir / f"page_{page:03}.json"
    
    def _save_page(self, data: dict, season: int, page: int ) -> None: # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        """Writes the JSON data to disk."""
        file_path = self._get_output_path(season, page)
        print(f"Saving page number {page} to disk....")
        try: 
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
                print(f"Saved: {file_path}")
        except IOError as e:
            print(f"Error saving file {file_path}: {e}")
            raise
        
    
    def fetch_page(self, season: int, limit: int = 30, offset: int = 0) -> dict: # type: ignore
        """Fetches a single page using the session and params dict."""
        endpoint = f"{self.base_url}/{season}/results.json"
        
        # TODO: Define params dict
        params = {
            "limit":limit,
            "offset":offset
        }
        try:
            response = self.session.get(endpoint,params=params)
            response.raise_for_status()
            print("API response recieved sucessfully!!!")
        except requests.exceptions.RequestException as e:
            print(f"An error occured: {e}")
            raise
        return response.json()
        
    def ingest_season(self, season: int) -> None:
        """Orchestrates the loop."""
        print(f"Starting ingestion for Season {season}...")
        
        # 1. Fetch first page to get metadata (Total/Pages)
        response = self.fetch_page(season, offset=0)
        self._save_page(response,season,1) # Save Page 1
        
        # 2. Calculate total pages
        limit = 30
        total_results = int(response['MRData']['total'])
        total_pages = (total_results + limit - 1) // limit # handiling edge cases for total_pages

        print(f"Total pages to ingest: {total_pages}")

        
        
        # 3. Loop through remaining pages
        # We handle offset calculation dynamically: (page - 1) * 30
        for page in range(2,total_pages+1):
            offset = (page - 1) * limit
            print(f"Fetching page {page} (Offset {offset})...")

            response = self.fetch_page(season,30,offset)
            self._save_page(response,season,page)

if __name__ == "__main__":

    # Quick test to see if it works
    ingestor = F1DataIngestor()
    print(f"\nSession created: {ingestor.session}")
    season = int(input("\nenter the Season: "))
    ingestor.ingest_season(season)
    