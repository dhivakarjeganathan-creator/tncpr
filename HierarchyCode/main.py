"""
Main application for the Hierarchy Management System
"""
import logging
import os
import sys
from pathlib import Path
from models import create_tables, create_database_if_not_exists, SessionLocal
from crud_operations import CRUDOperations
from csv_processor import CSVProcessor
from config import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hierarchy_system.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class HierarchyManager:
    """Main application class for managing the hierarchy system"""
    
    def __init__(self):
        self.db = None
        self.crud = None
        self.csv_processor = CSVProcessor()
    
    def initialize_database(self):
        """Initialize database and create tables"""
        try:
            logger.info("Initializing database...")
            # First, ensure the database exists
            if not create_database_if_not_exists():
                logger.error("Failed to create database")
                return False
            # Then create tables
            create_tables()
            logger.info("Database initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            return False
    
    def get_db_session(self):
        """Get database session"""
        if not self.db:
            self.db = SessionLocal()
            self.crud = CRUDOperations(self.db)
        return self.db, self.crud
    
    def close_db_session(self):
        """Close database session"""
        if self.db:
            self.db.close()
            self.db = None
            self.crud = None
    
    def create_csv_folder(self):
        """Create CSV folder if it doesn't exist"""
        csv_folder = Path(config.CSV_FOLDER_PATH)
        csv_folder.mkdir(exist_ok=True)
        logger.info(f"CSV folder ready: {csv_folder}")
    
    def process_csv_files(self, folder_path: str = None):
        """Process all CSV files in the specified folder"""
        try:
            logger.info("Starting CSV file processing...")
            results = self.csv_processor.process_csv_folder(folder_path)
            
            successful = len([r for r in results if r['status'] == 'SUCCESS'])
            failed = len([r for r in results if r['status'] == 'ERROR'])
            skipped = len([r for r in results if r['status'] == 'SKIPPED'])
            
            logger.info(f"Processing complete: {successful} successful, {failed} failed, {skipped} skipped")
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing CSV files: {e}")
            return []
    
    def get_hierarchy_summary(self):
        """Get summary of the current hierarchy"""
        try:
            db, crud = self.get_db_session()
            
            summary = {
                'regions': len(crud.get_all_regions()),
                'markets': 0,
                'gnbs': 0,
                'dus': 0,
                'sectors': 0,
                'carriers': 0
            }
            
            # Count all entities
            for region in crud.get_all_regions():
                markets = crud.get_markets_by_region(region.id)
                summary['markets'] += len(markets)
                
                for market in markets:
                    gnbs = crud.get_gnbs_by_market(market.id)
                    summary['gnbs'] += len(gnbs)
                    
                    for gnb in gnbs:
                        dus = crud.get_dus_by_gnb(gnb.id)
                        summary['dus'] += len(dus)
                        
                        sectors = crud.get_sectors_by_gnb(gnb.id)
                        summary['sectors'] += len(sectors)
                        
                        for du in dus:
                            du_sectors = crud.get_sectors_by_du(du.id)
                            summary['sectors'] += len(du_sectors)
                            
                            for sector in du_sectors:
                                carriers = crud.get_carriers_by_sector(sector.id)
                                summary['carriers'] += len(carriers)
                        
                        for sector in sectors:
                            carriers = crud.get_carriers_by_sector(sector.id)
                            summary['carriers'] += len(carriers)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting hierarchy summary: {e}")
            return {}
        finally:
            self.close_db_session()
    
    def get_processing_summary(self):
        """Get summary of CSV processing"""
        return self.csv_processor.get_processing_summary()
    
    def display_hierarchy(self):
        """Display the complete hierarchy"""
        try:
            db, crud = self.get_db_session()
            
            print("\n" + "="*80)
            print("HIERARCHY OVERVIEW")
            print("="*80)
            
            regions = crud.get_all_regions()
            for region in regions:
                print(f"\nRegion: {region.name} (ID: {region.id})")
                
                markets = crud.get_markets_by_region(region.id)
                for market in markets:
                    print(f"  └── Market: {market.name} (ID: {market.id})")
                    
                    gnbs = crud.get_gnbs_by_market(market.id)
                    for gnb in gnbs:
                        print(f"      └── GNB: {gnb.name} (ID: {gnb.id})")
                        
                        # DUs under GNB
                        dus = crud.get_dus_by_gnb(gnb.id)
                        for du in dus:
                            print(f"          └── DU: {du.name} (ID: {du.id})")
                            
                            # Sectors under DU
                            du_sectors = crud.get_sectors_by_du(du.id)
                            for sector in du_sectors:
                                print(f"              └── Sector: {sector.name} (ID: {sector.id})")
                                
                                carriers = crud.get_carriers_by_sector(sector.id)
                                for carrier in carriers:
                                    print(f"                  └── Carrier: {carrier.name} (ID: {carrier.id})")
                        
                        # Sectors directly under GNB
                        gnb_sectors = crud.get_sectors_by_gnb(gnb.id)
                        for sector in gnb_sectors:
                            print(f"          └── Sector: {sector.name} (ID: {sector.id}) [Direct to GNB]")
                            
                            carriers = crud.get_carriers_by_sector(sector.id)
                            for carrier in carriers:
                                print(f"              └── Carrier: {carrier.name} (ID: {carrier.id})")
            
            print("\n" + "="*80)
            
        except Exception as e:
            logger.error(f"Error displaying hierarchy: {e}")
        finally:
            self.close_db_session()

def main():
    """Main application entry point"""
    print("Hierarchy Management System")
    print("=" * 50)
    
    # Initialize the system
    manager = HierarchyManager()
    
    # Initialize database
    if not manager.initialize_database():
        print("Failed to initialize database. Please check your configuration.")
        return
    
    # Create CSV folder
    manager.create_csv_folder()
    
    while True:
        print("\nOptions:")
        print("1. Process CSV files")
        print("2. Display hierarchy")
        print("3. Show hierarchy summary")
        print("4. Show processing summary")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            folder_path = input(f"Enter CSV folder path (or press Enter for default: {config.CSV_FOLDER_PATH}): ").strip()
            if not folder_path:
                folder_path = None
            
            results = manager.process_csv_files(folder_path)
            
            print(f"\nProcessing Results:")
            for result in results:
                status = result['status']
                if status == 'SUCCESS':
                    records = result.get('records_added', {})
                    print(f"✓ {result.get('file_name', 'Unknown')}: {records}")
                elif status == 'ERROR':
                    print(f"✗ {result.get('file_name', 'Unknown')}: {result.get('error', 'Unknown error')}")
                elif status == 'SKIPPED':
                    print(f"- {result.get('file_name', 'Unknown')}: Already processed")
        
        elif choice == '2':
            manager.display_hierarchy()
        
        elif choice == '3':
            summary = manager.get_hierarchy_summary()
            print(f"\nHierarchy Summary:")
            for entity, count in summary.items():
                print(f"  {entity.capitalize()}: {count}")
        
        elif choice == '4':
            summary = manager.get_processing_summary()
            print(f"\nProcessing Summary:")
            print(f"  Total files processed: {summary['total_files_processed']}")
            print(f"  Successful: {summary['successful_files']}")
            print(f"  Failed: {summary['failed_files']}")
            print(f"\nTotal records added:")
            for entity, count in summary['total_records_added'].items():
                print(f"  {entity.capitalize()}: {count}")
        
        elif choice == '5':
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
