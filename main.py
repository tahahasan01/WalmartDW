"""
ETL Orchestration for Walmart Data Warehouse
Implements multi-threaded stream processing with HYBRIDJOIN
"""

import threading
import csv
import time
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
import pyodbc
import logging
from hybrid_join import HybridJoin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """
    Orchestrates ETL process with stream-relation join using HYBRIDJOIN.
    Handles data extraction, transformation, and loading into DW.
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize ETL orchestrator.
        
        Args:
            db_config: Database connection parameters
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self.hybrid_join = HybridJoin(hash_slots=10000, partition_size=500)
        
    def connect_database(self) -> bool:
        """
        Establish database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Check if using Windows Authentication (empty password = Windows auth)
            if not self.db_config['password'] or self.db_config['password'].lower() == 'windows':
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.db_config['server']};"
                    f"DATABASE={self.db_config['database']};"
                    f"Trusted_Connection=yes;"
                )
            else:
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.db_config['server']};"
                    f"DATABASE={self.db_config['database']};"
                    f"UID={self.db_config['username']};"
                    f"PWD={self.db_config['password']}"
                )
            
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def extract_master_data(self) -> Dict[str, List[Dict]]:
        """
        Extract Master Data (Customer & Product dimensions).
        
        Returns:
            Dictionary containing customer and product master data
        """
        logger.info("Extracting Master Data...")
        
        master_data = {
            'customers': [],
            'products': []
        }
        
        try:
            # Extract customer master data
            customer_df = pd.read_csv('customer_master_data.csv', index_col=0)
            master_data['customers'] = customer_df.to_dict('records')
            logger.info(f"Extracted {len(master_data['customers'])} customer records")
            
            # Extract product master data
            product_df = pd.read_csv('product_master_data.csv', index_col=0)
            master_data['products'] = product_df.to_dict('records')
            logger.info(f"Extracted {len(master_data['products'])} product records")
            
        except Exception as e:
            logger.error(f"Error extracting master data: {e}")
        
        return master_data
    
    def stream_transactional_data(self, stop_event: threading.Event) -> None:
        """
        Thread function: Stream transactional data from CSV to stream buffer.
        
        Args:
            stop_event: Threading event to signal completion
        """
        logger.info("Stream Reader Thread: Started")
        
        try:
            records_read = 0
            
            with open('transactional_data/transactional_data.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, skipinitialspace=True)
                
                for row in reader:
                    if stop_event.is_set():
                        break
                    
                    try:
                        # Transform row data
                        transaction = {
                            'orderID': row.get('orderID', ''),
                            'Customer_ID': row.get('Customer_ID', ''),
                            'Product_ID': row.get('Product_ID', ''),
                            'quantity': int(row.get('quantity', 0)),
                            'date': row.get('date', '')
                        }
                        
                        # Add to stream buffer
                        self.hybrid_join.add_stream_tuple(transaction)
                        records_read += 1
                        
                        # Log progress every 50k records
                        if records_read % 50000 == 0:
                            logger.info(f"Stream Reader: Read {records_read} records, Buffer size: {self.hybrid_join.stream_buffer.qsize()}")
                        
                        # Optional: Add throttling for real-time simulation
                        # time.sleep(0.001)  # 1ms per record
                    except ValueError as ve:
                        logger.warning(f"Skipping row due to parse error: {ve}, Row: {row}")
                        continue
            
            logger.info(f"Stream Reader Thread: Completed - {records_read} records read")
            
        except FileNotFoundError:
            logger.error(f"Stream Reader Thread Error: transactional_data.csv not found")
        except Exception as e:
            logger.error(f"Stream Reader Thread Error: {e}", exc_info=True)
        finally:
            # Signal that streaming is complete
            self.hybrid_join.stop_flag = True
    
    def transform_and_load_data(self, master_data: Dict) -> None:
        """
        Thread function: Transform data using HYBRIDJOIN and load to DW.
        
        Args:
            master_data: Master data for enrichment
        """
        logger.info("Transform & Load Thread: Started")
        
        try:
            # Prepare relation data (enriched master data)
            relation_data = []
            
            # Combine customer and product data
            for customer in master_data['customers']:
                for product in master_data['products']:
                    relation_data.append({
                        **customer,
                        **product
                    })
            
            logger.info(f"Relation data prepared: {len(relation_data)} enriched records")
            
            # Execute HYBRIDJOIN
            joined_records = self.hybrid_join.execute(relation_data)
            
            logger.info(f"HYBRIDJOIN completed: {len(joined_records)} records joined")
            
            # Load joined records to DW
            if joined_records:
                self.load_to_dw(joined_records)
            
            logger.info("Transform & Load Thread: Completed")
            
        except Exception as e:
            logger.error(f"Transform & Load Thread Error: {e}")
    
    def load_to_dw(self, joined_records: List[Dict]) -> None:
        """
        Load enriched records to Data Warehouse fact table.
        
        Args:
            joined_records: List of joined (enriched) records
        """
        logger.info(f"Loading {len(joined_records)} records to DW...")
        
        if not self.cursor:
            logger.warning("Database cursor not available")
            return
        
        try:
            loaded_count = 0
            
            for record in joined_records:
                try:
                    # Extract/derive field values
                    order_id = record.get('orderID', '')
                    customer_id = record.get('Customer_ID', '')
                    product_id = record.get('Product_ID', '')
                    quantity = int(record.get('quantity', 0))
                    price = float(record.get('price$', 0)) if 'price$' in record else 0
                    store_id = int(record.get('storeID', 0)) if 'storeID' in record else 0
                    supplier_id = int(record.get('supplierID', 0)) if 'supplierID' in record else 0
                    trans_date = record.get('date', '')
                    total_amount = quantity * price
                    
                    # Get surrogate keys (insert if not exists)
                    customer_sk = self._get_or_insert_customer(customer_id, record)
                    product_sk = self._get_or_insert_product(product_id, record)
                    store_sk = self._get_or_insert_store(store_id, record)
                    supplier_sk = self._get_or_insert_supplier(supplier_id, record)
                    date_sk = self._get_or_insert_date(trans_date)
                    
                    # Insert fact record
                    insert_query = """
                    INSERT INTO SALES_FACT 
                    (OrderID, Customer_SK, Product_SK, Store_SK, Supplier_SK, Date_SK, 
                     Quantity, UnitPrice, TotalAmount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    self.cursor.execute(
                        insert_query,
                        (order_id, customer_sk, product_sk, store_sk, supplier_sk, 
                         date_sk, quantity, price, total_amount)
                    )
                    
                    loaded_count += 1
                    
                except Exception as e:
                    logger.error(f"Error loading record {order_id}: {e}")
            
            # Commit transaction
            self.connection.commit()
            logger.info(f"Successfully loaded {loaded_count} records to DW")
            
        except Exception as e:
            logger.error(f"Error during DW loading: {e}")
            if self.connection:
                self.connection.rollback()
    
    def _get_or_insert_customer(self, customer_id: str, record: Dict) -> int:
        """Get surrogate key for customer, insert if not exists."""
        try:
            self.cursor.execute(
                "SELECT Customer_SK FROM DIM_CUSTOMER WHERE Customer_ID = ?",
                (customer_id,)
            )
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            
            # Insert new customer
            self.cursor.execute("""
                INSERT INTO DIM_CUSTOMER 
                (Customer_ID, Gender, Age_Group, Occupation, City_Category, 
                 Stay_In_Current_City_Years, Marital_Status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                customer_id,
                record.get('Gender', ''),
                record.get('Age', ''),
                record.get('Occupation', 0),
                record.get('City_Category', ''),
                record.get('Stay_In_Current_City_Years', 0),
                record.get('Marital_Status', 0)
            ))
            self.connection.commit()
            
            self.cursor.execute(
                "SELECT Customer_SK FROM DIM_CUSTOMER WHERE Customer_ID = ?",
                (customer_id,)
            )
            return self.cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error with customer {customer_id}: {e}")
            return 0
    
    def _get_or_insert_product(self, product_id: str, record: Dict) -> int:
        """Get surrogate key for product, insert if not exists."""
        try:
            self.cursor.execute(
                "SELECT Product_SK FROM DIM_PRODUCT WHERE Product_ID = ?",
                (product_id,)
            )
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            
            # Insert new product
            self.cursor.execute("""
                INSERT INTO DIM_PRODUCT (Product_ID, Product_Category, Price)
                VALUES (?, ?, ?)
            """, (
                product_id,
                record.get('Product_Category', ''),
                float(record.get('price$', 0)) if 'price$' in record else 0
            ))
            self.connection.commit()
            
            self.cursor.execute(
                "SELECT Product_SK FROM DIM_PRODUCT WHERE Product_ID = ?",
                (product_id,)
            )
            return self.cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error with product {product_id}: {e}")
            return 0
    
    def _get_or_insert_store(self, store_id: int, record: Dict) -> int:
        """Get surrogate key for store, insert if not exists."""
        try:
            self.cursor.execute(
                "SELECT Store_SK FROM DIM_STORE WHERE StoreID = ?",
                (store_id,)
            )
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            
            # Insert new store
            self.cursor.execute("""
                INSERT INTO DIM_STORE (StoreID, StoreName)
                VALUES (?, ?)
            """, (
                store_id,
                record.get('storeName', '')
            ))
            self.connection.commit()
            
            self.cursor.execute(
                "SELECT Store_SK FROM DIM_STORE WHERE StoreID = ?",
                (store_id,)
            )
            return self.cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error with store {store_id}: {e}")
            return 0
    
    def _get_or_insert_supplier(self, supplier_id: int, record: Dict) -> int:
        """Get surrogate key for supplier, insert if not exists."""
        try:
            self.cursor.execute(
                "SELECT Supplier_SK FROM DIM_SUPPLIER WHERE SupplierID = ?",
                (supplier_id,)
            )
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            
            # Insert new supplier
            self.cursor.execute("""
                INSERT INTO DIM_SUPPLIER (SupplierID, SupplierName)
                VALUES (?, ?)
            """, (
                supplier_id,
                record.get('supplierName', '')
            ))
            self.connection.commit()
            
            self.cursor.execute(
                "SELECT Supplier_SK FROM DIM_SUPPLIER WHERE SupplierID = ?",
                (supplier_id,)
            )
            return self.cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error with supplier {supplier_id}: {e}")
            return 0
    
    def _get_or_insert_date(self, date_str: str) -> int:
        """Get surrogate key for date, insert if not exists."""
        try:
            self.cursor.execute(
                "SELECT Date_SK FROM DIM_TIME WHERE TransactionDate = ?",
                (date_str,)
            )
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            
            # Parse date
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            quarter = (month - 1) // 3 + 1
            day_of_week = date_obj.weekday()  # 0=Monday, 6=Sunday
            is_weekend = 1 if day_of_week >= 5 else 0
            
            # Determine season
            if month in [12, 1, 2]:
                season = 'Winter'
            elif month in [3, 4, 5]:
                season = 'Spring'
            elif month in [6, 7, 8]:
                season = 'Summer'
            else:
                season = 'Fall'
            
            # Insert date dimension
            self.cursor.execute("""
                INSERT INTO DIM_TIME 
                (TransactionDate, Year, Month, Quarter, Day, DayOfWeek, Season, IsWeekend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date_str, year, month, quarter, day, day_of_week, season, is_weekend
            ))
            self.connection.commit()
            
            self.cursor.execute(
                "SELECT Date_SK FROM DIM_TIME WHERE TransactionDate = ?",
                (date_str,)
            )
            return self.cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error with date {date_str}: {e}")
            return 0
    
    def run_etl(self) -> None:
        """Execute full ETL process with multi-threading."""
        logger.info("="*80)
        logger.info("Starting ETL Process for Walmart Data Warehouse")
        logger.info("="*80)
        
        # Connect to database
        if not self.connect_database():
            logger.error("Failed to connect to database. Exiting.")
            return
        
        # Extract master data
        master_data = self.extract_master_data()
        
        # Create threading event
        stream_complete_event = threading.Event()
        
        # Thread 1: Stream transactional data
        stream_thread = threading.Thread(
            target=self.stream_transactional_data,
            args=(stream_complete_event,),
            name="StreamReader"
        )
        
        # Thread 2: Transform and load data using HYBRIDJOIN
        transform_thread = threading.Thread(
            target=self.transform_and_load_data,
            args=(master_data,),
            name="TransformLoad"
        )
        
        # Start threads
        start_time = time.time()
        stream_thread.start()
        
        # Give stream a moment to start
        time.sleep(1)
        
        transform_thread.start()
        
        # Wait for both threads to complete
        stream_thread.join()
        transform_thread.join()
        
        elapsed_time = time.time() - start_time
        
        # Print statistics
        stats = self.hybrid_join.get_statistics()
        logger.info("="*80)
        logger.info("ETL Process Complete")
        logger.info(f"Elapsed Time: {elapsed_time:.2f} seconds")
        logger.info(f"Statistics:")
        logger.info(f"  - Tuples Processed: {stats['tuples_processed']}")
        logger.info(f"  - Tuples Joined: {stats['tuples_joined']}")
        logger.info(f"  - Partitions Processed: {stats['partitions_processed']}")
        logger.info("="*80)
        
        # Close database connection
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def get_database_credentials() -> Dict[str, str]:
    """
    Prompt user for database credentials.
    
    Returns:
        Dictionary containing database connection parameters
    """
    print("\n" + "="*60)
    print("Database Connection Setup")
    print("="*60)
    print("\nNote: For Windows Authentication (recommended):")
    print("  - Leave Username and Password blank")
    print("  - Just press Enter when prompted")
    print("="*60 + "\n")
    
    config = {}
    config['server'] = input("Enter SQL Server address (e.g., localhost): ").strip()
    config['database'] = input("Enter Database name (e.g., WalmartDW): ").strip()
    config['username'] = input("Enter Username (leave blank for Windows Auth): ").strip()
    config['password'] = input("Enter Password (leave blank for Windows Auth): ").strip()
    
    print("="*60 + "\n")
    
    return config


def main():
    """Main entry point."""
    import sys
    
    print("\n" + "="*80)
    print("HYBRIDJOIN - Walmart Data Warehouse ETL Pipeline")
    print("="*80)
    
    # Support command line arguments for automation
    # Usage: python main.py <server> <database> <username> <password>
    if len(sys.argv) >= 3:
        db_config = {
            'server': sys.argv[1],
            'database': sys.argv[2],
            'username': sys.argv[3] if len(sys.argv) > 3 else '',
            'password': sys.argv[4] if len(sys.argv) > 4 else ''
        }
        print(f"Using command-line credentials for {db_config['database']}")
    else:
        # Get database credentials interactively
        db_config = get_database_credentials()
    
    # Create and run ETL orchestrator
    etl = ETLOrchestrator(db_config)
    etl.run_etl()


if __name__ == "__main__":
    main()
