"""
HYBRIDJOIN Algorithm Implementation
Data Warehousing Project - Walmart Near-Real-Time DW
Implements stream-relation join for ETL with data enrichment
"""

import threading
import time
import queue
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HybridJoin:
    """
    HYBRIDJOIN algorithm implementation for stream-relation join operations.
    
    Components:
    - Stream Buffer: Queue to hold incoming stream tuples
    - Hash Table: Multi-map to store stream tuples with hash key
    - Queue: Doubly-linked list tracking FIFO order of stream tuples
    - Disk Buffer: In-memory buffer for loaded relation partitions
    """
    
    def __init__(self, hash_slots: int = 10000, partition_size: int = 500):
        """
        Initialize HYBRIDJOIN components.
        
        Args:
            hash_slots: Number of slots in hash table (default 10,000)
            partition_size: Size of each disk partition (default 500 tuples)
        """
        self.hS = hash_slots
        self.vP = partition_size
        self.w = hash_slots  # Initially all slots are free
        
        # Stream Buffer: Queue to hold incoming stream tuples
        # Increased maxsize to 5000 for better throughput with large datasets
        self.stream_buffer = queue.Queue(maxsize=5000)
        
        # Hash Table: Multi-map structure
        # Structure: {hash_key: [(tuple_data, queue_node_id), ...]}
        self.hash_table = defaultdict(list)
        
        # Queue: Doubly-linked list for FIFO order tracking
        # Structure: [key1, key2, key3, ...]
        self.key_queue = deque()
        self.key_queue_map = {}  # For efficient node access
        
        # Disk Buffer: Stores current partition from relation R
        self.disk_buffer = []
        
        # Join Output Buffer
        self.join_output = []
        
        # Statistics
        self.tuples_joined = 0
        self.tuples_processed = 0
        self.partitions_processed = 0
        
        # Control flags
        self.stop_flag = False
        self.lock = threading.Lock()
        
        logger.info(f"HYBRIDJOIN initialized: hash_slots={hash_slots}, partition_size={partition_size}")
    
    def add_stream_tuple(self, tuple_data: Dict[str, Any]) -> None:
        """
        Add incoming stream tuple to stream buffer.
        
        Args:
            tuple_data: Dictionary containing stream tuple data
        """
        try:
            self.stream_buffer.put_nowait(tuple_data)
        except queue.Full:
            logger.warning("Stream buffer full, waiting for space...")
            try:
                self.stream_buffer.put(tuple_data, timeout=10)
            except queue.Full:
                logger.error("Stream buffer timeout - consumer thread may be blocked")
                raise
    
    def _hash_function(self, key: str) -> int:
        """
        Hash function to map join key to hash table slot.
        
        Args:
            key: Join key (e.g., Customer_ID)
            
        Returns:
            Slot number in hash table
        """
        return hash(key) % self.hS
    
    def _load_stream_tuples(self) -> int:
        """
        Load up to w stream tuples into hash table from stream buffer.
        
        Returns:
            Number of tuples loaded
        """
        tuples_loaded = 0
        
        while tuples_loaded < self.w and not self.stream_buffer.empty():
            try:
                tuple_data = self.stream_buffer.get_nowait()
                
                # Extract join key (Customer_ID)
                join_key = str(tuple_data.get('Customer_ID', ''))
                
                # Hash and insert into hash table
                slot = self._hash_function(join_key)
                self.hash_table[slot].append(tuple_data)
                
                # Add key to queue
                self.key_queue.append(join_key)
                self.key_queue_map[join_key] = tuple_data
                
                tuples_loaded += 1
                self.tuples_processed += 1
                
            except queue.Empty:
                break
        
        # Update available slots
        self.w = 0
        
        logger.debug(f"Loaded {tuples_loaded} tuples into hash table")
        return tuples_loaded
    
    def _get_oldest_key(self) -> str:
        """
        Get the oldest key from the queue (FIFO).
        
        Returns:
            Oldest join key or None if queue is empty
        """
        if self.key_queue:
            return self.key_queue[0]
        return None
    
    def _load_disk_partition(self, relation_data: List[Dict], key: str) -> None:
        """
        Load partition from relation R that matches the oldest key.
        
        Args:
            relation_data: Full relation data (Master Data)
            key: Join key to match
        """
        self.disk_buffer = []
        
        # Filter relation data for tuples matching the key
        count = 0
        for row in relation_data:
            if str(row.get('Customer_ID', '')) == key and count < self.vP:
                self.disk_buffer.append(row)
                count += 1
            if count >= self.vP:
                break
        
        self.partitions_processed += 1
        logger.debug(f"Loaded partition for key {key}: {len(self.disk_buffer)} tuples")
    
    def _probe_and_join(self) -> int:
        """
        Probe hash table with disk buffer tuples and generate join output.
        
        Returns:
            Number of tuples matched and deleted from hash table
        """
        matched_count = 0
        
        # Iterate through disk buffer
        for disk_tuple in self.disk_buffer:
            join_key = str(disk_tuple.get('Customer_ID', ''))
            slot = self._hash_function(join_key)
            
            # Check if key exists in hash table
            if slot in self.hash_table:
                # Probe: match tuples
                for stream_tuple in self.hash_table[slot]:
                    if str(stream_tuple.get('Customer_ID', '')) == join_key:
                        # Generate join output (enriched record)
                        joined_record = {
                            **stream_tuple,
                            **disk_tuple
                        }
                        self.join_output.append(joined_record)
                        matched_count += 1
                        self.tuples_joined += 1
                
                # Delete matched tuples from hash table
                self.hash_table[slot] = [
                    t for t in self.hash_table[slot]
                    if str(t.get('Customer_ID', '')) != join_key
                ]
                
                # Remove from queue
                if join_key in self.key_queue_map:
                    self.key_queue.remove(join_key)
                    del self.key_queue_map[join_key]
                
                # If slot is now empty, remove it
                if not self.hash_table[slot]:
                    del self.hash_table[slot]
        
        # Update available slots
        self.w += matched_count
        
        logger.debug(f"Matched {matched_count} tuples in probe phase")
        return matched_count
    
    def execute(self, relation_data: List[Dict]) -> List[Dict]:
        """
        Execute HYBRIDJOIN algorithm.
        
        Args:
            relation_data: Master Data (Customer & Product dimensions)
            
        Returns:
            List of joined records (enriched data for DW loading)
        """
        logger.info("Starting HYBRIDJOIN algorithm execution...")
        
        iteration = 0
        while not self.stop_flag or not self.stream_buffer.empty():
            iteration += 1
            logger.info(f"=== Iteration {iteration} ===")
            
            # Step 1: Load stream tuples
            loaded = self._load_stream_tuples()
            if loaded == 0 and self.stream_buffer.empty():
                if not self.key_queue:
                    break
            
            logger.info(f"Loaded {loaded} tuples | Hash table size: {len(self.hash_table)} slots")
            
            # Step 2: Get oldest key from queue
            oldest_key = self._get_oldest_key()
            if not oldest_key:
                if self.stop_flag:
                    break
                time.sleep(0.1)
                continue
            
            # Step 3: Load disk partition for oldest key
            self._load_disk_partition(relation_data, oldest_key)
            
            # Step 4: Probe and join
            matched = self._probe_and_join()
            
            logger.info(f"Iteration {iteration} complete | Matched: {matched} | Total joined: {self.tuples_joined}")
            
            if self.stop_flag and not self.key_queue:
                break
        
        logger.info(f"HYBRIDJOIN execution complete!")
        logger.info(f"Statistics:")
        logger.info(f"  - Total tuples processed: {self.tuples_processed}")
        logger.info(f"  - Total tuples joined: {self.tuples_joined}")
        logger.info(f"  - Partitions processed: {self.partitions_processed}")
        
        return self.join_output
    
    def get_statistics(self) -> Dict[str, int]:
        """Get algorithm statistics."""
        return {
            'tuples_processed': self.tuples_processed,
            'tuples_joined': self.tuples_joined,
            'partitions_processed': self.partitions_processed,
            'hash_table_size': len(self.hash_table),
            'queue_size': len(self.key_queue)
        }
