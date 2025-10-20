import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'portmanagement',
    'user': 'postgres',
    'password': 'srinija'  # CHANGE THIS!
}

# Connection pool
connection_pool: Optional[pool.SimpleConnectionPool] = None

def initialize_connection_pool(min_conn=1, max_conn=10):
    """
    Initialize PostgreSQL connection pool.
    
    Args:
        min_conn: Minimum number of connections
        max_conn: Maximum number of connections
    """
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            min_conn,
            max_conn,
            **DB_CONFIG
        )
        if connection_pool:
            logger.info("PostgreSQL connection pool created successfully")
            logger.info(f"   Database: {DB_CONFIG['database']}")
            logger.info(f"   Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    except Exception as e:
        logger.error(f"Failed to create connection pool: {e}")
        raise

def get_connection():
    """
    Get a connection from the pool.
    
    Returns:
        psycopg2 connection object
    """
    if connection_pool:
        return connection_pool.getconn()
    else:
        raise Exception("Connection pool not initialized")

def return_connection(conn):
    """
    Return a connection to the pool.
    
    Args:
        conn: psycopg2 connection object
    """
    if connection_pool:
        connection_pool.putconn(conn)

def close_all_connections():
    """Close all connections in the pool."""
    if connection_pool:
        connection_pool.closeall()
        logger.info("🔒 All database connections closed")

def test_connection():
    """
    Test database connection.
    
    Returns:
        bool: True if connection successful
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info("✅ Database connection test successful")
        logger.info(f"   PostgreSQL version: {version[0][:50]}...")
        cursor.close()
        return_connection(conn)
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

def insert_process_instance(
    process_instance_key: int, 
    operation_type: str = None,
    container_id: str = None,
    transportation_id: str = None
) -> bool:
    """
    Insert a new process instance from Camunda workflow with detailed description.
    
    Args:
        process_instance_key: Camunda process instance key
        operation_type: 'loading' or 'unloading'
        container_id: Container ID being processed
        transportation_id: Transport ID (truck or ship)
        
    Returns:
        bool: True if insert successful
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Build description with available information
        start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        description_parts = [f"Started: {start_date}"]
        
        if operation_type:
            description_parts.append(f"Operation: {operation_type}")
        
        if container_id:
            description_parts.append(f"Container: {container_id}")
            
        if transportation_id:
            description_parts.append(f"Transport: {transportation_id}")
        
        description = ", ".join(description_parts)
        
        insert_query = """
        INSERT INTO process_instance (process_instance_key, description)
        VALUES (%s, %s)
        ON CONFLICT (process_instance_key) DO UPDATE
        SET description = EXCLUDED.description;
        """
        
        cursor.execute(insert_query, (process_instance_key, description))
        conn.commit()
        cursor.close()
        
        logger.info(f"📝 Process instance {process_instance_key} recorded")
        logger.info(f"   Description: {description}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to insert process instance: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)

def update_process_instance_completion(
    process_instance_key: int,
    final_status: str = "completed"
) -> bool:
    """
    Update process instance with completion information.
    
    Args:
        process_instance_key: Camunda process instance key
        final_status: Final status of the process
        
    Returns:
        bool: True if update successful
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get existing description
        select_query = """
        SELECT description FROM process_instance
        WHERE process_instance_key = %s;
        """
        
        cursor.execute(select_query, (process_instance_key,))
        result = cursor.fetchone()
        
        if result:
            existing_desc = result[0]
            end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Append completion info
            updated_desc = f"{existing_desc}, Ended: {end_date}, Status: {final_status}"
            
            update_query = """
            UPDATE process_instance
            SET description = %s
            WHERE process_instance_key = %s;
            """
            
            cursor.execute(update_query, (updated_desc, process_instance_key))
            conn.commit()
            
            logger.info(f"📝 Process instance {process_instance_key} updated with completion")
            logger.info(f"   Final description: {updated_desc}")
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to update process instance: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)

def get_container(container_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve container information.
    
    Args:
        container_id: Container identifier
        
    Returns:
        Dict containing container data or None
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT c.*, s.storage_id, s.storage_status, 
               t.transportation_id, t.check_in, t.check_out
        FROM container c
        LEFT JOIN storage s ON c.container_id = s.container_id
        LEFT JOIN transport_mean t ON c.container_id = t.container_id
        WHERE c.container_id = %s;
        """
        
        cursor.execute(query, (container_id,))
        record = cursor.fetchone()
        cursor.close()
        
        if record:
            return dict(record)
        return None
        
    except Exception as e:
        logger.error(f"Failed to retrieve container: {e}")
        return None
    finally:
        if conn:
            return_connection(conn)

def update_storage_status(container_id: str, storage_status: str) -> bool:
    """
    Update storage status for a container.
    
    Args:
        container_id: Container identifier
        storage_status: 'complete' or 'incomplete'
        
    Returns:
        bool: True if update successful
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        update_query = """
        UPDATE storage
        SET storage_status = %s
        WHERE container_id = %s;
        """
        
        cursor.execute(update_query, (storage_status, container_id))
        conn.commit()
        rows_affected = cursor.rowcount
        cursor.close()
        
        if rows_affected > 0:
            logger.info(f"📦 Container {container_id} storage status updated: {storage_status}")
            return True
        else:
            logger.warning(f"⚠️ No storage record found for container: {container_id}")
            return False
        
    except Exception as e:
        logger.error(f"Failed to update storage status: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)

def update_transport_timestamps(transportation_id: str, check_in: str = None, check_out: str = None) -> bool:
    """
    Update truck check-in/check-out timestamps.
    Only applies to trucks (transportation_id starting with 'truck').
    
    Args:
        transportation_id: Transport ID
        check_in: Check-in timestamp
        check_out: Check-out timestamp
        
    Returns:
        bool: True if update successful
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Build dynamic update based on provided values
        updates = []
        values = []
        
        if check_in is not None:
            updates.append("check_in = %s")
            values.append(check_in)
        
        if check_out is not None:
            updates.append("check_out = %s")
            values.append(check_out)
        
        if not updates:
            logger.warning("No timestamp values provided for update")
            return False
        
        values.append(transportation_id)
        
        update_query = f"""
        UPDATE transport_mean
        SET {', '.join(updates)}
        WHERE transportation_id = %s;
        """
        
        cursor.execute(update_query, values)
        conn.commit()
        rows_affected = cursor.rowcount
        cursor.close()
        
        if rows_affected > 0:
            logger.info(f" Transport {transportation_id} timestamps updated")
            return True
        else:
            logger.warning(f" No transport record found: {transportation_id}")
            return False
        
    except Exception as e:
        logger.error(f" Failed to update transport timestamps: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)

def get_all_containers() -> list:
    """
    Retrieve all containers with their associated data.
    
    Returns:
        List of dictionaries containing container data
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT c.container_id, c.operation_type, c.weight,
               s.storage_id, s.storage_status,
               t.transportation_id, t.check_in, t.check_out
        FROM container c
        LEFT JOIN storage s ON c.container_id = s.container_id
        LEFT JOIN transport_mean t ON c.container_id = t.container_id
        ORDER BY c.container_id;
        """
        
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        
        return [dict(record) for record in records]
        
    except Exception as e:
        logger.error(f" Failed to retrieve containers: {e}")
        return []
    finally:
        if conn:
            return_connection(conn)

def get_active_operations() -> list:
    """
    Get all active operations (incomplete storage).
    
    Returns:
        List of active operation records
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM active_operations;"
        
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        
        return [dict(record) for record in records]
        
    except Exception as e:
        logger.error(f" Failed to retrieve active operations: {e}")
        return []
    finally:
        if conn:
            return_connection(conn)

def get_all_process_instances() -> list:
    """
    Retrieve all process instances from database.
    
    Returns:
        List of process instance records
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT process_instance_key, description
        FROM process_instance
        ORDER BY process_instance_key DESC;
        """
        
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        
        return [dict(record) for record in records]
        
    except Exception as e:
        logger.error(f" Failed to retrieve process instances: {e}")
        return []
    finally:
        if conn:
            return_connection(conn)