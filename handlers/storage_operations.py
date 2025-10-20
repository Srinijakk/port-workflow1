"""
Storage Operation Handlers - FIXED for Variable Mapping
Handles updating storage status with proper camelCase to snake_case conversion.
"""

import logging
import time
from typing import Dict, Any
from config.database import get_container, update_storage_status

logger = logging.getLogger(__name__)


def handle_storage(job_key: int, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle storage operations.
    Maps camelCase variables from Camunda to snake_case for database.
    Updates storage status to 'complete' after successful operations.
    
    Args:
        job_key: Unique identifier for the job
        variables: Process variables from Camunda (camelCase)
        
    Returns:
        Updated variables dictionary with storage information (camelCase)
    """
    logger.info(f" Starting STORAGE OPERATION - Job Key: {job_key}")
    
    # Map camelCase to snake_case - with validation
    container_id = variables.get('containerId')
    operation_type = variables.get('operationType', 'unknown')
    transportation_id = variables.get('transportationId')
    
    # Log received variables
    logger.info(f"   Container ID: {container_id}")
    logger.info(f"   Operation Type: {operation_type}")
    logger.info(f"   Transportation ID: {transportation_id}")
    
    # Validation: Check if required variables are provided
    if not container_id or container_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'containerId' must be provided when starting the workflow"
        logger.error(error_msg)
        logger.error(" Start the workflow with: {\"containerId\": \"C1002\", \"transportationId\": \"ship9109\", \"operationType\": \"unloading\"}")
        raise ValueError(error_msg)
    
    try:
        # Get container information from database (using snake_case columns)
        container_data = get_container(container_id)
        
        if not container_data:
            error_msg = f" Container {container_id} not found in database"
            logger.error(error_msg)
            logger.error(" Make sure the container exists in the 'container' table")
            raise Exception(error_msg)
        
        weight = container_data.get('weight', 0)
        current_storage_status = container_data.get('storage_status', 'incomplete')
        
        logger.info(f"   Container Weight: {weight} kg")
        logger.info(f"   Current Storage Status: {current_storage_status}")
        
        # Simulate storage operation
        logger.info("    Processing storage operation...")
        time.sleep(0.5)
        
        logger.info("    Transporting to storage location...")
        time.sleep(1.5)
        
        logger.info("    Positioning container...")
        time.sleep(1)
        
        logger.info("    Securing container...")
        time.sleep(1)
        
        logger.info("    Finalizing storage...")
        time.sleep(0.5)
        
        logger.info("    Container stored successfully!")
        
        # Update storage status to complete (database uses snake_case)
        storage_status = 'complete'
        success = update_storage_status(container_id, storage_status)
        
        if success:
            logger.info(f" Database updated: storage status = {storage_status}")
        else:
            logger.warning(f" Database update may have failed for {container_id}")
        
        # Prepare result variables (camelCase for Camunda)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        storage_operator = 'STORAGE-OP-001'
        
        result = {
            **variables,  # Keep all existing variables
            'storageStatus': storage_status,
            'storageTimestamp': timestamp,
            'storageOperator': storage_operator,
            'storageId': container_data.get('storage_id'),
            'containerId': container_id,  # Ensure it's passed forward
            'transportationId': transportation_id,
            'operationType': operation_type
        }
        
        logger.info(f" STORAGE completed - Status: {storage_status}")
        return result
        
    except ValueError as ve:
        # Re-raise validation errors
        raise
    except Exception as e:
        logger.error(f" Error during storage operation: {e}")
        raise