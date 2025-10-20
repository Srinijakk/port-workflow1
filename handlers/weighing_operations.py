"""
Weighing Operation Handlers - FIXED for Variable Mapping
Handles container weighing tasks with proper camelCase to snake_case conversion.
"""

import logging
import time
from typing import Dict, Any
from config.database import get_container

logger = logging.getLogger(__name__)


def handle_weighing(job_key: int, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle container weighing operations.
    Maps camelCase variables from Camunda to snake_case for database.
    Retrieves weight from database and validates it.
    
    Args:
        job_key: Unique identifier for the job
        variables: Process variables from Camunda (camelCase)
        
    Returns:
        Updated variables dictionary with weight information (camelCase)
    """
    logger.info(f" Starting WEIGHING OPERATION - Job Key: {job_key}")
    
    # Map camelCase to snake_case - with validation
    container_id = variables.get('containerId')
    transportation_id = variables.get('transportationId')
    operation_type = variables.get('operationType', 'unknown')
    
    # Log received variables
    logger.info(f"   Container ID: {container_id}")
    logger.info(f"   Transportation ID: {transportation_id}")
    logger.info(f"   Operation Type: {operation_type}")
    
    # Validation: Check if required variables are provided
    if not container_id or container_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'containerId' must be provided when starting the workflow"
        logger.error(error_msg)
        logger.error(" Start the workflow with: {\"containerId\": \"C1002\", \"transportationId\": \"ship9109\", \"operationType\": \"unloading\"}")
        logger.error(" Available containers in database: C1001-C1010")
        raise ValueError(error_msg)
    
    try:
        # Get container information from database (using snake_case columns)
        container_data = get_container(container_id)
        
        if not container_data:
            error_msg = f" Container {container_id} not found in database"
            logger.error(error_msg)
            logger.error(" Make sure the container exists in the 'container' table")
            logger.error(" Available sample containers: C1001, C1002, C1003, C1004, C1005, C1006, C1007, C1008, C1009, C1010")
            raise Exception(error_msg)
        
        # Get weight from database (snake_case column)
        weight = container_data.get('weight', 0)
        
        # Simulate weighing process
        logger.info("    Positioning container on scale...")
        time.sleep(1)
        
        logger.info("    Calibrating scale...")
        time.sleep(0.5)
        
        logger.info("    Measuring weight...")
        time.sleep(1)
        
        logger.info(f"    Container Weight: {weight} kg")
        
        # Verify weight limits
        max_weight = 30480  # Standard ISO container max weight (kg)
        weight_status = 'OK' if weight <= max_weight else 'OVERWEIGHT'
        
        if weight_status == 'OVERWEIGHT':
            logger.warning(f"    WARNING: Container exceeds maximum weight!")
        else:
            logger.info(f"   Weight within acceptable limits")
        
        # Prepare result variables (camelCase for Camunda)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        scale_id = 'SCALE-001'
        weighing_operator = 'WEIGH-OP-001'
        
        result = {
            **variables,  # Keep all existing variables
            'weighingStatus': 'completed',
            'weighingTimestamp': timestamp,
            'weight': weight,
            'weightUnit': 'kg',
            'weightStatus': weight_status,
            'scaleId': scale_id,
            'weighingOperator': weighing_operator,
            'containerId': container_id,  # Ensure it's passed forward
            'transportationId': transportation_id,
            'operationType': operation_type
        }
        
        logger.info(f" WEIGHING completed successfully - {weight} kg")
        return result
        
    except ValueError as ve:
        # Re-raise validation errors
        raise
    except Exception as e:
        logger.error(f" Error during weighing operation: {e}")
        raise