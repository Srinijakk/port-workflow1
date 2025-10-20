"""
Crane Operation Handlers - FIXED for Variable Mapping
Handles crane loading and unloading tasks with proper camelCase to snake_case conversion.
"""

import logging
import time
from typing import Dict, Any
from config.database import get_container, update_storage_status

logger = logging.getLogger(__name__)


def handle_crane_loading(job_key: int, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle crane loading operations.
    Maps camelCase variables from Camunda to snake_case for database.
    
    Args:
        job_key: Unique identifier for the job
        variables: Process variables from Camunda (camelCase)
        
    Returns:
        Updated variables dictionary (camelCase)
    """
    logger.info(f" Starting CRANE LOADING - Job Key: {job_key}")
    
    # Map camelCase to snake_case - with validation
    container_id = variables.get('containerId')
    transportation_id = variables.get('transportationId')
    operation_type = variables.get('operationType', 'loading')
    
    # Log received variables
    logger.info(f"   Container ID: {container_id}")
    logger.info(f"   Transportation ID: {transportation_id}")
    logger.info(f"   Operation Type: {operation_type}")
    
    # Validation: Check if required variables are provided
    if not container_id or container_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'containerId' must be provided when starting the workflow"
        logger.error(error_msg)
        logger.error(" Start the workflow with: {\"containerId\": \"C1001\", \"transportationId\": \"truck101\", \"operationType\": \"loading\"}")
        raise ValueError(error_msg)
    
    if not transportation_id or transportation_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'transportationId' must be provided when starting the workflow"
        logger.error(error_msg)
        logger.error(" Start the workflow with: {\"containerId\": \"C1001\", \"transportationId\": \"truck101\", \"operationType\": \"loading\"}")
        raise ValueError(error_msg)
    
    try:
        # Get container information from database (using snake_case)
        container_data = get_container(container_id)
        if container_data:
            logger.info(f"   Container Weight: {container_data.get('weight')} kg")
            logger.info(f"   Storage Status: {container_data.get('storage_status')}")
        else:
            logger.warning(f"    Container {container_id} not found in database (may be created later)")
        
        # Simulate crane loading operation
        logger.info("    Positioning crane...")
        time.sleep(1)
        
        logger.info("    Attaching container...")
        time.sleep(1)
        
        logger.info("    Lifting container...")
        time.sleep(1)
        
        logger.info("    Moving to target location...")
        time.sleep(1)
        
        logger.info("    Lowering container...")
        time.sleep(1)
        
        logger.info("    Container secured!")
        
        # Prepare result variables (camelCase for Camunda)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        operator = 'CRANE-OP-001'
        
        result = {
            **variables,  # Keep all existing variables
            'craneLoadingStatus': 'completed',
            'craneLoadingTimestamp': timestamp,
            'craneOperator': operator,
            'containerId': container_id,  # Ensure it's passed forward
            'transportationId': transportation_id,
            'operationType': operation_type
        }
        
        logger.info(f" CRANE LOADING completed successfully")
        return result
        
    except ValueError as ve:
        # Re-raise validation errors
        raise
    except Exception as e:
        logger.error(f" Error during crane loading: {e}")
        raise


def handle_crane_unloading(job_key: int, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle crane unloading operations.
    Maps camelCase variables from Camunda to snake_case for database.
    
    Args:
        job_key: Unique identifier for the job
        variables: Process variables from Camunda (camelCase)
        
    Returns:
        Updated variables dictionary (camelCase)
    """
    logger.info(f" Starting CRANE UNLOADING - Job Key: {job_key}")
    
    # Map camelCase to snake_case - with validation
    container_id = variables.get('containerId')
    transportation_id = variables.get('transportationId')
    operation_type = variables.get('operationType', 'unloading')
    
    # Log received variables
    logger.info(f"   Container ID: {container_id}")
    logger.info(f"   Transportation ID: {transportation_id}")
    logger.info(f"   Operation Type: {operation_type}")
    
    # Validation: Check if required variables are provided
    if not container_id or container_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'containerId' must be provided when starting the workflow"
        logger.error(error_msg)
        logger.error(" Start the workflow with: {\"containerId\": \"C1002\", \"transportationId\": \"ship9109\", \"operationType\": \"unloading\"}")
        raise ValueError(error_msg)
    
    if not transportation_id or transportation_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'transportationId' must be provided when starting the workflow"
        logger.error(error_msg)
        logger.error(" Start the workflow with: {\"containerId\": \"C1002\", \"transportationId\": \"ship9109\", \"operationType\": \"unloading\"}")
        raise ValueError(error_msg)
    
    try:
        # Get container information from database (using snake_case)
        container_data = get_container(container_id)
        if container_data:
            logger.info(f"   Container Weight: {container_data.get('weight')} kg")
            logger.info(f"   Storage Status: {container_data.get('storage_status')}")
        else:
            logger.warning(f"  Container {container_id} not found in database (may be created later)")
        
        # Simulate crane unloading operation
        logger.info("    Positioning crane...")
        time.sleep(1)
        
        logger.info("    Attaching to container...")
        time.sleep(1)
        
        logger.info("    Lifting container...")
        time.sleep(1)
        
        logger.info("    Moving to unloading zone...")
        time.sleep(1)
        
        logger.info("   ⬇ Placing container...")
        time.sleep(1)
        
        logger.info("    Container unloaded!")
        
        # Prepare result variables (camelCase for Camunda)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        operator = 'CRANE-OP-002'
        unloading_zone = 'ZONE-A1'
        
        result = {
            **variables,  # Keep all existing variables
            'craneUnloadingStatus': 'completed',
            'craneUnloadingTimestamp': timestamp,
            'craneOperator': operator,
            'unloadingZone': unloading_zone,
            'containerId': container_id,  # Ensure it's passed forward
            'transportationId': transportation_id,
            'operationType': operation_type
        }
        
        logger.info(f" CRANE UNLOADING completed successfully")
        return result
        
    except ValueError as ve:
        # Re-raise validation errors
        raise
    except Exception as e:
        logger.error(f" Error during crane unloading: {e}")
        raise