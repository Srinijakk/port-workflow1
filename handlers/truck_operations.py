"""
Truck Check-In/Check-Out Operation Handlers
Handles truck entry and exit operations with timestamp management.
"""

import logging
import time
from typing import Dict, Any
from datetime import datetime
from config.database import update_transport_timestamps

logger = logging.getLogger(__name__)


def handle_truck_checkin(job_key: int, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle truck check-in operations.
    Records the truck arrival time and updates database.
    
    Args:
        job_key: Unique identifier for the job
        variables: Process variables from Camunda (camelCase)
        
    Returns:
        Updated variables dictionary (camelCase)
    """
    logger.info(f" Starting TRUCK CHECK-IN - Job Key: {job_key}")
    
    # Extract variables
    container_id = variables.get('containerId')
    transportation_id = variables.get('transportationId')
    operation_type = variables.get('operationType', 'unknown')
    check_in = variables.get('checkIn')  # May already exist from database
    
    # Log received variables
    logger.info(f"   Container ID: {container_id}")
    logger.info(f"   Transportation ID: {transportation_id}")
    logger.info(f"   Operation Type: {operation_type}")
    
    # Validation
    if not container_id or container_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'containerId'"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    if not transportation_id or transportation_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'transportationId'"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate it's a truck
    if not transportation_id.startswith('truck'):
        error_msg = f" Invalid transportation ID: {transportation_id} (must start with 'truck')"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        # Use existing check_in time from database or generate new one
        if not check_in:
            check_in_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"    Generated Check-In Time: {check_in_timestamp}")
        else:
            check_in_timestamp = check_in
            logger.info(f"    Using Existing Check-In Time: {check_in_timestamp}")
        
        # Simulate check-in process
        logger.info("    Verifying truck documents...")
        time.sleep(0.5)
        
        logger.info("    Inspecting vehicle...")
        time.sleep(0.5)
        
        logger.info("    Truck checked in successfully!")
        
        # Update database with check-in timestamp
        success = update_transport_timestamps(
            transportation_id=transportation_id,
            check_in=check_in_timestamp
        )
        
        if success:
            logger.info(f" Database updated: check_in = {check_in_timestamp}")
        else:
            logger.warning(f" Database update may have failed for {transportation_id}")
        
        # Prepare result variables (camelCase for Camunda)
        result = {
            **variables,  # Keep all existing variables
            'checkIn': check_in_timestamp,
            'truckCheckInStatus': 'completed',
            'truckCheckInOperator': 'GATE-OP-001',
            'containerId': container_id,
            'transportationId': transportation_id,
            'operationType': operation_type
        }
        
        logger.info(f" TRUCK CHECK-IN completed successfully")
        return result
        
    except ValueError as ve:
        raise
    except Exception as e:
        logger.error(f" Error during truck check-in: {e}")
        raise


def handle_truck_checkout(job_key: int, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle truck check-out operations.
    Records the truck departure time and updates database.
    
    Args:
        job_key: Unique identifier for the job
        variables: Process variables from Camunda (camelCase)
        
    Returns:
        Updated variables dictionary (camelCase)
    """
    logger.info(f" Starting TRUCK CHECK-OUT - Job Key: {job_key}")
    
    # Extract variables
    container_id = variables.get('containerId')
    transportation_id = variables.get('transportationId')
    operation_type = variables.get('operationType', 'unknown')
    check_in = variables.get('checkIn')
    check_out = variables.get('checkOut')  # May already exist from database
    
    # Log received variables
    logger.info(f"   Container ID: {container_id}")
    logger.info(f"   Transportation ID: {transportation_id}")
    logger.info(f"   Operation Type: {operation_type}")
    logger.info(f"   Check-In Time: {check_in}")
    
    # Validation
    if not container_id or container_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'containerId'"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    if not transportation_id or transportation_id == 'N/A':
        error_msg = " MISSING REQUIRED VARIABLE: 'transportationId'"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate it's a truck
    if not transportation_id.startswith('truck'):
        error_msg = f" Invalid transportation ID: {transportation_id} (must start with 'truck')"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        # Use existing check_out time from database or generate new one
        if not check_out:
            check_out_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"    Generated Check-Out Time: {check_out_timestamp}")
        else:
            check_out_timestamp = check_out
            logger.info(f"    Using Existing Check-Out Time: {check_out_timestamp}")
        
        # Simulate check-out process
        logger.info("    Verifying cargo documentation...")
        time.sleep(0.5)
        
        logger.info("    Final vehicle inspection...")
        time.sleep(0.5)
        
        logger.info("  Truck checked out successfully!")
        
        # Calculate duration if check-in exists
        if check_in:
            try:
                checkin_dt = datetime.strptime(check_in, '%Y-%m-%d %H:%M:%S')
                checkout_dt = datetime.strptime(check_out_timestamp, '%Y-%m-%d %H:%M:%S')
                duration_minutes = (checkout_dt - checkin_dt).total_seconds() / 60
                logger.info(f"    Total Duration: {duration_minutes:.1f} minutes")
            except:
                logger.warning("    Could not calculate duration")
        
        # Update database with check-out timestamp
        success = update_transport_timestamps(
            transportation_id=transportation_id,
            check_out=check_out_timestamp
        )
        
        if success:
            logger.info(f" Database updated: check_out = {check_out_timestamp}")
        else:
            logger.warning(f" Database update may have failed for {transportation_id}")
        
        # Prepare result variables (camelCase for Camunda)
        result = {
            **variables,  # Keep all existing variables
            'checkOut': check_out_timestamp,
            'truckCheckOutStatus': 'completed',
            'truckCheckOutOperator': 'GATE-OP-002',
            'containerId': container_id,
            'transportationId': transportation_id,
            'operationType': operation_type
        }
        
        logger.info(f" TRUCK CHECK-OUT completed successfully")
        return result
        
    except ValueError as ve:
        raise
    except Exception as e:
        logger.error(f" Error during truck check-out: {e}")
        raise