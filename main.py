"""
Camunda 8 External Task Worker for Port Operations Workflow
Enhanced with better process instance tracking and variable handling.
Includes truck check-in/check-out handlers.
"""

import logging
import asyncio
from pyzeebe import ZeebeWorker, create_insecure_channel, Job
from handlers.crane_operations import handle_crane_loading, handle_crane_unloading
from handlers.weighing_operations import handle_weighing
from handlers.storage_operations import handle_storage
from handlers.truck_operations import handle_truck_checkin, handle_truck_checkout
from config.database import (
    initialize_connection_pool, 
    test_connection, 
    close_all_connections,
    insert_process_instance,
    update_process_instance_completion
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Camunda 8 connection settings
ZEEBE_ADDRESS = "localhost:26500"

async def main():
    """Initialize and start the Zeebe worker with database connection."""
    try:
        # Initialize PostgreSQL connection pool
        logger.info("🔗 Initializing database connection...")
        try:
            initialize_connection_pool(min_conn=2, max_conn=10)
            
            # Test database connection
            if test_connection():
                logger.info("Database connection verified")
            else:
                logger.error("Database connection test failed")
                logger.error("Worker will continue but database writes may fail")
        except Exception as db_error:
            logger.error(f"Database initialization failed: {db_error}")
            logger.error(" Worker will continue but database writes will fail")
            logger.error(" Check your database configuration in config/database.py")
        
        # Create insecure channel for local c8run.exe
        logger.info("🔗 Connecting to Zeebe...")
        channel = create_insecure_channel(grpc_address=ZEEBE_ADDRESS)
        
        # Create worker instance
        worker = ZeebeWorker(channel)
        
        # Register task handlers using decorators
        logger.info("Registering task handlers...")
        
        @worker.task(task_type="crane_loading")
        async def crane_loading_task(job: Job):
            """Handle crane loading task"""
            process_instance_key = job.process_instance_key
            
            # Extract variables (camelCase from Camunda)
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'loading')
            
            # Record process instance with details
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_crane_loading(job.key, job.variables)
        
        @worker.task(task_type="crane_unloading")
        async def crane_unloading_task(job: Job):
            """Handle crane unloading task"""
            process_instance_key = job.process_instance_key
            
            # Extract variables (camelCase from Camunda)
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unloading')
            
            # Record process instance with details
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_crane_unloading(job.key, job.variables)
        
        @worker.task(task_type="weighing")
        async def weighing_task(job: Job):
            """Handle weighing task"""
            process_instance_key = job.process_instance_key
            
            # Extract variables
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            # Update process instance if needed
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_weighing(job.key, job.variables)
        
        @worker.task(task_type="storage")
        async def storage_task(job: Job):
            """Handle storage task"""
            process_instance_key = job.process_instance_key
            
            # Extract variables
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            # Update process instance
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            # Execute storage task
            result = handle_storage(job.key, job.variables)
            
            # Mark process as completed after storage
            if process_instance_key:
                update_process_instance_completion(process_instance_key, "completed")
            
            return result
        
        @worker.task(task_type="truck_checkin")
        async def truck_checkin_task(job: Job):
            """Handle truck check-in task"""
            process_instance_key = job.process_instance_key
            
            # Extract variables
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            # Update process instance
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_truck_checkin(job.key, job.variables)
        
        @worker.task(task_type="truck_checkout")
        async def truck_checkout_task(job: Job):
            """Handle truck check-out task"""
            process_instance_key = job.process_instance_key
            
            # Extract variables
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            # Update process instance
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_truck_checkout(job.key, job.variables)
        
        logger.info("=" * 60)
        logger.info(" Worker started successfully!")
        logger.info(f"Connected to Zeebe at {ZEEBE_ADDRESS}")
        logger.info(f" Database: portmanagement (PostgreSQL)")
        logger.info("Schema: process_instance, container, storage, transport_mean")
        logger.info(" Polling for tasks:")
        logger.info("   • crane_loading")
        logger.info("   • crane_unloading")
        logger.info("   • weighing")
        logger.info("   • storage")
        logger.info("   • truck_checkin")
        logger.info("   • truck_checkout")
        logger.info("=" * 60)
        logger.info("")
        logger.info("  IMPORTANT: Start workflows WITH variables!")
        logger.info("   Example: {\"containerId\": \"C1002\", \"transportationId\": \"ship9109\", \"operationType\": \"unloading\"}")
        logger.info("")
        logger.info("Press Ctrl+C to stop")
        logger.info("")
        
        # Keep the worker running
        await worker.work()
        
    except KeyboardInterrupt:
        logger.info("\nShutdown signal received")
    except Exception as e:
        logger.error(f" Error starting worker: {e}")
        raise
    finally:
        # Cleanup
        logger.info(" Cleaning up resources...")
        close_all_connections()
        logger.info(" Worker stopped gracefully")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n Worker stopped by user")