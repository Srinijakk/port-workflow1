"""
Integrated Port Management System
Combines Camunda Worker and Workflow Starter in a single application.
Manages container operations with ships and trucks.
"""

import logging
import asyncio
import sys
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from pyzeebe import ZeebeWorker, ZeebeClient, create_insecure_channel, Job

# Import handlers
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

# Configuration
ZEEBE_ADDRESS = "localhost:26500"
BPMN_PROCESS_ID = "Port_Workflow"

DB_CONFIG = {
    "dbname": "portmanagement",
    "user": "postgres",
    "password": "srinija",
    "host": "localhost",
    "port": 5432
}


class PortManagementSystem:
    """Main system class that handles both worker and workflow starter functionality."""
    
    def __init__(self):
        self.worker = None
        self.client = None
        self.channel = None
    
    async def initialize_database(self):
        """Initialize database connection pool."""
        logger.info(" Initializing database connection...")
        try:
            initialize_connection_pool(min_conn=2, max_conn=10)
            
            if test_connection():
                logger.info(" Database connection verified")
                return True
            else:
                logger.error(" Database connection test failed")
                return False
        except Exception as db_error:
            logger.error(f" Database initialization failed: {db_error}")
            logger.error(" Check your database configuration in config/database.py")
            return False
    
    async def initialize_zeebe(self):
        """Initialize Zeebe connection for both worker and client."""
        logger.info(" Connecting to Zeebe...")
        try:
            self.channel = create_insecure_channel(grpc_address=ZEEBE_ADDRESS)
            logger.info(" Connected to Zeebe successfully")
            return True
        except Exception as e:
            logger.error(f" Failed to connect to Zeebe: {e}")
            return False
    
    def register_task_handlers(self):
        """Register all task handlers with the Zeebe worker."""
        logger.info(" Registering task handlers...")
        
        self.worker = ZeebeWorker(self.channel)
        
        @self.worker.task(task_type="crane_loading")
        async def crane_loading_task(job: Job):
            """Handle crane loading task"""
            process_instance_key = job.process_instance_key
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'loading')
            
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_crane_loading(job.key, job.variables)
        
        @self.worker.task(task_type="crane_unloading")
        async def crane_unloading_task(job: Job):
            """Handle crane unloading task"""
            process_instance_key = job.process_instance_key
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unloading')
            
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_crane_unloading(job.key, job.variables)
        
        @self.worker.task(task_type="weighing")
        async def weighing_task(job: Job):
            """Handle weighing task"""
            process_instance_key = job.process_instance_key
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_weighing(job.key, job.variables)
        
        @self.worker.task(task_type="storage")
        async def storage_task(job: Job):
            """Handle storage task"""
            process_instance_key = job.process_instance_key
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            result = handle_storage(job.key, job.variables)
            
            if process_instance_key:
                update_process_instance_completion(process_instance_key, "completed")
            
            return result
        
        @self.worker.task(task_type="truck_checkin")
        async def truck_checkin_task(job: Job):
            """Handle truck check-in task"""
            process_instance_key = job.process_instance_key
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_truck_checkin(job.key, job.variables)
        
        @self.worker.task(task_type="truck_checkout")
        async def truck_checkout_task(job: Job):
            """Handle truck check-out task"""
            process_instance_key = job.process_instance_key
            container_id = job.variables.get('containerId')
            transportation_id = job.variables.get('transportationId')
            operation_type = job.variables.get('operationType', 'unknown')
            
            if process_instance_key:
                insert_process_instance(
                    process_instance_key,
                    operation_type=operation_type,
                    container_id=container_id,
                    transportation_id=transportation_id
                )
            
            return handle_truck_checkout(job.key, job.variables)
        
        logger.info(" All task handlers registered")
    
    def fetch_workflow_scenarios(self) -> List[Dict]:
        """Fetch all valid workflow scenarios from database."""
        scenarios = []
        
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            query = """
                SELECT 
                    c.container_id,
                    c.operation_type,
                    c.weight,
                    t.transportation_id,
                    t.check_in,
                    t.check_out,
                    s.storage_status
                FROM transport_mean t
                JOIN container c ON t.container_id = c.container_id
                JOIN storage s ON t.storage_id = s.storage_id
                ORDER BY t.transportation_id, c.container_id;
            """
            
            cur.execute(query)
            rows = cur.fetchall()
            conn.close()
            
            for row in rows:
                transportation_id = row['transportation_id']
                
                # Validate truck timestamps
                if transportation_id.startswith('truck'):
                    if not row['check_in'] or not row['check_out']:
                        logger.warning(f" Skipping {transportation_id} - missing timestamps")
                        continue
                
                scenario = {
                    'containerId': row['container_id'],
                    'transportationId': str(row['transportation_id']),
                    'operationType': row['operation_type'],
                    'weight': float(row['weight']),
                    'storageStatus': row['storage_status']
                }
                
                # Add truck-specific timestamps
                if transportation_id.startswith('truck'):
                    scenario['checkIn'] = row['check_in'].strftime("%Y-%m-%d %H:%M:%S")
                    scenario['checkOut'] = row['check_out'].strftime("%Y-%m-%d %H:%M:%S")
                
                scenarios.append(scenario)
            
            logger.info(f" Fetched {len(scenarios)} valid scenarios from database")
            return scenarios
            
        except Exception as e:
            logger.error(f" Failed to fetch scenarios: {e}")
            return []
    
    async def start_single_workflow(self, variables: Dict) -> bool:
        """Start a single workflow instance."""
        try:
            if 'transportationId' not in variables or not variables['transportationId']:
                logger.error(f" transportationId missing for {variables.get('containerId', 'UNKNOWN')}")
                return False
            
            logger.info(f"\n{'='*60}")
            logger.info(f" Starting workflow for {variables['containerId']}")
            logger.info(f"   Transport: {variables['transportationId']}")
            logger.info(f"   Operation: {variables['operationType']}")
            
            if not self.client:
                self.client = ZeebeClient(self.channel)
            
            result = await self.client.run_process(
                bpmn_process_id=BPMN_PROCESS_ID,
                variables=variables
            )
            
            logger.info(f" Workflow started successfully!")
            logger.info(f"   Process Instance Key: {result.process_instance_key}")
            logger.info(f"{'='*60}\n")
            
            return True
            
        except Exception as e:
            logger.error(f" Failed to start workflow: {e}")
            return False
    
    async def start_workflows_parallel(self, scenarios: List[Dict]):
        """Start all workflows in parallel."""
        logger.info(f"\n Starting {len(scenarios)} workflows in parallel...\n")
        
        tasks = [self.start_single_workflow(scenario) for scenario in scenarios]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful
        
        logger.info(f"\n{'='*60}")
        logger.info(f" Workflow Execution Summary:")
        logger.info(f"   Total: {len(scenarios)}")
        logger.info(f"    Successful: {successful}")
        logger.info(f"    Failed: {failed}")
        logger.info(f"{'='*60}\n")
    
    async def start_workflows_sequential(self, scenarios: List[Dict]):
        """Start workflows one by one with delays."""
        logger.info(f"\n Starting {len(scenarios)} workflows sequentially...\n")
        
        successful = 0
        failed = 0
        
        for i, scenario in enumerate(scenarios, 1):
            logger.info(f"[{i}/{len(scenarios)}] Processing...")
            result = await self.start_single_workflow(scenario)
            
            if result:
                successful += 1
            else:
                failed += 1
            
            if i < len(scenarios):
                await asyncio.sleep(1)
        
        logger.info(f"\n{'='*60}")
        logger.info(f" Workflow Execution Summary:")
        logger.info(f"   Total: {len(scenarios)}")
        logger.info(f"    Successful: {successful}")
        logger.info(f"    Failed: {failed}")
        logger.info(f"{'='*60}\n")
    
    async def run_worker_mode(self):
        """Run in worker mode - continuously process tasks."""
        logger.info("=" * 60)
        logger.info(" Worker Mode - Started successfully!")
        logger.info(f" Connected to Zeebe at {ZEEBE_ADDRESS}")
        logger.info(f" Database: portmanagement (PostgreSQL)")
        logger.info(" Schema: process_instance, container, storage, transport_mean")
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
        await self.worker.work()
    
    async def run_starter_mode(self):
        """Run in starter mode - start workflows from database."""
        logger.info("=" * 60)
        logger.info(" Workflow Starter Mode")
        logger.info("=" * 60)
        
        # Fetch scenarios
        logger.info("\n Fetching workflow scenarios from database...")
        scenarios = self.fetch_workflow_scenarios()
        
        if not scenarios:
            logger.error(" No valid scenarios found in database!")
            return False
        
        # Show summary
        ships = sum(1 for s in scenarios if s['transportationId'].startswith('ship'))
        trucks = sum(1 for s in scenarios if s['transportationId'].startswith('truck'))
        loading = sum(1 for s in scenarios if s['operationType'] == 'loading')
        unloading = sum(1 for s in scenarios if s['operationType'] == 'unloading')
        
        logger.info(f"\n Scenario Breakdown:")
        logger.info(f"    Ships: {ships}")
        logger.info(f"    Trucks: {trucks}")
        logger.info(f"    Loading: {loading}")
        logger.info(f"    Unloading: {unloading}")
        
        # Ask for execution mode
        print("\n" + "="*60)
        print("Select execution mode:")
        print("  1. Parallel (all workflows start simultaneously)")
        print("  2. Sequential (one at a time, with delays)")
        print("="*60)
        
        choice = input("\nEnter choice (1 or 2) [default: 1]: ").strip() or "1"
        
        if choice == "2":
            await self.start_workflows_sequential(scenarios)
        else:
            await self.start_workflows_parallel(scenarios)
        
        logger.info("\n All workflows have been started!")
        logger.info(" Check Camunda Operate: http://localhost:8080/operate")
        logger.info("\n  Note: Workflows need the Python worker running to complete!")
        logger.info("   Start worker with: python port_management_system.py --worker")
        
        return True
    
    def cleanup(self):
        """Clean up resources."""
        logger.info(" Cleaning up resources...")
        close_all_connections()
        logger.info(" System stopped gracefully")


async def main():
    """Main entry point for the integrated system."""
    system = PortManagementSystem()
    
    try:
        # Show menu
        print("\n" + "="*60)
        print(" Port Management System - Integrated Application")
        print("="*60)
        print("\nSelect mode:")
        print("  1. Worker Mode (Process workflow tasks)")
        print("  2. Starter Mode (Start workflows from database)")
        print("  3. Both (Start worker, then allow workflow starting)")
        print("="*60)
        
        mode = input("\nEnter mode (1, 2, or 3) [default: 1]: ").strip() or "1"
        
        # Initialize database and Zeebe
        if not await system.initialize_database():
            logger.error(" Database initialization failed. Exiting...")
            sys.exit(1)
        
        if not await system.initialize_zeebe():
            logger.error(" Zeebe connection failed. Exiting...")
            sys.exit(1)
        
        if mode == "1":
            # Worker mode only
            system.register_task_handlers()
            await system.run_worker_mode()
            
        elif mode == "2":
            # Starter mode only
            await system.run_starter_mode()
            
        elif mode == "3":
            # Both modes
            system.register_task_handlers()
            
            # Start worker in background
            worker_task = asyncio.create_task(system.run_worker_mode())
            
            # Wait a bit for worker to start
            await asyncio.sleep(2)
            
            logger.info("\n Worker is now running in background")
            logger.info(" Starting workflows...\n")
            
            # Run starter
            await system.run_starter_mode()
            
            # Keep worker running
            logger.info("\n Workflows started. Worker continues running...")
            logger.info("Press Ctrl+C to stop\n")
            await worker_task
        else:
            logger.error(" Invalid mode selected")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("\n\n Shutdown signal received")
    except Exception as e:
        logger.error(f"\n Error: {e}")
        raise
    finally:
        system.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n System stopped by user")