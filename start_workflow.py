"""
Enhanced Workflow Starter - FIXED for Variable Passing
Ensures transportationId is always present for gateway routing
"""

import asyncio
import sys
import logging
from typing import List, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from pyzeebe import ZeebeClient, create_insecure_channel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(_name_)

# Configuration
BPMN_PROCESS_ID = "Port_Workflow"
ZEEBE_ADDRESS = "localhost:26500"

DB_CONFIG = {
    "dbname": "portmanagement",
    "user": "postgres",
    "password": "srinija",
    "host": "localhost",
    "port": 5432
}


def fetch_workflow_scenarios() -> List[Dict]:
    """
    Fetch all valid workflow scenarios from database.
    CRITICAL: Ensures transportationId is always present!
    """
    scenarios = []
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Fetch all transport records with container details
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
            
            if transportation_id.startswith('truck'):
                if not row['check_in'] or not row['check_out']:
                    logger.warning(f" Skipping truck {transportation_id} - missing timestamps")
                    continue
            
            #  CRITICAL FIX: Build scenario with ALL required variables
            scenario = {
                'containerId': row['container_id'],
                'transportationId': str(row['transportation_id']),  #  Ensure string type
                'operationType': row['operation_type'],
                'weight': float(row['weight']),  # Ensure numeric type
                'storageStatus': row['storage_status']
            }
            
            # Add truck-specific timestamps
            if transportation_id.startswith('truck'):
                scenario['checkIn'] = row['check_in'].strftime("%Y-%m-%d %H:%M:%S")
                scenario['checkOut'] = row['check_out'].strftime("%Y-%m-%d %H:%M:%S")
            
            # VALIDATION: Log the transportationId to verify
            logger.debug(f"Scenario prepared: {scenario['containerId']} → {scenario['transportationId']}")
            
            scenarios.append(scenario)
        
        
        logger.info(f" Fetched {len(scenarios)} valid scenarios from database")
        # return scenarios
        print("Scenario:", scenarios[0])
        return [scenarios[0]]
        
    except Exception as e:
        logger.error(f" Failed to fetch scenarios: {e}")
        return []


async def start_single_workflow(client: ZeebeClient, variables: Dict) -> bool:
    """
    Start a single workflow instance with given variables.
    CRITICAL: Validates transportationId before sending!
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # ✅ PRE-VALIDATION: Check critical variable
        if 'transportationId' not in variables or not variables['transportationId']:
            logger.error(f"❌ CRITICAL: transportationId missing for {variables.get('containerId', 'UNKNOWN')}")
            return False
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 Starting workflow for {variables['containerId']}")
        logger.info(f"   Transport: {variables['transportationId']}")
        logger.info(f"   Operation: {variables['operationType']}")
        logger.info(f"   📦 Variables being sent:")
        for key, value in variables.items():
            logger.info(f"      • {key}: {value}")
        
        result = await client.run_process(
            bpmn_process_id=BPMN_PROCESS_ID,
            variables=variables
        )
        
        logger.info(f"✅ Workflow started successfully!")
        logger.info(f"   Process Instance Key: {result.process_instance_key}")
        logger.info(f"   BPMN Process ID: {result.bpmn_process_id}")
        logger.info(f"{'='*60}\n")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to start workflow for {variables.get('containerId', 'UNKNOWN')}: {e}")
        return False


async def start_all_workflows_parallel(client: ZeebeClient, scenarios: List[Dict]):
    """
    Start all workflows in parallel and wait for all to complete.
    """
    logger.info(f"\n Starting {len(scenarios)} workflows in parallel...\n")
    
    # Create tasks for all workflows
    tasks = [
        start_single_workflow(client, scenario)
        for scenario in scenarios
    ]
    
    # Execute all in parallel
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Count successes
    successful = sum(1 for r in results if r is True)
    failed = len(results) - successful
    
    logger.info(f"\n{'='*60}")
    logger.info(f" Workflow Execution Summary:")
    logger.info(f"   Total: {len(scenarios)}")
    logger.info(f"    Successful: {successful}")
    logger.info(f"    Failed: {failed}")
    logger.info(f"{'='*60}\n")


async def start_all_workflows_sequential(client: ZeebeClient, scenarios: List[Dict]):
    """
    Start workflows one by one with delays (for debugging).
    """
    logger.info(f"\n Starting {len(scenarios)} workflows sequentially...\n")
    
    successful = 0
    failed = 0
    
    for i, scenario in enumerate(scenarios, 1):
        logger.info(f"[{i}/{len(scenarios)}] Processing...")
        result = await start_single_workflow(client, scenario)
        
        if result:
            successful += 1
        else:
            failed += 1
        
        # Small delay between workflows
        if i < len(scenarios):
            await asyncio.sleep(1)
    
    logger.info(f"\n{'='*60}")
    logger.info(f" Workflow Execution Summary:")
    logger.info(f"   Total: {len(scenarios)}")
    logger.info(f"    Successful: {successful}")
    logger.info(f"    Failed: {failed}")
    logger.info(f"{'='*60}\n")


async def test_single_workflow(client: ZeebeClient):
    """
    Test with a single hardcoded workflow to verify variable passing.
    """
    logger.info("\n TEST MODE: Starting single test workflow\n")
    
    test_scenario = {
        'containerId': 'C1001',
        'transportationId': 'truck101',  #  Explicitly set
        'operationType': 'loading',
        'weight': 12000,
        'storageStatus': 'complete',
        'checkIn': '2025-10-08 08:00:00',
        'checkOut': '2025-10-08 10:00:00'
    }
    
    result = await start_single_workflow(client, test_scenario)
    
    if result:
        logger.info("\n TEST PASSED: Workflow started successfully!")
    else:
        logger.error("\n TEST FAILED: Could not start workflow!")


async def main():
    """
    Main execution function.
    """
    try:
        logger.info("="*60)
        logger.info(" Port Operations Workflow Starter (FIXED)")
        logger.info("="*60)
        
        # Connect to Camunda
        logger.info(f"\n🔗 Connecting to Camunda at {ZEEBE_ADDRESS}...")
        channel = create_insecure_channel(grpc_address=ZEEBE_ADDRESS)
        client = ZeebeClient(channel)
        logger.info(" Connected to Camunda successfully!")
        
        # Ask user for mode
        print("\n" + "="*60)
        print("Select execution mode:")
        print("  1. Production (all workflows from database)")
        print("  2. Test (single hardcoded workflow)")
        print("="*60)
        
        choice = input("\nEnter choice (1 or 2) [default: 1]: ").strip() or "1"
        
        if choice == "2":
            # Test mode
            await test_single_workflow(client)
        else:
            # Production mode
            # Fetch scenarios from database
            logger.info("\n Fetching workflow scenarios from database...")
            scenarios = fetch_workflow_scenarios()
            
            if not scenarios:
                logger.error(" No valid scenarios found in database!")
                sys.exit(1)
            
            # Show summary
            ships = sum(1 for s in scenarios if s['transportationId'].startswith('ship'))
            trucks = sum(1 for s in scenarios if s['transportationId'].startswith('truck'))
            loading = sum(1 for s in scenarios if s['operationType'] == 'loading')
            unloading = sum(1 for s in scenarios if s['operationType'] == 'unloading')
            
            logger.info(f"\nScenario Breakdown:")
            logger.info(f"    Ships: {ships}")
            logger.info(f"    Trucks: {trucks}")
            logger.info(f"    Loading: {loading}")
            logger.info(f"    Unloading: {unloading}")
            
            # Ask for parallel or sequential
            print("\n" + "="*60)
            print("Select execution mode:")
            print("  1. Parallel (all workflows start simultaneously)")
            print("  2. Sequential (one at a time, with delays)")
            print("="*60)
            
            exec_choice = input("\nEnter choice (1 or 2) [default: 1]: ").strip() or "1"
            
            if exec_choice == "2":
                await start_all_workflows_sequential(client, scenarios)
            else:
                await start_all_workflows_parallel(client, scenarios)
        
        logger.info("\n All workflows have been started!")
        logger.info(" Check Camunda Operate: http://localhost:8080/operate")
        logger.info("\n  Note: Workflows need the Python worker running to complete!")
        logger.info("   Start worker with: python main.py")
        
    except KeyboardInterrupt:
        logger.info("\n Interrupted by user")
    except Exception as e:
        logger.error(f"\n Error: {e}")
        sys.exit(1)


if _name_ == "_main_":
    asyncio.run(main())