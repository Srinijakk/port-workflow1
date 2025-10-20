"""
Port Operations Workflow Starter - FINAL FIX
Critical Fix: Properly sends variables to Camunda process instances
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
logger = logging.getLogger(__name__)

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
            
            # Validate trucks must have timestamps
            if transportation_id.startswith('truck'):
                if not row['check_in'] or not row['check_out']:
                    logger.warning(f"⚠️  Skipping truck {transportation_id} - missing timestamps")
                    continue
            
            #  Build scenario with explicit types
            scenario = {
                'containerId': str(row['container_id']),
                'transportationId': str(row['transportation_id']),
                'operationType': str(row['operation_type']),
                'weight': int(row['weight']),
                'storageStatus': str(row['storage_status'])
            }
            
            # Add truck-specific timestamps
            if transportation_id.startswith('truck'):
                scenario['checkIn'] = row['check_in'].strftime("%Y-%m-%d %H:%M:%S")
                scenario['checkOut'] = row['check_out'].strftime("%Y-%m-%d %H:%M:%S")
            
            scenarios.append(scenario)
        
        logger.info(f" Fetched {len(scenarios)} valid scenarios")
        return scenarios
        
    except Exception as e:
        logger.error(f" Failed to fetch scenarios: {e}")
        import traceback
        traceback.print_exc()
        return []


async def start_single_workflow_v2(client: ZeebeClient, variables: Dict) -> bool:
    """
    CRITICAL FIX: Use run_process_instance with await
    This ensures variables are sent BEFORE the gateway evaluation
    """
    try:
        #  Validation
        required_vars = ['containerId', 'transportationId', 'operationType']
        for var in required_vars:
            if var not in variables or not variables[var]:
                logger.error(f" Missing: {var}")
                return False
        
        logger.info(f"\n{'='*70}")
        logger.info(f" Starting: {variables['containerId']}")
        logger.info(f"    transportationId = '{variables['transportationId']}'")
        logger.info(f"     operationType = '{variables['operationType']}'")
        
        #  CRITICAL FIX: Use run_process_instance (not run_process)
        # This method properly sends variables to the process instance
        process_instance_key = await client.run_process_instance(
            bpmn_process_id=BPMN_PROCESS_ID,
            variables=variables,
            version=-1  # Latest version
        )
        
        logger.info(f" Started! Key: {process_instance_key}")
        logger.info(f"   Variables sent: {list(variables.keys())}")
        return True
        
    except Exception as e:
        logger.error(f" Failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_mode(client: ZeebeClient):
    """Test with hardcoded scenario."""
    logger.info("\n" + "="*70)
    logger.info(" TEST MODE")
    logger.info("="*70)
    
    test_scenario = {
        'containerId': 'C1001',
        'transportationId': 'truck101',  # ← Explicit string
        'operationType': 'loading',
        'weight': 12000,
        'storageStatus': 'complete',
        'checkIn': '2025-10-08 08:00:00',
        'checkOut': '2025-10-08 10:00:00'
    }
    
    logger.info(f"\n📋 Test Variables:")
    for k, v in test_scenario.items():
        logger.info(f"   • {k:20s} = {v}")
    
    result = await start_single_workflow_v2(client, test_scenario)
    
    if result:
        logger.info("\n TEST PASSED")
        logger.info(" Now check Camunda Operate:")
        logger.info("   1. Go to http://localhost:8080/operate")
        logger.info("   2. Find the process instance")
        logger.info("   3. Click on 'Route by Transport Type' gateway")
        logger.info("   4. Check the Variables tab")
        logger.info("   5. You should see 'transportationId' = 'truck101'")
    else:
        logger.error("\n TEST FAILED")


async def production_mode(client: ZeebeClient, sequential=True):
    """Start all workflows."""
    scenarios = fetch_workflow_scenarios()
    
    if not scenarios:
        logger.error(" No scenarios found!")
        return
    
    # Summary
    ships = sum(1 for s in scenarios if 'ship' in s['transportationId'])
    trucks = sum(1 for s in scenarios if 'truck' in s['transportationId'])
    
    logger.info(f"\n Scenarios: {len(scenarios)} total")
    logger.info(f"    Ships: {ships}")
    logger.info(f"    Trucks: {trucks}")
    
    successful = 0
    failed = 0
    
    if sequential:
        logger.info("\n Starting SEQUENTIALLY...\n")
        for i, scenario in enumerate(scenarios, 1):
            logger.info(f"[{i}/{len(scenarios)}]")
            result = await start_single_workflow_v2(client, scenario)
            if result:
                successful += 1
            else:
                failed += 1
            await asyncio.sleep(0.5)
    else:
        logger.info("\n Starting in PARALLEL...\n")
        tasks = [start_single_workflow_v2(client, s) for s in scenarios]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful
    
    logger.info(f"\n{'='*70}")
    logger.info(f" Summary:")
    logger.info(f"    Success: {successful}")
    logger.info(f"    Failed: {failed}")
    logger.info(f"{'='*70}")


async def main():
    """Main execution."""
    try:
        logger.info("="*70)
        logger.info(" Port Operations Workflow Starter v3.0")
        logger.info("="*70)
        
        # Connect
        logger.info(f"\n🔗 Connecting to {ZEEBE_ADDRESS}...")
        channel = create_insecure_channel(grpc_address=ZEEBE_ADDRESS)
        client = ZeebeClient(channel)
        logger.info(" Connected!")
        
        # Menu
        print("\n" + "="*70)
        print("Select mode:")
        print("  1.  Test (single workflow - RECOMMENDED)")
        print("  2.  Production Sequential (one at a time)")
        print("  3.  Production Parallel (all at once)")
        print("="*70)
        
        choice = input("\nChoice (1/2/3) [default: 1]: ").strip() or "1"
        
        if choice == "1":
            await test_mode(client)
        elif choice == "2":
            await production_mode(client, sequential=True)
        else:
            await production_mode(client, sequential=False)
        
        logger.info("\n Done!")
        logger.info(" Check: http://localhost:8080/operate")
        logger.info("  Worker must be running: python main.py")
        
    except KeyboardInterrupt:
        logger.info("\n  Interrupted")
    except Exception as e:
        logger.error(f"\n Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())