#!/usr/bin/env python3
# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Alpha Trade Exchange Contributors

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import sys
import time
import asyncio
import argparse
import threading
import traceback
import bittensor as bt

from alphatrade.validator.validator import Validator

# Function to periodically run the main method
async def run_main_loop(validator, interval_seconds=5400):
    """
    Run the validator's main maintenance loop periodically.
    
    Args:
        validator: The validator instance
        interval_seconds: Seconds between each run (default: 5 minutes)
    """
    step = 1
    
    while True:
        try:
            bt.logging.info(f"Running validator step: {step}")
            await validator.main()
            
            # Increment step and reset if necessary
            step += 1
            if step >= 1000:
                step = 1
                
        except Exception as e:
            bt.logging.error(f"Error in main loop: {e}")
            stack_trace = traceback.format_exc()
            bt.logging.error(f"Stack Trace: {stack_trace}")
            
        # Sleep before the next run
        bt.logging.info("Sleeping for 45 minutes. This is normal behavior")
        await asyncio.sleep(interval_seconds)

def main():
    """Main entry point for the validator."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Alpha Trade Exchange validator")
    
    # Add standard Bittensor arguments
    bt.wallet.add_args(parser)
    bt.subtensor.add_args(parser)
    bt.logging.add_args(parser)
    
    # Add specific arguments for this validator
    parser.add_argument('--netuid', type=int, default=1, help="The chain subnet uid.")
    
    # Parse arguments
    config = bt.config(parser)
    
    # Enable logging
    bt.logging.info(f"Starting validator with config: {config}")
    
    # Create validator instance
    validator_instance = Validator(config)
    
    # Create and run event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run the validator in a try-except block to catch any errors
    try:
        main_task = loop.create_task(run_main_loop(validator_instance))
        
        # Keep running
        loop.run_until_complete(main_task)
        
    except KeyboardInterrupt:
        bt.logging.info("Validator stopped by keyboard interrupt")
        if 'main_task' in locals() and not main_task.done():
            main_task.cancel()
    except Exception as e:
        bt.logging.error(f"Validator failed with error: {e}")
        stack_trace = traceback.format_exc()
        bt.logging.error(f"Stack Trace: {stack_trace}")
    finally:
        # Clean up the event loop
        loop.close()
        
# Entry point
if __name__ == "__main__":
    main()