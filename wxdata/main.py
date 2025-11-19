"""
Main Entry Point
CSV to Iceberg Pipeline for Watsonx.data
"""

import argparse
import sys
from src.orchestrator import PipelineOrchestrator
import logging

logger = logging.getLogger(__name__)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="CSV to Iceberg Pipeline for Watsonx.data"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["once", "continuous"],
        default="once",
        help="Run mode: 'once' for single run, 'continuous' for continuous processing (default: once)"
    )
    
    args = parser.parse_args()
    
    try:
        orchestrator = PipelineOrchestrator(config_path=args.config)
        
        if args.mode == "continuous":
            orchestrator.run_continuous()
        else:
            result = orchestrator.run_once()
            print(f"\nProcessing complete:")
            print(f"  Total files: {result['total']}")
            print(f"  Processed: {result['processed']}")
            print(f"  Failed: {result['failed']}")
            
            sys.exit(0 if result['failed'] == 0 else 1)
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()




