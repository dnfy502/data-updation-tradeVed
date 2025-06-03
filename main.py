#!/usr/bin/env python3
"""
Main entry point for the Market Data Fetching System
"""
import argparse
import sys
import json
import signal
from datetime import datetime
from typing import Optional

from services.data_service import create_data_service, DataService
from utils.logging_config import setup_logging, get_logger
from utils.market_hours import get_market_status_now, is_market_open_now
from config.symbols import get_symbols

# Global service instance for signal handling
service_instance: Optional[DataService] = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger = get_logger(__name__)
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    
    if service_instance:
        service_instance.cleanup()
    
    sys.exit(0)

def main():
    """Main function with CLI interface"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description='Market Data Fetching System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s status                                    # Show system status
  %(prog)s fetch --timeframe 15m                    # Fetch 15-minute data
  %(prog)s fetch --timeframe 1d --symbols RELIANCE.NS TCS.NS
  %(prog)s scheduler start --symbol-set production  # Start scheduler with production symbols
  %(prog)s scheduler stop                           # Stop scheduler
  %(prog)s data summary                            # Show data summary
  %(prog)s data load --timeframe 1h                # Load 1-hour data
        """
    )
    
    # Global options
    parser.add_argument('--data-source', choices=['yfinance', 'fyers'], 
                       help='Data source to use (default: from config)')
    parser.add_argument('--symbol-set', default='development',
                       choices=['development', 'production', 'sector_banking', 'sector_it', 'sector_auto'],
                       help='Symbol set to use (default: development)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    status_parser.add_argument('--json', action='store_true', help='Output as JSON')
    
    # Fetch command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch market data')
    fetch_parser.add_argument('--timeframe', required=True,
                             choices=['15m', '1h', '1d', '1wk'],
                             help='Timeframe to fetch')
    fetch_parser.add_argument('--symbols', nargs='+',
                             help='Specific symbols to fetch (default: all from symbol set)')
    fetch_parser.add_argument('--no-save', action='store_true',
                             help='Don\'t save data to storage')
    
    # Scheduler commands
    scheduler_parser = subparsers.add_parser('scheduler', help='Scheduler management')
    scheduler_subparsers = scheduler_parser.add_subparsers(dest='scheduler_action')
    
    start_parser = scheduler_subparsers.add_parser('start', help='Start scheduler')
    stop_parser = scheduler_subparsers.add_parser('stop', help='Stop scheduler')
    status_parser_sched = scheduler_subparsers.add_parser('status', help='Scheduler status')
    update_parser = scheduler_subparsers.add_parser('update', help='Manual update')
    update_parser.add_argument('--timeframe', choices=['15m', '1h', '1d', '1wk'],
                              help='Specific timeframe to update')
    
    # Data commands
    data_parser = subparsers.add_parser('data', help='Data management')
    data_subparsers = data_parser.add_subparsers(dest='data_action')
    
    summary_parser = data_subparsers.add_parser('summary', help='Show data summary')
    load_parser = data_subparsers.add_parser('load', help='Load and display data')
    load_parser.add_argument('--timeframe', required=True,
                            choices=['15m', '1h', '1d', '1wk'],
                            help='Timeframe to load')
    load_parser.add_argument('--symbols', nargs='+',
                            help='Specific symbols to load')
    load_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    load_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    load_parser.add_argument('--head', type=int, default=10,
                            help='Number of rows to display (default: 10)')
    
    cleanup_parser = data_subparsers.add_parser('cleanup', help='Cleanup old files')
    
    # Interactive mode
    interactive_parser = subparsers.add_parser('interactive', help='Start interactive mode')
    daemon_parser = subparsers.add_parser('daemon', help='Run as daemon with scheduler')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    log_level = 'DEBUG' if args.verbose else 'INFO'
    setup_logging(log_level=log_level)
    logger = get_logger(__name__)
    
    # Create service instance
    global service_instance
    try:
        service_instance = create_data_service(
            data_source=args.data_source,
            symbol_set=args.symbol_set,
            auto_start=False
        )
        logger.info("Service initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize service: {str(e)}")
        return 1
    
    # Execute command
    try:
        if args.command == 'status':
            return cmd_status(service_instance, args)
        elif args.command == 'fetch':
            return cmd_fetch(service_instance, args)
        elif args.command == 'scheduler':
            return cmd_scheduler(service_instance, args)
        elif args.command == 'data':
            return cmd_data(service_instance, args)
        elif args.command == 'interactive':
            return cmd_interactive(service_instance)
        elif args.command == 'daemon':
            return cmd_daemon(service_instance)
        else:
            parser.print_help()
            return 0
            
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 0
    except Exception as e:
        logger.error(f"Command failed: {str(e)}")
        return 1
    finally:
        if service_instance:
            service_instance.cleanup()

def cmd_status(service: DataService, args) -> int:
    """Handle status command"""
    status = service.get_status()
    
    if args.json:
        print(json.dumps(status, indent=2, default=str))
    else:
        print("\n=== Market Data Fetching System Status ===")
        print(f"Data Source: {status['service']['data_source']}")
        print(f"Symbol Set: {status['service']['symbol_set']} ({status['service']['symbols_count']} symbols)")
        print(f"Storage: {status['service']['storage_type']}")
        print(f"Data Source Available: {status['service']['data_source_available']}")
        
        print(f"\nMarket Status: {status['market']['status']}")
        print(f"Market Open: {status['market']['is_open']}")
        print(f"Next Open: {status['market']['next_open']}")
        
        print(f"\nScheduler Running: {status['scheduler'].get('is_running', False)}")
        if status['scheduler'].get('scheduled_jobs'):
            print("Scheduled Jobs:")
            for job in status['scheduler']['scheduled_jobs']:
                print(f"  - {job['name']}: {job.get('next_run', 'Not scheduled')}")
    
    return 0

def cmd_fetch(service: DataService, args) -> int:
    """Handle fetch command"""
    logger = get_logger(__name__)
    
    save_data = not args.no_save
    symbols = args.symbols if args.symbols else None
    
    logger.info(f"Fetching {args.timeframe} data...")
    
    data = service.fetch_data(
        timeframe=args.timeframe,
        symbols=symbols,
        save_data=save_data
    )
    
    if data.empty:
        print(f"No data retrieved for {args.timeframe}")
        return 1
    
    print(f"\nFetched {len(data)} records for {args.timeframe}")
    print(f"Symbols: {sorted(data['Symbol'].unique()) if 'Symbol' in data.columns else 'N/A'}")
    print(f"Date range: {data['Datetime'].min()} to {data['Datetime'].max()}" if 'Datetime' in data.columns else "")
    
    if save_data:
        print(f"Data saved to storage")
    
    # Show sample data
    print(f"\nSample data (first 5 rows):")
    print(data.head().to_string(index=False))
    
    return 0

def cmd_scheduler(service: DataService, args) -> int:
    """Handle scheduler commands"""
    logger = get_logger(__name__)
    
    if args.scheduler_action == 'start':
        success = service.start_scheduler()
        if success:
            print("Scheduler started successfully")
            print("Use 'scheduler status' to monitor progress")
            return 0
        else:
            print("Failed to start scheduler")
            return 1
            
    elif args.scheduler_action == 'stop':
        service.stop_scheduler()
        print("Scheduler stopped")
        return 0
        
    elif args.scheduler_action == 'status':
        status = service.get_status()['scheduler']
        print(f"Scheduler Running: {status.get('is_running', False)}")
        
        if status.get('scheduled_jobs'):
            print("\nScheduled Jobs:")
            for job in status['scheduled_jobs']:
                print(f"  {job['name']}: {job.get('next_run', 'Not scheduled')}")
        
        if status.get('last_updates'):
            print("\nLast Updates:")
            for timeframe, update_time in status['last_updates'].items():
                print(f"  {timeframe}: {update_time}")
        
        return 0
        
    elif args.scheduler_action == 'update':
        print("Triggering manual update...")
        success = service.manual_update(timeframe=args.timeframe)
        
        if success:
            print("Manual update completed successfully")
            return 0
        else:
            print("Manual update failed")
            return 1
    
    return 0

def cmd_data(service: DataService, args) -> int:
    """Handle data commands"""
    if args.data_action == 'summary':
        summary = service.get_data_summary()
        
        print("\n=== Data Summary ===")
        for timeframe, info in summary.items():
            if 'error' in info:
                print(f"{timeframe}: Error - {info['error']}")
            else:
                print(f"{timeframe}:")
                print(f"  Records: {info['records']}")
                print(f"  Symbols: {info['symbols']}")
                if info['date_range']:
                    print(f"  Date Range: {info['date_range']['start']} to {info['date_range']['end']}")
                print(f"  Last Update: {info['last_update']}")
                print()
        
        return 0
        
    elif args.data_action == 'load':
        data = service.load_data(
            timeframe=args.timeframe,
            symbols=args.symbols,
            start_date=args.start_date,
            end_date=args.end_date
        )
        
        if data.empty:
            print(f"No data found for {args.timeframe}")
            return 1
        
        print(f"\nLoaded {len(data)} records for {args.timeframe}")
        print(f"Displaying first {args.head} rows:\n")
        print(data.head(args.head).to_string(index=False))
        
        return 0
        
    elif args.data_action == 'cleanup':
        print("Cleaning up old files...")
        service.storage_manager.cleanup_old_files()
        print("Cleanup completed")
        return 0
    
    return 0

def cmd_interactive(service: DataService) -> int:
    """Start interactive mode"""
    print("\n=== Interactive Market Data System ===")
    print("Type 'help' for available commands or 'quit' to exit")
    
    while True:
        try:
            command = input("\n> ").strip().lower()
            
            if command in ['quit', 'exit', 'q']:
                break
            elif command == 'help':
                print_interactive_help()
            elif command == 'status':
                cmd_status(service, type('Args', (), {'json': False})())
            elif command.startswith('fetch'):
                # Simple fetch command parsing
                parts = command.split()
                if len(parts) >= 2 and parts[1] in ['15m', '1h', '1d', '1wk']:
                    args = type('Args', (), {
                        'timeframe': parts[1],
                        'symbols': None,
                        'no_save': False
                    })()
                    cmd_fetch(service, args)
                else:
                    print("Usage: fetch [15m|1h|1d|1wk]")
            elif command == 'summary':
                cmd_data(service, type('Args', (), {'data_action': 'summary'})())
            elif command == 'market':
                print(f"Market Status: {get_market_status_now()}")
                print(f"Market Open: {is_market_open_now()}")
            else:
                print(f"Unknown command: {command}")
                print("Type 'help' for available commands")
                
        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            print(f"Error: {e}")
    
    print("\nGoodbye!")
    return 0

def cmd_daemon(service: DataService) -> int:
    """Run as daemon with scheduler"""
    logger = get_logger(__name__)
    
    logger.info("Starting in daemon mode...")
    
    # Start scheduler
    success = service.start_scheduler()
    if not success:
        logger.error("Failed to start scheduler")
        return 1
    
    logger.info("Daemon started successfully. Press Ctrl+C to stop.")
    
    try:
        # Keep the daemon running
        while True:
            import time
            time.sleep(60)  # Check every minute
            
            # You could add health checks or other monitoring here
            
    except KeyboardInterrupt:
        logger.info("Daemon shutdown requested")
        service.stop_scheduler()
        logger.info("Daemon stopped")
    
    return 0

def print_interactive_help():
    """Print help for interactive mode"""
    print("""
Available commands:
  status          - Show system status
  fetch [tf]      - Fetch data for timeframe (15m, 1h, 1d, 1wk)
  summary         - Show data summary
  market          - Show market status
  help            - Show this help
  quit/exit/q     - Exit interactive mode
    """)

if __name__ == '__main__':
    sys.exit(main()) 