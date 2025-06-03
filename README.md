# Market Data Fetching System

A modular, scalable system for fetching and managing market data with automated scheduling, designed for easy maintenance and extensibility.

## Features

- **Modular Architecture**: Easy to switch between data sources (YFinance, Fyers, etc.)
- **Automated Scheduling**: Smart scheduling based on market hours and timeframes
- **Multiple Timeframes**: Support for 15m, 1h, 1d, and 1wk data
- **Configurable Storage**: CSV files (with database support planned)
- **Market-Aware**: Respects Indian market hours and holidays
- **CLI Interface**: Comprehensive command-line interface
- **Robust Logging**: Detailed logging with performance monitoring

## Architecture

```
screener/
├── config/           # Configuration files
├── data_sources/     # Abstract data source implementations
├── schedulers/       # Scheduling logic and timeframe handlers
├── storage/          # Storage backends (CSV, Database)
├── utils/            # Utilities (logging, market hours)
├── services/         # Main orchestration service
└── main.py          # CLI entry point
```

## Quick Start

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Check system status:
```bash
python main.py status
```

### Basic Usage

#### Fetch Data Manually
```bash
# Fetch 15-minute data for development symbols
python main.py fetch --timeframe 15m

# Fetch daily data for specific symbols
python main.py fetch --timeframe 1d --symbols RELIANCE.NS TCS.NS

# Fetch without saving (just display)
python main.py fetch --timeframe 1h --no-save
```

#### Start Automated Scheduler
```bash
# Start scheduler with development symbols (5 stocks)
python main.py scheduler start

# Start scheduler with production symbols (Nifty 50)
python main.py scheduler start --symbol-set production

# Check scheduler status
python main.py scheduler status

# Stop scheduler
python main.py scheduler stop
```

#### Data Management
```bash
# View data summary
python main.py data summary

# Load and display data
python main.py data load --timeframe 1d --head 20

# Load data for specific symbols and date range
python main.py data load --timeframe 15m --symbols RELIANCE.NS --start-date 2024-01-01

# Cleanup old backup files
python main.py data cleanup
```

#### Interactive Mode
```bash
python main.py interactive
```

#### Daemon Mode (Background Service)
```bash
python main.py daemon
```

## Configuration

### Symbol Sets

The system comes with predefined symbol sets:

- **development**: 5 major stocks for testing
- **production**: Nifty 50 stocks
- **sector_banking**: Banking sector stocks
- **sector_it**: IT sector stocks  
- **sector_auto**: Auto sector stocks

Edit `config/symbols.py` to modify or add symbol sets.

### Timeframe Settings

Configure data periods and intervals in `config/settings.py`:

```python
TIMEFRAME_CONFIGS = {
    '15m': {'period': '5d', 'interval': '15m'},
    '1h': {'period': '5d', 'interval': '1h'},
    '1d': {'period': '1y', 'interval': '1d'},
    '1wk': {'period': '2y', 'interval': '1wk'}
}
```

### Scheduling

Modify `config/schedules.py` to adjust update frequencies:

```python
SCHEDULES = {
    '15m': {'cron': '*/15 9-15 * * 1-5'},  # Every 15 min during market hours
    '1h': {'cron': '0 9-15 * * 1-5'},      # Every hour during market hours
    '1d': {'cron': '0 16 * * 1-5'},        # Daily at 4 PM
    '1wk': {'cron': '0 8 * * 6'}           # Saturday at 8 AM
}
```

## Data Sources

### YFinance (Default)
Currently active and fully implemented.

### Fyers (Planned)
Framework ready for implementation. To add Fyers support:

1. Implement the methods in `data_sources/fyers_source.py`
2. Add API credentials configuration
3. Switch data source: `--data-source fyers`

## Programmatic Usage

```python
from services.data_service import create_data_service

# Create service
service = create_data_service(
    data_source='yfinance',
    symbol_set='development',
    auto_start=False
)

# Fetch data
data = service.fetch_data('15m', symbols=['RELIANCE.NS'])

# Start scheduler
service.start_scheduler()

# Get status
status = service.get_status()
print(status)

# Cleanup when done
service.cleanup()
```

## Market Hours Awareness

The system automatically handles:

- **Market Hours**: 9:15 AM - 3:30 PM IST
- **Trading Days**: Monday to Friday (excluding holidays)
- **Holidays**: Pre-configured Indian market holidays
- **Smart Scheduling**: Updates only when appropriate

## File Organization

### Data Storage
Data is saved in the `data/` directory:
- `market_data_15m.csv` - Latest 15-minute data
- `market_data_1h.csv` - Latest 1-hour data
- `market_data_1d.csv` - Latest daily data
- `market_data_1wk.csv` - Latest weekly data
- `market_data_15m_YYYYMMDD.csv` - Daily backups

### Logging
Logs are written to `data_fetcher.log` with rotation.

## Development

### Adding a New Data Source

1. Create a new file in `data_sources/` (e.g., `alpha_vantage_source.py`)
2. Implement the `BaseDataSource` abstract class
3. Add the source to `services/data_service.py`
4. Update configuration as needed

### Adding New Timeframes

1. Add configuration to `config/settings.py`
2. Add schedule to `config/schedules.py`  
3. Optionally create custom handler in `schedulers/timeframe_handlers.py`

### Database Integration

The framework is ready for database integration:

1. Implement methods in `storage/database.py`
2. Use your existing `schema.sql`
3. Switch storage type: `storage_type='database'`

## Monitoring and Maintenance

### Health Checks
```bash
# System status
python main.py status --json

# Data freshness
python main.py data summary

# Scheduler status
python main.py scheduler status
```

### Troubleshooting

1. **Data Source Issues**: Check `python main.py status` for availability
2. **Scheduling Problems**: Verify market hours and holidays in logs
3. **Storage Issues**: Check disk space and permissions
4. **Performance**: Monitor logs for timing information

### Performance Optimization

- Adjust `RATE_LIMIT_DELAY` in settings for API rate limiting
- Use appropriate symbol sets (development vs production)
- Monitor and cleanup old backup files regularly

## Production Deployment

### Systemd Service (Linux)
```ini
[Unit]
Description=Market Data Fetcher
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/screener
ExecStart=/path/to/python main.py daemon --symbol-set production
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Cron Job Alternative
```cron
# Manual scheduling via cron (if not using built-in scheduler)
*/15 9-15 * * 1-5 cd /path/to/screener && python main.py scheduler update --timeframe 15m
0 16 * * 1-5 cd /path/to/screener && python main.py scheduler update --timeframe 1d
```

## License

This project is provided as-is for educational and personal use.

## Contributing

To contribute:
1. Fork the repository
2. Create a feature branch
3. Implement changes with proper tests
4. Submit a pull request

---

For questions or issues, please check the logs and status first, then consult the troubleshooting section. 