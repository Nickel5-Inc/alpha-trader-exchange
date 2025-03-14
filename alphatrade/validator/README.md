# Validator Installation

## Configuration

The API client is configured using environment variables:

```
# API URL for TaoStats
TAOSTATS_API_URL=https://api.taostats.io

# API Key for TaoStats (required for authenticated endpoints)
# Format: tao-{uuid}:{secret}
TAOSTATS_API_KEY=...
```

This should be saved to a .env file in the base directory. 

You can obtain an API key by registering at https://taostats.io. The highest tier is required for this subnet to process all transactions. We will move to on-chain handling in the future.

The validator can be started with the following command:

pm2 start "python neurons/validator.py --wallet.name <NAME> --wallet.hotkey <HOTKEY> --subtensor.network finney --netuid 63 --logging.trace"