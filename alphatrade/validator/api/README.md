# Validator Installation

## Configuration

The API client is configured using environment variables:

```
# API URL for TaoStats
TAOSTATS_API_URL=https://api.taostats.io

# API Key for TaoStats (required for authenticated endpoints)
# Format: tao-{uuid}:{secret}
TAOSTATS_API_KEY=tao-44658c1e-7dce-41ef-ac6b-15942665440e:ef1cc958
```

This should be saved to a .env file in the base directory. 

You can obtain an API key by registering at https://taostats.io. The highest tier is required for this subnet to process all transactions. We will move to on-chain handling in the future.
