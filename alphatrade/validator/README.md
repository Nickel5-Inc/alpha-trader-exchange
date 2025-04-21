# Validator Installation

## Configuration

The API client is configured using environment variables:

```
# API URL for Taoapp
https://api.tao.app/

# API Key for Taoapp (required for authenticated endpoints)
TAOAPP_API_KEY=...
```

This should be saved to a .env file in the base directory. 

You can obtain an API key by registering at https://api.tao.app . The highest tier is required for this subnet to process all transactions. We will move to on-chain handling in the future.

The virtual environment can be set up with the following commands:

```
# Create a virtual environment
python -m venv atx

# Activate the virtual environment
source atx/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install sqlalchemy
pip install sqlalchemy

# Install PyTorch
pip install torch

# Install bittensor
pip install bittensor
```

The validator can be started with the following command:

```pm2 start "python neurons/validator.py --wallet.name <NAME> --wallet.hotkey <HOTKEY> --subtensor.network finney --netuid 63 --logging.trace"```
