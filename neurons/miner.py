import argparse
from argparse import ArgumentParser
import bittensor as bt
from alphatrade.miner.atx_miner import ATXMiner


def build_argument_parser() -> ArgumentParser:
    """
    Build the argument parser object
    Returns:
        Instance of the ArgumentParser class
    """
    parser: ArgumentParser = ArgumentParser()  # Create arg parser
    bt.wallet.add_args(parser)  # Add wallet args
    bt.subtensor.add_args(parser)  # Add subtensor args
    bt.logging.add_args(parser)  # Add logging args
    bt.axon.add_args(parser)  # Add axon args
    parser.add_argument("--blacklist.force_validator_permit", action="store_true", default=True)  # Enforce validator permit
    return parser


def main():
    parser: ArgumentParser = build_argument_parser()  # Get arg parser
    config: bt.config = bt.config(parser)  # Build config
    args: argparse.Namespace = parser.parse_args()  # Parse args
    hotkey: str = args.hotkey  # Extract miner's hotkey
    atx_miner: ATXMiner = ATXMiner(hotkey=hotkey, config=config)  # Instantiate ATXMiner class
    bt.logging.trace(f"⛏️Miner created, beginning iteration 🚀")
    atx_miner.run()  # Run the miner


if __name__ == "__main__":
    main()
