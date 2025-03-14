import bittensor as bt
from typing import Tuple
from alphatrade.base.miner import BaseMinerNeuron
from alphatrade.protocol import ATXSynapse


class ATXMiner(BaseMinerNeuron):

    def __init__(self, hotkey: str, config=None):
        super(ATXMiner, self).__init__(config)
        self.hotkey: str = hotkey

    def forward(self, synapse: ATXSynapse) -> ATXSynapse:
        """
        Perform the forward logic for the neuron
        Args:
            synapse: Synapse object received from network

        Returns:
            Synapse object updated by reference
        """
        synapse.message.miner_hotkey = self.hotkey
        return synapse

    def priority(self, synapse: ATXSynapse) -> float:
        """
        Calculate synapse priority
        Args:
            synapse: Synapse object received from network

        Returns:
            The stake of the validator who sent the synapse
        """
        bt.logging.debug(f"🧮 Calculating priority for synapse from {synapse.dendrite.hotkey}")
        stake, uid = self.get_validator_stake_and_uid(synapse.dendrite.hotkey)  # Extract Stake & UID
        bt.logging.debug(f"🏆 Prioritized: {synapse.dendrite.hotkey} (UID: {uid} - Stake: {stake})")
        return stake

    def get_validator_stake_and_uid(self, hotkey) -> Tuple[float, int]:
        """
        Get stake and uid from the metagraph using the validator's hotkey
        Args:
            hotkey: Hotkey of the validator

        Returns:
            Stake (float) and UID (int)
        """
        uid = self.metagraph.hotkeys.index(hotkey)  # Get uid
        return float(self.metagraph.S[uid]), uid  # Return validator stake

    def blacklist(self, synapse: ATXSynapse) -> Tuple[bool, str]:
        """
        Blacklist malicious validators
        Args:
            synapse: Synapse object received from network

        Returns:
            Whether or not the blacklist the  validator, and am accompanying message
        """
        # Check if synapse hotkey is in the metagraph
        if synapse.dendrite.hotkey not in self.metagraph.hotkeys:
            return True, f"❗Hotkey {synapse.dendrite.hotkey} was not found from metagraph.hotkeys",

        stake, uid = self.get_validator_stake_and_uid(synapse.dendrite.hotkey)

        # Check if validator has sufficient stake
        validator_min_stake = 0.0
        if stake < validator_min_stake:
            return True, f"❗Hotkey {synapse.dendrite.hotkey} has insufficient stake: {stake} (UID: {uid})",

        # Valid hotkey
        return False, f"✅ Accepted hotkey: {synapse.dendrite.hotkey}"
