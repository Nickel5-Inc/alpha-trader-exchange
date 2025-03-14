# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import bittensor as bt
from pydantic import BaseModel, Field
from typing import Optional


class ATXMessage(BaseModel):
    hotkey: Optional[str] = Field(None, description="The hotkey of the Miner")
    # ...other fields


class ATXSynapse(bt.Synapse):
    """Synapse for Alpha Trade Exchange subnet."""
    message: ATXMessage

    @classmethod
    def create(cls, message: ATXMessage = None):
        return cls(message=message)

    def deserialize(self):
        return self.message


class Dummy(bt.Synapse):
    """Dummy synapse for testing."""
    dummy_input: int = Field(0, description="Dummy input for testing")

    def deserialize(self):
        return self.dummy_input
