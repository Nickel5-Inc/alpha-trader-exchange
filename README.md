<div align="center">

# **Alpha Trade Exchange Subnet** <!-- omit in toc -->
[![Discord Chat](https://img.shields.io/discord/308323056592486420.svg)](https://discord.gg/bittensor)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) 

---

## The Incentivized Alpha Token Trading Network <!-- omit in toc -->

[Discord](https://discord.gg/bittensor) • [Research](https://bittensor.com/whitepaper)
</div>

---
- [Introduction](#introduction)
- [How It Works](#how-it-works)
  - [For Miners](#for-miners)
  - [For Validators](#for-validators)
- [Installation](#installation)
  - [Before you proceed](#before-you-proceed)
  - [Install](#install)
- [Integration](#integration)
- [License](#license)

---

## Introduction

The Alpha Trader Exchange Subnet is designed to incentivize and promote liquidity across alpha token pools in the Bittensor network. This subnet creates a unique ecosystem where:

- Miners earn rewards by executing profitable trades between alpha tokens
- Validators track and score trading performance

The subnet's primary goal is to enhance the overall liquidity and trading efficiency of alpha tokens while providing a transparent and incentivized trading environment.

### For Miners

Miners in this subnet have a uniquely passive role:

1. Register to the subnet using their coldkey
2. Execute trades between alpha tokens on the Bittensor chain
3. Earn rewards based on trading performance

IMPORTANT INFORMATION FOR MINERS:
Miners should only have one hotkey per coldkey on the subnet. If you have multiple hotkeys, one will simply go to waste; trades are tracked by coldkeys. 

Key points for miners:
- No need to run additional software
- All trades associated with the registered coldkey are automatically tracked
- Performance is measured by ROI
- Weekly performance metrics determine rewards

### For Validators

Validators play a crucial role in maintaining the subnet's integrity:

1. Track and analyze miner trades by querying on-chain data
2. Calculate performance metrics including ROI over weekly periods
3. Assign scores to miners based on their trading performance
4. Distribute rewards according to the scoring mechanism

Key responsibilities:
- Accurate trade data collection and analysis
- Fair and transparent scoring
- Weekly performance evaluation
- Reward distribution based on trading success

## Scoring

Miners in the Alpha Trade Exchange network are rewarded based on their trading performance. The scoring system:

1. Tracks closed positions and their ROI (Return on Investment)
2. Weighs recent performance more heavily than older performance
3. Normalizes scores
4. Updates miner weights on the network based on these scores
5. Trades must be open for a minimum of 360 blocks to be considered valid trades. Trades shorter than 360 blocks will be given an ROI of 0
6. The maximum lookback period is 21 days
7. Right now only subnets 1-74 are tradeable; other pools are too illiquid. This will adjust dynamically to pool size in the future


#### Daily Returns Calculation
For each miner, the system:
- Groups positions by the date they were closed
- Calculates the total ROI for each day
- Determines how many days ago each position was closed

#### Decay
Recent performance is valued more than older performance through an exponential decay model:

Calculate the decay factor
```return_decay = np.exp(np.log(20) / 14)```  # Roughly 1.221

Calculate weight for a specific day
```weight = np.exp(-0.25 * return_decay * days_difference)```

### Practical Example 

For a miner with the following daily returns:

- Today: +5T ROI (weight: 1.0) → weighted: 5.0
- 3 days ago: +8T ROI (weight: ~0.6) → weighted: ~3.2
- 7 days ago: +12T ROI (weight: ~0.2) → weighted: ~1.44

The total weighted return would be approximately 9.46, and the average weighted return (divided by 3 days) would be approximately 3.21. This is then normalized with all other scores in the subnet.

## Integration

This subnet will soon partner with an exchange. Once we're confident things are running smoothly we will integrate with their platform.

---

## Installation

Choose the appropriate installation guide based on your role:

- **For Validators**: Follow the step-by-step instructions in [Running on Mainnet](./docs/running_on_mainnet.md)
- **For Miners**: Simply register your coldkey to the subnet and begin trading on any subnet. No additional installation required.

## License
This repository is licensed under the MIT License.
```text
# The MIT License (MIT)
# Copyright © 2024 Opentensor Foundation

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
```
