# CONTRACT SCANNER 

- CONTRACT SCANNER is the data pipeline behind Defi Data [Contract Scanner](https://www.defidata.dev/contracts)
 - Code licensed under GNU3
 - Built with Python3 & PostgreSQL on Ubuntu Server 20.04

## Prerequisites

- ### Ethereum RPC

    In order to use this software you will need an Ethereum Node RPC endpoint connection. The easiest way to obtain one is using an RPC service such as [Infura](https://infura.io/) & [Alchemy](https://www.alchemy.com/). If you would like to run your own, a good place to start is [here](https://ethereum.org/en/developers/tutorials/run-light-node-geth/).

- ### PostgreSQL

## Quick Start

Firstly update the .env.start file with your RPC & PostgreSQL credentials and save it as .env. You will need to Uncomment line 121 (*executeSql(txdatacontracts*) the first time you run the script to create the main table. Then to start streaming data from your node run:

```
python main.py
```

The **main.py** script scans continuously for the latest block and when it finds a new one (every ~13 seconds) it decodes the data and processes all the transactions within that block. The data is then pushed to a PostgreSQL db to be processed later. 


In order to aggregate the data and see the top 20 contracts by total ETH & total transactions can run:
```
python aggregate_stats.py
```

The **aggregate_stats.py** script takes a simple count of the transaction and sums the ETH volume for any new contracts deployed in the last rolling 7 day period.

## Notes

- You may find [tmux](https://github.com/tmux/tmux/wiki/Installing) useful to run processes robustly in the terminal.
- **aggregate_stats.py** can be scheduled as often as you need the data to be updated. 






