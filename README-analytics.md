# CLN Node Analytics Dataset

## Overview

The `node_analytics.json.xz` file is a compressed JSON snapshot of a Core Lightning (CLN) node. It aggregates configuration, peer connection data, channel liquidity, historical routing events, and peer availability metrics into a single file.

It is designed to allow off-node analysis of **Liquidity Efficiency (APY)**, **Fee Strategy Performance**, **Peer Health (Tor vs Clearnet)**, and **Routing Reliability** without requiring direct database access.

## Generation

### Using justfile (Recommended)

Configure your `.env` file with the required variables:

```bash
NODE_SSH=user@node
AVAILDB_PATH=/home/casatta/.lightning/bitcoin/summars/availdb.json
```

Then run:

```bash
just dataset
```

This executes `scripts/fetch-dataset.sh` which SSHs to the remote node, collects data from CLN CLI commands and the summars availability database, then compresses the output.

## Data Structure

The JSON object contains five root keys. Here is how to interpret them:

### 1. `info`

**Source:** `lightning-cli getinfo`

* **Purpose:** Global node context.
* **Key Fields:**
* `id`: The local node's Pubkey.
* `blockheight`: Current chain height (used to calculate channel age).
* `version`: CLN version.



### 2. `peers`

**Source:** `lightning-cli listpeers`

* **Purpose:** Network connection details (Transport Layer).
* **Key Fields:**
* `id`: Peer Pubkey.
* `netaddr`: Array of connection strings (e.g., `["1.2.3.4:9735"]` or `["xyz.onion:9735"]`).
* *Usage:* Used to distinguish **Clearnet** vs **Tor Only** peers.


* `connected`: Boolean status.



### 3. `channels`

**Source:** `lightning-cli listpeerchannels`

* **Purpose:** The "Source of Truth" for Liquidity and Policy. It merges `listfunds` (liquidity) with `listchannels` (gossip/fee config).
* **Key Fields:**
* `short_channel_id` (SCID): Unique identifier.
* `peer_id`: The remote peer.
* `to_us_msat`: **Local Balance** (Outbound Liquidity).
* `total_msat` (or `amount_msat`): **Capacity**.
* `fee_per_millionth`: The fee rate **YOU** are charging (PPM).
* `state`: e.g., `CHANNELD_NORMAL`.
* `private`: Boolean (if the channel is unannounced).



### 4. `forwards`

**Source:** `lightning-cli listforwards`

* **Purpose:** Historical routing logs (Traffic).
* **Filter:** Only contains events newer than `START` timestamp.
* **Data Quality Note:** CLN often garbage collects *failed* forwards after a short period, but keeps *settled* forwards longer. Trust `settled` stats for long-term analysis; treat `failed` stats as "recent context only."
* **Key Fields:**
* `in_channel`: SCID of the incoming peer.
* `out_channel`: SCID of the outgoing peer.
* `fee_msat`: Profit earned.
* `status`: `settled` or `failed`.
* `received_time`: Timestamp.



### 5. `availdb`

**Source:** summars plugin `availdb.json`

* **Purpose:** Peer availability/uptime tracking.
* **Key Fields:**
* Node pubkeys as keys, with availability percentage values indicating how often a peer is online.
* *Usage:* Filter out unreliable peers or weight routing decisions by uptime.



---

## Common Analysis Logic

When analyzing this file in Python (via `lzma` + `json`), use the following logic to derive metrics:

1. **Channel Age:**
```python
open_block = int(scid.split('x')[0])
age_days = (info['blockheight'] - open_block) / 144
```


2. **Liquidity Saturation:**
```python
# > 80% means Channel is Full (Source)
# < 20% means Channel is Empty (Sink)
saturation = channel['to_us_msat'] / channel['total_msat']
```


3. **True APY (Annualized):**
```python
# Filter forwards for 'settled' status on specific 'out_channel'
revenue_1y = sum(f['fee_msat'] for f in forwards if f['out_channel'] == scid)

# If channel age < 1 year, scale revenue up
scaling = 365 / min(age_days, 365)
annualized_rev = revenue_1y * scaling

# Yield
apy_percent = (annualized_rev / channel['to_us_msat']) * 100
```


4. **Network Classification:**
* **Tor Only:** `netaddr` contains `.onion` AND does NOT contain IPv4/6.
* **Clearnet:** `netaddr` contains IPv4/6.
* **Hybrid:** Contains both.
