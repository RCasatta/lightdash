# Channel Capacity Expansion and Shrink Analysis

This document records the initial capacity-allocation analysis for the Lightdash
snapshot generated at `2026-07-17T07:55:38Z`. It is also intended as the
starting context for future analyses of a newer snapshot.

## Executive conclusion

The current evidence supports expanding a small set of liquidity-constrained,
profitable channels. It does **not** support closing or shrinking any channel
immediately.

The earlier idea of treating public channel capacity as this node's capital was
incorrect. Public channel capacity is the sum of the two sides of the channel:
our local/outbound liquidity plus the peer's remote/inbound liquidity. Only the
local balance is currently owed to this node. Remote balance is not recoverable
capital, and closing a mostly remote channel does not fund another channel.
Closing may also destroy useful inbound liquidity and the incoming side of
profitable routes.

The normal evaluation period is one year because Lightdash's dynamic fee search
can take time to find demand at the appropriate price. Channels younger than a
year should normally remain open unless there is a stronger operational reason
to act, such as persistent unavailability, peer failure, or unacceptable risk.

## Snapshot context

- Snapshot schema: v4
- Current normal channels: 93
- Public capacity of normal channels: 430,079,598 sat
- Current local balance in normal channels: approximately 198,541,190 sat
- On-chain wallet balance: 10,825,624 sat
- Node 12-month net ROIC: 0.1423%
- Node 12-month routed amount: 660,685,805 sat
- Node 12-month rebalance cost: 6,064.906 sat

### Capacity is not invested capital

The snapshot currently uses two different denominators:

- Node-level `net_roic_12_months_percent` uses the current sum of local channel
  balances. It therefore excludes inbound liquidity, although current balance
  is only a proxy for the average capital deployed over the trailing year.
- Per-channel `gross_roic_percent`, `net_roic_percent`, and
  `indirect_roic_percent` divide by public channel capacity, which includes the
  peer's inbound liquidity.

The per-channel percentages are therefore not true ROIC from this node's point
of view. They are capacity-normalized revenue yields. They should not be used as
the primary ranking for capital allocation.

For a rigorous per-channel ROIC, the preferred denominator is the node's
time-weighted average local balance over the same measurement period. Current
local balance is useful for deciding how much capital a splice-out would release,
but it can be misleading as a historical denominator because routing moves local
capital between channels. For a proposed splice-in, the added amount is known
exactly and is the correct marginal capital denominator.

The present snapshot does not export a time-weighted local-capital metric. The
liquidity-history dataset can approximate it only for the period covered by the
archive. Until this metric is available, use absolute net fees, realized fee
rate, liquidity turnover, rebalance cost, and persistent depletion rather than
calling the per-channel capacity-normalized percentage ROIC.

Indirect fees must also be retained in the analysis. They are not additional
node revenue, but they identify channels that supplied the incoming side of
forwards whose revenue was earned on another channel. Closing such a channel can
remove those routes.

## How fee discovery works

The behavior below comes from `src/fees.rs` and `src/sling.rs`.

For every normal channel, the fee process examines outbound forwards from the
previous 24 hours:

- At least one settled outbound forward increases PPM by 10%.
- No settled outbound forward decreases PPM by 5%.
- While the current PPM is above 1,000, no success decreases it by 10% instead.
- PPM is clamped to the range 10–5,000.
- When local balance is below 50,000 sat, the lower bound becomes 2,500 PPM.
- Maximum HTLC is adjusted to the largest power of two no greater than the local
  balance, so the advertised usable amount follows available liquidity.
- A peer with measured availability below 80% is disabled by setting its HTLC
  bounds to 1 msat.

This is a feedback search rather than a fixed fee policy. An apparently idle
young channel may still be walking down from an uneconomic fee. A successful
channel walks upward until traffic stops, then probes downward again.

Sling complements that search. Channels at or below 30% local balance are
eligible as rebalance targets. It uses realized time-decayed and historical fee
rates to set a rebalance budget, and it can bootstrap an almost completely
depleted channel above the 50,000 sat threshold so dynamic fee search resumes.

Consequences for capacity analysis:

1. Do not infer failure from a few idle months alone.
2. Give a channel about one year to discover its market unless there is a
   compelling exception.
3. Treat depleted, high-fee channels as possible expansion candidates, not
   closure candidates.
4. Evaluate shrinkable capital from local balance, not public capacity.
5. Include incoming-side attribution and availability before recommending a
   close.

## Recommended first expansion tranche

The snapshot wallet contains about 10.83M sat. A conservative first tranche is
8M sat, leaving roughly 2.8M sat before splice fees and CLN's emergency reserve.
Confirm the live wallet balance and reserve configuration before executing.

| Priority | Peer | Channel | Add | Evidence |
|---:|---|---|---:|---|
| 1 | Lightning Goats CLN | `956943x1092x0` | 3M sat | In nine days it routed 4.94M sat, earned 8,514 sat, and fell to 58,030 sat local (1.16%). Realized and time-decayed fees are both about 1,700 PPM. |
| 2 | `e960fd...` | `957006x2209x0` | 3M sat | In nine days it routed 4.94M sat, earned 7,690 sat, and fell to 63,962 sat local (1.28%). Realized and time-decayed fees are about 1,500–1,560 PPM. The same peer's older channel is also depleted and profitable. |
| 3 | Megalith LSP | `925417x701x0` | 1M sat | The channel is only 1M sat and has 15,098 sat local. It earned 436 sat on 496k sat routed in the last 30 days with no recorded rebalance cost. |
| 4 | zap.opentimestamps.org | `848864x399x0` | 1M sat | The channel has 112,230 sat local (3.47%). It routed 2.20M sat and earned 1,437 sat in 30 days with only 2.856 sat of target rebalance cost. |

The first two channels have only nine days of history, so no long-term return
should be inferred. Their stronger signal is that they moved nearly their entire
original 5M sat channel size at a realized fee around 1,500–1,700 PPM. The 3M
additions are deliberately smaller than the initial observed flow.

### Next expansion candidates

If the first tranche is absorbed profitably, or more on-chain funds become
available, consider:

| Peer | Channel | Possible addition | Reason |
|---|---|---:|---|
| LQWD-Australia | `894520x931x16` | 1–2M sat | 24,134 sat local (1.21%); routed 906k sat and earned 967 sat in the last 30 days without recorded target rebalance cost in that window. |
| zap.opentimestamps.org | `848864x399x0` | another 1–2M sat | Strong recent result and persistent depletion; add incrementally to observe marginal demand. |
| Megalith LSP | `925417x701x0` | another 1M sat | Small existing channel, strong lifetime result, and continuing recent traffic. |
| `e960fd...` | peer-level | another 2M sat | Both parallel channels are depleted. Prefer enlarging a splice-capable channel instead of creating another parallel channel. |

Ark Labs has a strong historical capacity-normalized yield but is not an
immediate expansion target: it currently has 99% local balance and no forwards
in the last 30 days. It is not presently constrained by outbound liquidity.

## CLN splice-in instructions

These instructions target CLN v26.04 or newer, where the stable `splicein` RPC
is available. Complete the node upgrade and confirm that `lightning-cli help`
lists `splicein` before using them.

Both the channel and peer must support the splice, the peer must cooperate, and
the channel should be connected and `CHANNELD_NORMAL`.

First inspect the live node:

```bash
lightning-cli getinfo | jq -r .version
lightning-cli help splicein
lightning-cli listfunds
lightning-cli feerates perkw | jq '.perkw | {splice, opening, floor}'
```

Check each channel immediately before acting. The full channel IDs below avoid
ambiguity:

```bash
lightning-cli listpeerchannels -k channel_id=98391126aec4bbf59c167b6fe7a1ba714f6611cf939c2f25028716b057bf5cbd
lightning-cli listpeerchannels -k channel_id=efb4fec2b9d45641980dd114608563d314a8f94559e02ec1acc245179ee3b4e7
lightning-cli listpeerchannels -k channel_id=bcfc16257a1fbbdd2a45144e08e8f00c10cbafe2b9d472e9420efcae28de9119
lightning-cli listpeerchannels -k channel_id=c16aa45dde535ee17bf4c2a11532e279e31ee47288a6ae905ed00fe0cfbc71b3
```

Use CLN's safe `check` RPC to validate each `splicein` command and its parameters
without changing state:

```bash
lightning-cli check -k command_to_check=splicein channel=98391126aec4bbf59c167b6fe7a1ba714f6611cf939c2f25028716b057bf5cbd amount=3M
lightning-cli check -k command_to_check=splicein channel=efb4fec2b9d45641980dd114608563d314a8f94559e02ec1acc245179ee3b4e7 amount=3M
lightning-cli check -k command_to_check=splicein channel=bcfc16257a1fbbdd2a45144e08e8f00c10cbafe2b9d472e9420efcae28de9119 amount=1M
lightning-cli check -k command_to_check=splicein channel=c16aa45dde535ee17bf4c2a11532e279e31ee47288a6ae905ed00fe0cfbc71b3 amount=1M
```

After reviewing live balances, channel state, peer connectivity, the splice
feerate, and the successful `check` result, execute one at a time:

```bash
lightning-cli splicein -k channel=98391126aec4bbf59c167b6fe7a1ba714f6611cf939c2f25028716b057bf5cbd amount=3M
lightning-cli splicein -k channel=efb4fec2b9d45641980dd114608563d314a8f94559e02ec1acc245179ee3b4e7 amount=3M
lightning-cli splicein -k channel=bcfc16257a1fbbdd2a45144e08e8f00c10cbafe2b9d472e9420efcae28de9119 amount=1M
lightning-cli splicein -k channel=c16aa45dde535ee17bf4c2a11532e279e31ee47288a6ae905ed00fe0cfbc71b3 amount=1M
```

Do not paste all four commands blindly. After each splice, wait for the RPC to
finish, inspect `listpeerchannels`, confirm the new funding transaction and
remaining wallet balance, and then decide whether to proceed with the next one.
During confirmation, CLN keeps the channel active but spendability is limited to
the smaller of the old and new balances.

## Shrink and close assessment

There is no current shrink or close recommendation.

The previously named channels VIVA, MOLA, `sarsen-2`, and the depleted channels
do not release meaningful capital because their current local balances are
approximately zero or very small. Most are also much younger than one year.
Closing them now would mainly discard remote inbound liquidity and interrupt
fee discovery.

For future review, the following channels are more than a year old and currently
contain meaningful local balance, but they are only **review candidates**, not
action recommendations:

| Peer | Age | Public capacity | Current local | Direct / attributed fees, trailing 365 d | Why review carefully |
|---|---:|---:|---:|---:|---|
| TwentyTwo | 761 d | 2.20M | 1.94M | 91 / 147 sat | Low recent direct and indirect contribution despite a completed discovery period. |
| TACO | 443 d | 4.00M | 3.92M | 482 / 1,619 sat | Most capacity is local, but incoming-side contribution is meaningful and would be at risk. |
| HelloLightning | 721 d | 2.50M | 1.54M | 534 / 922 sat | Balanced liquidity and two-sided forwarding argue against a mechanical close. |
| Luna | 379 d | 5.00M | 1.67M | 981 / 1,972 sat | Only just past one year and still contributes on both sides. |
| MAGNUS | 402 d | 3.50M | 1.79M | 1,338 / 904 sat | Not idle; recent traffic, indirect value, and 59 sat of target rebalance cost need trend analysis. |

Before shrinking any of these, inspect at least:

- Current local balance and actual `spendable_msat`, not capacity alone.
- One-year fee-policy history and whether the fee has reached a stable range.
- Recent time-decayed PPM versus current PPM.
- Incoming-side fee attribution and common channel pairs.
- Peer availability and disconnection history.
- Rebalance source value as well as target cost.
- Expected on-chain splice cost and the amount of local capital actually freed.
- Whether reducing capacity would damage receivable liquidity or routing limits.

## Procedure for a future inquiry

1. Generate a fresh snapshot and record its timestamp and schema version.
2. Confirm the live dynamic-fee constants and Sling strategy have not changed.
3. Establish the capital budget from on-chain spendable funds and any explicitly
   approved splice-out donors.
4. Rank expansion candidates using recent direct net revenue, realized PPM,
   persistent low local balance, rebalance cost, and evidence of capacity
   exhaustion.
5. Do not rank channels by the current per-channel `*_roic_percent` fields,
   because their denominator includes the peer's inbound liquidity. Size young
   channels by observed profitable flow instead.
6. Exclude channels with strong capacity-normalized yield but ample unused local
   balance from immediate expansion unless inbound capacity is the constraint.
7. Apply the one-year discovery presumption before considering shrinkage.
8. For shrink candidates, rank by recoverable local balance and total route
   contribution, not by full capacity and direct revenue alone.
9. Recommend incremental changes, then compare the next 30- and 90-day marginal
   revenue against the known splice-in capital and splice cost. Where history is
   sufficiently complete, calculate return using time-weighted local balance.

## Source material

- Snapshot data: `target/snapshot/manifest.json`, `summary.json`,
  `channels.json`, `settled-forwards.jsonl`, and `rebalances.jsonl`
- Dynamic fees: `src/fees.rs`
- Rebalance discovery support: `src/sling.rs`
- Snapshot metric definitions: `src/snapshot_metadata.rs`
- Local CLN references:
  - `~/references/apis/core-lightning/reference/splicein.md`
  - `~/references/apis/core-lightning/reference/dev-splice.md`
  - `~/references/apis/core-lightning/reference/check.md`
  - `~/references/apis/core-lightning/reference/listpeerchannels.md`
  - `~/references/apis/core-lightning/reference/feerates.md`
