# Dynamic Fee Strategy

Status: implemented in `src/fees.rs`.

This document describes Lightdash's daily dynamic outbound fee policy and its
relationship with [SLING_REBALANCE_STRATEGY.md](SLING_REBALANCE_STRATEGY.md).

## Objective

The policy tries to discover an attractive forwarding price while maintaining
a minimum outbound reserve:

- settled outbound forwards routing at least 5,000 sats in the last 24 hours are
  positive price evidence
- smaller settled outbound traffic keeps the price unchanged
- a channel with no forwarding history should search downward quickly
- an established channel should search downward slowly
- a channel below its depleted threshold should not search downward
- failed, offered, and local-failed HTLCs must not affect price

The controller deliberately does not use forward attempts, TPPM, historical
PPM, or raw forward count in its fee step. TPPM and historical PPM remain
useful for analysis and Sling budgets. TPPM is the time-decayed,
amount-weighted full realized fee rate over settled outbound forwards of at
least 1,000 sats; it includes the base fee.

## Depleted threshold

A channel's depleted threshold is:

```text
depleted_threshold_sat = max(50,000, channel_capacity_sat / 20)
```

Channels up to 1,000,000 sats therefore use the fixed 50,000-sat reserve, while
larger channels keep 5% of their capacity. The threshold is an operational
reserve target, not an attempt to price scarcity across the full balance range.
HTLC maximum policy is the primary control on how remaining liquidity can be
used.

## Production cadence

The deployed `unique` system declares the fee service in
`~/systems/unique/lightdash.nix`:

```text
service: lightdash_fees
command: lightdash fees --availdb <summars-availdb>
execution: EXECUTE_SETCHANNEL=1
schedule: *-*-* 00:01:00
```

Fees therefore change at most once per scheduled daily run. Sling runs later
at 02:13.

## Channel states

Every normal channel is classified into one of three states.

### Bootstrap

A channel is bootstrap when:

- its local balance is at least its depleted threshold
- it has never had a settled outbound forward retained by Core Lightning

Without recent forwarding, its PPM decreases by 15% per day. This searches
quickly from the initial high fee.

### Normal

A channel is normal when:

- its local balance is at least its depleted threshold
- it has at least one settled outbound forward in its retained history

Without recent forwarding, its PPM decreases by 2% per day. This is close to
decreasing 5% every three days, but requires no idle counter:

```text
0.98^3 = 0.941192
```

The normal-price half-life is about 34 idle days.

### Depleted

A channel is depleted whenever its local balance is below its depleted
threshold, regardless of forwarding history.

Without recent forwarding, its PPM increases by 1% per day. The purpose is to
prevent downward price search while Sling restores the reserve, not to use fees
as the primary way to discourage channel use.

If a depleted channel later returns to normal balance, the normal 2% daily
decrease is roughly twice as fast as the preceding 1% increase:

```text
days_to_reverse ~= depleted_days * log(1.01) / -log(0.98)
                ~= depleted_days * 0.49
```

## Decision precedence

The daily decision is:

```text
if peer availability < 80%:
    disable forwarding through the HTLC range
else if settled outbound forwards in the last 24 hours routed >= 5,000 sats:
    increase PPM by 5%
else if any settled outbound forward in the last 24 hours:
    keep PPM unchanged
else if local balance < depleted threshold:
    increase PPM by 1%
else if no settled outbound forward has ever been retained:
    decrease PPM by 15%
else:
    decrease PPM by 2%
```

Recent forwarding overrides channel state. For example, a depleted channel
with enough recent settled volume receives the 5% forwarding increase, not a
stacked 6% increase, and a depleted channel with only a small recent settlement
keeps its price.

The depleted state has precedence over bootstrap and normal classification when
there is no recent settlement.

## Policy table

| Condition | Daily PPM action |
|---|---:|
| Availability below 80% | Keep PPM; disable HTLC forwarding |
| Settled outbound forwards in last 24 hours routed at least 5,000 sats | `+5%` |
| Settled outbound forwards in last 24 hours routed less than 5,000 sats | unchanged |
| No recent settlement, below depleted threshold | `+1%` |
| No recent settlement, never settled outbound | `-15%` |
| No recent settlement, established channel | `-2%` |

Every result is clamped to 1–5,000 PPM.

The base fee remains 1,000 msat.

## Fractional PPM

Channel PPM is an integer, but percentage steps are applied to an unrounded
value so small steps accumulate at low fees instead of being lost to rounding.
Each run:

1. Loads the stored unrounded PPM from the Core Lightning datastore key
   `lightdash/fee_ppm_exact/<short_channel_id>`.
2. Uses it only if it still rounds to the PPM currently set on the channel.
   Otherwise the fee was changed outside this controller, or never stored, and
   the current integer PPM becomes the starting point.
3. Applies the percentage step to the unrounded value and clamps it to
   1–5,000 PPM.
4. Sets the channel fee to the result rounded to the nearest integer.
5. Stores the new unrounded value, even when the rounded fee did not change.

For example, a 5 PPM channel decreasing 2% per day keeps advertising 5 PPM
for five days while its stored value falls from 4.900 to 4.520, then advertises
4 PPM on the sixth day (4.429). Rounding to the nearest integer every day
instead would keep it at 5 PPM forever, because 4.9 rounds back to 5.

## Forward evidence

Only a record satisfying both conditions counts as outbound forwarding:

- `status == "settled"`
- `out_channel == channel being priced`

The recent window contains forwards received in the last 24 hours. Its routed
amount is the sum of `out_msat` over those settled forwards. All retained
settled outbound forwards determine whether a channel has graduated from
bootstrap to normal.

Non-settled attempts are retained only for diagnostics and logging. They do
not:

- change the fee direction or percentage
- graduate a channel from bootstrap
- justify a rebalance
- increase a Sling budget

This exclusion is also based on prior local investigation: failed-HTLC
observations were noise rather than a useful demand signal. A failure is not
evidence that a higher price was accepted.

## HTLC and availability behavior

The operational safeguards are:

- below 80% peer availability, Lightdash sets both HTLC limits to 1 msat
- otherwise maximum HTLC is the largest power of two no greater than what a
  single HTLC can actually carry (see below), and at least 1 msat
- minimum HTLC remains at least 100,000 msat unless maximum HTLC is smaller

The basis for maximum HTLC is:

```text
basis = spendable_msat + outgoing HTLCs currently in flight
basis = min(basis, 2^32 - 1 msat)   when the peer lacks large-channel support
```

`spendable_msat` already excludes the channel reserve (1% of capacity), the
commitment fee when we opened the channel, and the per-HTLC protocol limit for
peers without `option_support_large_channel`. Basing the maximum on the local
balance instead would advertise amounts that fail at our node: a 10,000,000-sat
channel holding exactly its 100,000-sat reserve can forward nothing.

`spendable_msat` also drops while our own outgoing HTLCs are in flight. Adding
them back keeps the basis equal to balance minus reserve and fees, which moves
only when HTLCs settle, so forwards in flight do not cause extra gossip.
Incoming HTLCs do not affect it until they settle.

The power-of-two floor publishes only the order of magnitude of the spendable
balance, and changes only when a settled payment crosses a power-of-two
boundary.

HTLC changes and fee changes are sent in the same `setchannel` command. The
minutely `lightdash htlc` job applies the same rule between fee runs but only
ever lowers the maximum, lowering the minimum with it when needed. Increases
happen only in the daily fee run, at most once per channel per day.

The maximum-HTLC rule, rather than a capacity-relative fee curve, is the main
mechanism limiting use as local liquidity falls.

This is a recovery target, not a hard balance guarantee. Maximum HTLC limits a
single HTLC, so several concurrent HTLCs, such as parts of one multi-part
payment, can together exceed what is spendable. It also ignores the depleted
threshold, so an accepted forward can cross that boundary.

## Relationship with Sling

The state policies have complementary roles:

- bootstrap fee discovery finds whether a new channel has demand
- normal pricing searches slowly around an accepted region
- depleted pricing preserves and gradually raises the retained price while the
  reserve is restored
- Sling attempts to restore depleted targets from cheap, locally liquid sources

The deployed order is intentional:

```text
00:01  dynamic fee adjustment
02:13  Sling target and job generation
```

Sling's ordinary rebalance budget is 75% of the lower of TPPM and the current
channel PPM, so the depleted increase can gradually raise that budget for
channels whose TPPM is above their current PPM. Sling retains its independent
safety caps and profitability filter documented in
`SLING_REBALANCE_STRATEGY.md`.

The ordinary rebalance budget follows the forwarding floor down to 1 PPM,
while Sling's source-PPM ceiling keeps its own independent 10 PPM floor.

For a channel with fewer than 10 local sats, Sling performs one bounded
100,000-sat bootstrap at up to 1,100 PPM. The amount is twice the 50,000-sat
base reserve, so one successful operation restores channels up to 1,000,000
sats above their depleted threshold. Larger channels need ordinary Sling jobs
to reach their 5%-of-capacity threshold.

## Why this policy is intentionally simple

The policy needs no idle counter or learned demand model. Current balance,
capacity, the latest 24-hour settled window, retained settled history, and the
stored unrounded PPM fully determine the action.

The deployment also aims for one channel per peer and uses splicing to change
capacity. Under that operating model, channel-scoped and peer-scoped pricing
refer to the same economic relationship while exact outbound-SCID evidence
keeps decisions reconstructible.

This makes every decision easy to reconstruct:

```text
state + recent settled volume + unrounded PPM = next PPM
```

The asymmetry is intentional:

- unknown price: move down quickly
- previously accepted price: move down gently
- scarce inventory: move up very gently
- newly accepted price with meaningful volume: test upward
- accepted price with little volume: hold

## Known tradeoffs

- A single settled HTLC, even a small MPP part, graduates a channel to normal.
- Every 24-hour window with at least 5,000 routed sats receives the same 5%
  step regardless of how much more was routed.
- A normal channel adapts slowly to a genuine downward market-price change.
- The bootstrap/normal distinction depends on Core Lightning retaining at
  least one successful forward. Deployments which prune successful forwards
  can eventually misclassify an old channel as bootstrap.
- The recent window uses `received_time` rather than settlement time or a
  forward watermark.
- Daily fee changes produce more gossip updates than a multi-day idle counter,
  though the deployed cadence remains within Core Lightning's documented
  update limits.

These tradeoffs are accepted in exchange for a small, explainable controller.
Evaluation should focus on net forwarding revenue, realized rebalance cost,
capital turnover, and time spent depleted rather than forward attempts or raw
volume alone.
