# Dynamic Fee Strategy

Status: implemented in `src/fees.rs`.

This document describes Lightdash's daily dynamic outbound fee policy and its
relationship with [SLING_REBALANCE_STRATEGY.md](SLING_REBALANCE_STRATEGY.md).

## Objective

The policy tries to discover an attractive forwarding price while protecting
scarce outbound liquidity:

- a settled outbound forward is positive price evidence
- a channel with no forwarding history should search downward quickly
- an established channel should search downward slowly
- a depleted channel should become gradually more expensive
- failed, offered, and local-failed HTLCs must not affect price

The controller deliberately does not use forward attempts, TPPM, historical
PPM, raw forward count, or routed amount in its fee step. TPPM and historical
PPM remain useful for analysis and Sling budgets.

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

- it has at least 50,000 local sats
- it has never had a settled outbound forward retained by Core Lightning

Without a recent settlement, its PPM decreases by 15% per day. This searches
quickly from the initial high fee.

### Normal

A channel is normal when:

- it has at least 50,000 local sats
- it has at least one settled outbound forward in its retained history

Without a recent settlement, its PPM decreases by 2% per day. This is close to
decreasing 5% every three days, but requires no idle counter or additional
datastore state:

```text
0.98^3 = 0.941192
```

The normal-price half-life is about 34 idle days.

### Depleted

A channel is depleted whenever it has fewer than 50,000 local sats, regardless
of forwarding history.

Without a recent settlement, its PPM increases by 1% per day. This is a simple
inventory-scarcity adjustment. It does not jump or reset the channel to 2,500
PPM.

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
else if settled outbound forward in the last 24 hours:
    increase PPM by 5%
else if local balance < 50,000 sats:
    increase PPM by 1%
else if no settled outbound forward has ever been retained:
    decrease PPM by 15%
else:
    decrease PPM by 2%
```

A recent settlement overrides channel state. For example, a depleted channel
with a settlement receives the 5% forwarding increase, not a stacked 6%
increase.

The depleted state has precedence over bootstrap and normal classification when
there is no recent settlement.

## Policy table

| Condition | Daily PPM action |
|---|---:|
| Availability below 80% | Keep PPM; disable HTLC forwarding |
| Settled outbound forward in last 24 hours | `+5%` |
| No recent settlement, below 50,000 local sats | `+1%` |
| No recent settlement, never settled outbound | `-15%` |
| No recent settlement, established channel | `-2%` |

Every result is clamped to 10–5,000 PPM.

The base fee remains 1,000 msat.

## Rounding

PPM is an integer. Increases round upward so low values always make progress:

```text
forwarded_ppm = ceil(current_ppm * 1.05)
depleted_ppm = ceil(current_ppm * 1.01)
```

Examples:

```text
10 PPM + 5% = 11 PPM
100 PPM + 1% = 101 PPM
101 PPM + 1% = 103 PPM
```

Decreases round downward:

```text
bootstrap_ppm = floor(current_ppm * 0.85)
normal_ppm = floor(current_ppm * 0.98)
```

The final clamp prevents a decrease below 10 PPM or an increase above 5,000
PPM.

## Forward evidence

Only a record satisfying both conditions is positive price evidence:

- `status == "settled"`
- `out_channel == channel being priced`

Recent evidence uses the existing 24-hour forwarding window. All retained
settled outbound forwards determine whether a channel has graduated from
bootstrap to normal.

Non-settled attempts are retained only for diagnostics and logging. They do
not:

- change the fee direction or percentage
- graduate a channel from bootstrap
- justify a rebalance
- increase a Sling budget

## HTLC and availability behavior

The existing operational safeguards remain unchanged:

- below 80% peer availability, Lightdash sets both HTLC limits to 1 msat
- otherwise maximum HTLC is the largest power of two no greater than current
  local balance
- minimum HTLC remains at least 100,000 msat unless maximum HTLC is smaller

HTLC changes and fee changes are sent in the same `setchannel` command.

## Relationship with Sling

The state policies have complementary roles:

- bootstrap fee discovery finds whether a new channel has demand
- normal pricing searches slowly around an accepted region
- depleted pricing charges gradually more for scarce remaining liquidity
- Sling attempts to restore depleted targets from cheap, locally liquid sources

The deployed order is intentional:

```text
00:01  dynamic fee adjustment
02:13  Sling target and job generation
```

The depleted increase can also gradually relax Sling's current-channel-PPM cap
for established channels when their history-derived rebalance budget is higher.
Sling retains its independent safety caps and profitability limitations
documented in `SLING_REBALANCE_STRATEGY.md`.

## Why this policy is intentionally simple

The policy needs no persisted idle counter or learned demand model. Current
balance, the latest 24-hour settled window, and retained settled history fully
determine the action.

This makes every decision easy to reconstruct:

```text
state + recent settlement + current PPM = next PPM
```

The asymmetry is intentional:

- unknown price: move down quickly
- previously accepted price: move down gently
- scarce inventory: move up very gently
- newly accepted price: test upward

## Migration caveat

Before this policy, depleted channels were forced to a minimum of 2,500 PPM.
The new controller cannot determine whether an existing 2,500 PPM value was
learned by forwarding or imposed by the old floor.

Consequently, an already depleted channel at 2,500 PPM will initially increase
by 1% per day until liquidity returns. Once it has at least 50,000 local sats,
it follows bootstrap or normal behavior based on retained settlement history.

This is a one-time transition issue. New depletion events no longer overwrite
the previous price with 2,500 PPM.

## Known tradeoffs

- A single settled HTLC, even a small MPP part, graduates a channel to normal.
- Every settlement receives the same 5% step regardless of amount.
- A normal channel adapts slowly to a genuine downward market-price change.
- The bootstrap/normal distinction depends on Core Lightning retaining at
  least one successful forward. Deployments which prune successful forwards
  can eventually misclassify an old channel as bootstrap.
- The recent window uses `received_time`, matching the existing Store filter,
  rather than settlement time or a forward watermark.
- Daily fee changes produce more gossip updates than a multi-day idle counter,
  though the deployed cadence remains within Core Lightning's documented
  update limits.

These tradeoffs are accepted in exchange for a small, explainable controller.
Evaluation should focus on net forwarding revenue, realized rebalance cost,
capital turnover, and time spent depleted rather than forward attempts or raw
volume alone.
