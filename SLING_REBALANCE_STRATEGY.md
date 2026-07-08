# Sling Rebalance Strategy Notes

## Current Behavior

Lightdash currently uses `sling-once` for rebalancing. For each depleted target
channel, it looks at settled outbound forwards in the last 24 hours and sums the
forwarded amount on that channel.

That routed amount becomes the basis for the rebalance:

- round the total down to a multiple of 4 sats
- use the whole amount when it is at most 400,000 sats
- split it into 2 chunks when it is above 400,000 sats
- split it into 4 chunks when it is above 800,000 sats

This is intentionally demand-driven: a channel is replenished roughly according
to recent proven outbound usage.

## Problem

For very depleted channels, using only the last 24 hours of outbound routed
amount can be too conservative. A channel at very low local balance may need more
liquidity to become useful again, even if recent forwards were small.

However, simply filling every depleted channel up to 50% local balance is also
risky. The liquidity has to come from other channels. If a rebalance drains a
candidate channel below a useful balance, the node has only moved the imbalance
elsewhere while paying fees for the move.

The source side matters as much as the target side.

## Better Direction

Prefer smaller, more frequent rebalances over large one-shot corrections.

Small operations let the system observe updated balances between attempts. If a
candidate gets used heavily, the next run can avoid it. This reduces the chance
of overcorrecting based on stale balances or one recent forward.

A reasonable policy is:

- target depleted channels that have recent outbound demand
- allow a demand boost when the target is especially depleted
- cap the target at 50% local balance
- avoid draining source candidates below a conservative floor
- keep each rebalance amount small

## Why Consider `sling-job`

`sling-once` gives exact control over the total amount through `onceamount`, but
it does not support `target`. Lightdash therefore has to estimate the full
rebalance amount itself.

`sling-job` supports balance-aware continuous behavior:

- `target` lets the job idle once the target channel reaches the desired balance
- `depleteuptopercent` controls how far candidate channels may be depleted
- `depleteuptoamount` caps the candidate depletion formula
- `amount` controls the size of each rebalance operation

This better matches the desired behavior: converge toward a target balance using
small operations, while leaving source channels usable.

## Candidate Depletion Caveat

Sling's deplete floor is:

```text
min(depleteuptopercent * channel_capacity, depleteuptoamount)
```

The default `depleteuptoamount` is 2,000,000 sats. For large channels, that can
be lower than 50% of capacity. If the goal is to avoid draining candidates below
roughly half capacity, `depleteuptoamount` must be set high enough that the
percentage floor is effective for the channels being used.

## Suggested Shape

Use `sling-job` for target channels, with conservative per-operation amounts:

```text
sling-job -k \
  scid=<target_scid> \
  direction=pull \
  amount=<small_amount_sat> \
  maxppm=<budget_ppm> \
  target=0.5 \
  candidates=<candidate_scids> \
  depleteuptopercent=0.5 \
  depleteuptoamount=<large_floor_cap_sat>
```

The `amount` should stay modest, for example 25,000 to 100,000 sats depending on
channel size and fee budget. The job can then make progress gradually and idle
when the target reaches 50%.

## Open Questions

- Whether Lightdash should create persistent `sling-job` jobs, update them each
  run, or continue deleting/recreating jobs.
- What default per-operation amount is appropriate across small and large
  channels.
- How aggressively to boost demand-based rebalancing for channels below 10% or
  25% local balance.
- What `depleteuptoamount` should be used so `depleteuptopercent=0.5` is not
  accidentally capped too low on large channels.
