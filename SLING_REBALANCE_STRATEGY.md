# Sling Rebalance Strategy

Status: description of the policy currently implemented in `src/sling.rs`.

This document is an implementation reference for Lightdash's Sling policy. For
the proposed long-term economic coupling between fees and rebalancing, see
[DYNAMIC_FEE_STRATEGY.md](DYNAMIC_FEE_STRATEGY.md).

## Purpose

Lightdash uses Sling to pull local liquidity into depleted channels from
channels which currently appear cheap and locally liquid.

The implemented policy is primarily balance- and capacity-driven:

- source candidates must be above 70% local balance and below 300 PPM
- targets must be at or below 30% local balance
- ordinary jobs pull toward 50% local balance
- rebalances use small, variable operation sizes
- the fee budget is derived from realized forwarding PPM when history exists

Recent settled routed amount does not currently control target eligibility or
the total amount replenished.

## Policy constants

| Setting | Current value |
|---|---:|
| Maximum source-channel PPM | `< 300` |
| Minimum source local balance | `> 70%` |
| Maximum target local balance | `<= 30%` |
| Ordinary job target balance | `50%` |
| Minimum operation amount | `10,000 sats` |
| Capacity amount hint | `5% of capacity` |
| Ordinary budget clamp before current-PPM cap | `10–1,100 PPM` |
| Realized-fee budget multiplier | `60%` |
| Candidate `depleteuptopercent` | `0.5` |
| Candidate `depleteuptoamount` | `1,000,000 sats` |
| Dust bootstrap threshold | `< 10 local sats` |
| Dust bootstrap amount | `100,000 sats` |
| Dust bootstrap maximum budget | `1,100 PPM` |

The 10 PPM minimum and 100,000-sat dust bootstrap amount reuse constants from
the dynamic fee policy:

```text
ordinary bootstrap budget = PPM_MIN
dust bootstrap amount = 2 * DEPLETED_LOCAL_BALANCE_SAT
```

## Source candidate selection

Lightdash computes one explicit candidate list before examining targets. A
normal channel is included only when:

1. it has a short channel ID
2. Lightdash can resolve the local channel announcement
3. the local advertised fee is below 300 PPM
4. local balance is strictly greater than 70% of channel capacity

The same JSON candidate list is passed to every `sling-once` and `sling-job`
invocation during the run. Lightdash computes this list itself because Sling's
PPM filtering does not also enforce the desired current-balance filter.

If the list is empty, no bootstrap or ordinary rebalance is created.

## Target selection

Lightdash examines every normal channel. A channel is a target when:

- its local balance is at most 30% of capacity
- it has a short channel ID
- at least one source candidate exists

There is no current requirement for:

- a settled forward in the last 24 hours
- a minimum recent routed amount
- a positive historical net margin
- a minimum number of forwards

Forward history affects the ordinary PPM budget, but not target eligibility.

Targets follow one of two paths: a dust bootstrap or an ordinary persistent
job.

## Dust bootstrap

A target with fewer than 10 local sats uses a bounded one-shot rebalance:

```text
lightning-cli sling-once -k \
  scid=<target_scid> \
  direction=pull \
  candidates=<candidate_scids> \
  maxppm=1100 \
  amount=100000 \
  onceamount=100000
```

This path deliberately ignores the ordinary history-derived budget. It may pay
up to 1,100 PPM, but only for a single 100,000-sat bootstrap.

The amount is twice the fee controller's 50,000-sat depleted threshold. A
successful bootstrap therefore moves the channel out of that absolute
depleted state and lets bootstrap or normal dynamic fee behavior resume,
depending on whether the channel has retained settled outbound history.

Dust bootstraps are executed immediately when `EXECUTE_SLING` is set. They are
not persistent jobs and do not require `sling-go`.

## Ordinary target amount

For every other eligible target, Lightdash first computes an amount hint.

```text
capacity_hint
  = floor_to_multiple_of_4(channel_capacity_sat / 20)

missing_to_target
  = max(50% * channel_capacity_sat - local_balance_sat, 0)

amount_hint
  = floor_to_multiple_of_4(min(capacity_hint, missing_to_target))
```

The target is skipped if the resulting amount is below 10,000 sats. This also
means an ordinary channel smaller than 200,000 sats cannot produce the minimum
5%-of-capacity hint.

The hint is not necessarily the Sling operation amount. Lightdash selects one
of these values using a per-run jitter seed:

```text
10,000
20,000
40,000
80,000
160,000
320,000 sats
```

The selected value is capped by `amount_hint` and never allowed below 10,000
sats.

The jitter is derived from the target SCID, current time, and process ID. Its
purpose is to vary operation sizes between runs instead of making every target
use the same predictable amount.

## Ordinary rebalance budget

Lightdash reads two realized forwarding metrics for the target:

- **TPPM:** time-decayed, amount-weighted variable fee PPM with a seven-day
  half-life and an assumed one-sat base fee removed
- **historical effective PPM:** all-time forwarding fees divided by all-time
  routed amount

Only finite values greater than zero are usable.

When one or both metrics are available:

```text
realized_ppm
  = arithmetic_mean(all usable metrics)

budget_ppm
  = truncate(realized_ppm * 0.60)
  |> clamp(10, 1100)
  |> min(current_channel_ppm, when available)
```

When neither metric is usable, the budget is exactly 10 PPM. This early
fallback does not apply the current-channel-PPM cap, although the fee
controller normally keeps channel PPM at or above 10.

The budget is what Lightdash is willing to pay for replenishment, not what it
charges for outbound forwarding. The 60% multiplier reserves an intended gross
spread between past forwarding revenue and rebalance cost.

The two input metrics do not currently have identical meaning: TPPM removes
the fixed base fee while historical effective PPM includes it. Both are also
currently derived after per-forward fee values have been truncated to sats.
These limitations are documented in `DYNAMIC_FEE_STRATEGY.md`.

## Ordinary Sling job

An ordinary target creates this persistent job:

```text
lightning-cli sling-job -k \
  scid=<target_scid> \
  direction=pull \
  amount=<jittered_operation_amount_sat> \
  maxppm=<history_derived_budget_ppm> \
  target=0.5 \
  candidates=<candidate_scids> \
  depleteuptopercent=0.5 \
  depleteuptoamount=1000000
```

`amount` controls each Sling operation. It is not a total replenishment cap.
The job can continue operating until Sling considers the 50% target reached or
another Sling condition prevents progress.

The 50% target is therefore a balance target, whereas the 5%-of-capacity hint
and jittered amount keep individual operations smaller.

## Candidate depletion caveat

Sling's candidate floor is:

```text
min(depleteuptopercent * candidate_capacity, depleteuptoamount)
```

With the current arguments:

```text
min(50% * candidate_capacity, 1,000,000 sats)
```

For candidates up to 2,000,000 sats, the percentage term is effective. For
larger candidates, the 1,000,000-sat cap is lower than 50% of capacity and may
allow Sling to drain the source below 50%.

The initial Lightdash filter only proves that a candidate was above 70% when
the run began. It does not by itself guarantee a 50% post-rebalance balance.

## Execution lifecycle

Without `EXECUTE_SLING`, the command is a dry run. It computes candidates,
targets, budgets, and amounts and logs the proposed commands without changing
Sling state.

With `EXECUTE_SLING`:

1. Lightdash runs `sling-stop`.
2. Lightdash runs `sling-deletejob all`.
3. If there are no candidates, the run returns with no replacement jobs.
4. Dust targets execute immediately through `sling-once`.
5. Ordinary targets are recreated through `sling-job`.
6. If at least one ordinary job was created, Lightdash runs `sling-go`.

Jobs are therefore not updated in place or preserved across Lightdash runs.
Every executing run replaces the complete Sling job set.

## Relationship with dynamic fees

The current policies are connected in limited but important ways:

- the dust bootstrap amount is derived from the dynamic fee depleted threshold
- ordinary Sling budgets use TPPM and historical effective PPM
- an ordinary budget is normally capped at the current advertised channel PPM
- source candidates must advertise a low outbound PPM

They do not yet share a unified market-price estimate, inventory multiplier,
replacement-cost floor, demand cap, or source opportunity-cost estimate.

The intended future direction is specified in
`DYNAMIC_FEE_STRATEGY.md`: preserve small operations, but replenish only
demand-supported liquidity whose expected forwarding revenue exceeds realized
rebalance cost, source opportunity cost, and the required profit margin.

## Current limitations

- Target eligibility is based on balance, not recent proven demand.
- A persistent job may replenish more than recent routed demand because
  `target=0.5` is not a total-amount cap.
- The dust bootstrap can pay 1,100 PPM without realized forwarding history.
- Candidate selection uses current advertised PPM as a proxy for source value
  and does not include direct or indirect opportunity cost.
- TPPM and historical effective PPM mix variable-only and total-fee meanings.
- Historical metrics use sat-truncated forwarding fees.
- Rebalance profitability is not checked later using the realized Sling cost.
- The fixed 1,000,000-sat candidate depletion cap weakens the intended 50%
  floor on channels larger than 2,000,000 sats.
- Executing with no eligible candidates still stops and deletes all existing
  jobs before returning.

These are descriptions of the current policy, not reasons to reintroduce
failed forwarding attempts as demand. Only settled traffic should be used for
future economic target selection.
