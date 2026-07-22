# Lightning Node Income and Capital Allocation Guide

This document collects general lessons for improving the income of a routing
node. It deliberately avoids recommendations about particular peers or
channels. Its purpose is to make future analyses consistent, capital-aware,
and less vulnerable to attractive but misleading short-term results.

## Optimize capital turnover, not gross forwarding volume

A routing node earns sustainable income when the same local capital can be used
repeatedly. A channel that forwards a large amount once and then remains empty
may show impressive volume and fees, but it has completed only one capital
turnover.

The key distinction is:

- **Demand:** traffic wants to leave through the channel.
- **Circulation:** liquidity also returns through routing, rebalancing, leases,
  swaps, or another economically justified mechanism.

Demand without circulation turns local liquidity into remote liquidity. That
can still be a profitable liquidity sale, but it is not automatically a good
long-term capital allocation.

## Do not mechanically replenish sink channels

A sink channel consistently moves liquidity away from the node without routing
it back. Replenishing it can earn quick forwarding fees, especially when its
outbound price is high, but repeated replenishment can progressively unbalance
the node.

Before replenishing a depleted channel, ask:

1. How many times has its capital turned over, rather than how much did it route
   once?
2. Is there measurable incoming flow or rebalance-source value?
3. Can liquidity be returned at a cost below the revenue earned by sending it
   out?
4. Is the proposed addition expected to earn repeatedly, or only once?
5. What other channel could use the same local capital?
6. What will it cost to recover the funds later through a splice-out, close,
   swap, or rebalance?

A small experimental replenishment can be reasonable when demand is clearly
capacity-constrained and on-chain fees are low. Treat it as a bounded liquidity
sale:

- Add substantially less than the amount that recently drained.
- Measure marginal revenue on the newly added capital.
- Do not automatically add a second tranche.
- Require evidence of return flow, profitable rebalancing, or sufficiently high
  one-way revenue before repeating it.

Low on-chain fees improve the economics of entering and eventually recovering
capital. They do not solve the absence of return flow.

## Evidence that a channel is capital-constrained

Low local balance alone does not prove that a splice-in will earn money. Strong
evidence combines several signals:

- Persistent low local or spendable balance.
- Recent profitable outbound forwarding, not only lifetime activity.
- Repeated local temporary-channel failures while the channel is depleted.
- A realized fee rate high enough to cover replenishment and opportunity costs.
- Low or economically justified target-rebalance cost.
- Continued demand after fee discovery has had enough time to operate.

Failed forwarding attempts need careful interpretation. Remote failures,
expiry failures, fee failures, probing, and repeated attempts are not all
evidence of missing local liquidity. Prefer failures generated locally on the
candidate outgoing channel, and do not treat attempted volume as unique demand:
the same payment may have been retried many times.

## Splice-in candidate rule

A strong splice-in candidate should satisfy all of the following:

1. The peer currently supports splicing.
2. The channel is connected and in a normal operating state.
3. Local outbound liquidity is genuinely constrained.
4. Recent direct revenue is economically meaningful.
5. The channel also has circulation, or its one-way yield justifies treating the
   splice as a liquidity sale.
6. The addition is small enough to measure marginal demand.
7. Wallet reserves and transaction fees remain acceptable.

Good historical routing economics do not establish a present capacity
constraint. Conversely, a recently depleted young channel may show a very high
annualized return based on one short burst. Use recent absolute revenue,
realized PPM, and capital turnover alongside annualized percentages.

Adding local funds is also directional. If a channel's main value is incoming
traffic and it already has ample local balance, a splice-in adds liquidity to
the wrong side. It does not create additional remote balance for the peer to
send toward the node.

## Direct and indirect channel value

Direct fees are earned when a channel is the outgoing side of a forward.
Indirect fees attribute the node's earned fee to the channel that supplied the
incoming side of that forward.

Indirect fees are important for channel evaluation, but they are not additional
node revenue. The same forwarding fee is earned once and can be viewed from two
roles:

- The outgoing channel monetized the forward.
- The incoming channel supplied the route and liquidity that made it possible.

This distinction prevents two mistakes:

- Double-counting indirect attribution as extra node income.
- Closing an apparently unprofitable incoming channel that enables profitable
  outgoing routes elsewhere.

Capacity decisions should therefore consider direct net revenue and indirect
route contribution together.

## ROIC must use the correct capital and revenue

Full public channel capacity is not the node's invested capital. It contains
both local and remote balances. The remote balance belongs economically to the
peer and cannot be redeployed by closing or splicing out the channel.

For node-level ROIC, use local deployed capital. For channel-level historical
ROIC, the ideal denominator is time-weighted average local capital over the same
measurement window. Current local balance is only a proxy because routing moves
liquidity between channels.

For a proposed splice-in, the marginal capital is known exactly: it is the
amount being added. Evaluate the marginal fees produced by that tranche rather
than assuming that the channel's previous percentage return will apply to the
new capacity.

ROIC should include every economically relevant cash flow in the same period:

```text
net revenue
  = forwarding revenue
  + lease-fee earnings
  - lease-fee costs
  - attributed rebalance costs
```

Lease earnings are real income and belong in gross and net ROIC. Lease fees paid
to obtain liquidity are costs. Keep all windows aligned: a twelve-month return
must use twelve-month forwarding fees, lease flows, rebalance costs, and an
appropriate twelve-month capital estimate.

Do not include lease revenue in a routing-price metric such as effective PPM or
fees per routed amount. Those metrics describe the price of forwarding, whereas
lease income is earned through a different mechanism.

## Fee discovery needs time

No recent outbound forwarding does not necessarily mean a channel lacks
demand. It may still be in fee discovery, especially when it started with a
high fee and the controller is gradually reducing it.

Avoid closing or permanently classifying a channel based on a short idle
period. Review:

- Channel age and how long the present fee policy has been active.
- Historical fee changes.
- Recent time-decayed realized PPM.
- Whether traffic resumes after fee reductions.
- Peer availability and HTLC limits.
- Direct and incoming-side activity over multiple windows.

A fee controller should explore without becoming trapped at an uneconomic
price. More aggressive reductions and a higher temporary cap can accelerate
discovery, but parameters should be evaluated through realized revenue and
traffic response rather than adopted as universal constants.

## Rebalance-source selection must preserve exploration

A channel with little observed outbound demand can be a useful rebalance source,
but lack of traffic is not sufficient by itself. The channel may still be
discovering its fee or may provide valuable incoming routes.

A source-selection rule should consider:

- Current local balance and whether it is genuinely surplus.
- Recent and historical outbound demand.
- Incoming-side route contribution.
- Channel age and fee-discovery state.
- Peer availability.
- The source channel's effect on node balance after the rebalance.
- The target channel's expected marginal revenue.

The correct rebalance is not merely one that succeeds below a PPM cap. It should
move liquidity from a lower-opportunity location to a higher-opportunity one at
a cost that preserves positive expected net revenue.

## Splice support comes from peer features

`listchannels` primarily exposes public gossip and directional channel policy.
Its channel feature bitmap is not the authoritative signal for whether the
currently connected peer negotiated splice support.

Use the peer INIT feature bitmap from `listpeers.features`. BOLT 9 assigns
`option_splice` to required/optional feature bits 62 and 63. A dashboard field
derived from this source should be nullable:

- `true`: the peer advertises either splice bit.
- `false`: peer INIT features are available and contain neither bit.
- `null`: the peer feature bitmap is unavailable.

The value is connection-negotiated and may change after a reconnect or peer
software upgrade. Store its source and caveat in snapshot metadata instead of
duplicating explanatory text in HTML templates.

## Safe splice procedure

Before executing a splice:

1. Confirm the node version and known splice fixes.
2. Confirm peer connectivity, channel state, and negotiated splice support.
3. Inspect current spendable balance and wallet reserves.
4. Inspect the splice fee estimate.
5. Use the safe `check` RPC to validate command parameters and state.
6. Execute one splice at a time.
7. Record the returned transaction ID and inspect the channel's inflight funding
   candidates.
8. Regenerate analytical data after the operation.

Generic commands:

```bash
lightning-cli getinfo | jq -r .version
lightning-cli listpeerchannels -k channel_id=<full-channel-id>
lightning-cli feerates perkw | jq '.perkw | {splice, opening, floor}'

lightning-cli check -k command_to_check=splicein \
  channel=<full-channel-id> amount=<amount>sat

lightning-cli splicein <full-channel-id> <amount>sat
```

`check` is useful but cannot guarantee that the complete stateful workflow will
succeed. Plugin crashes, peer behavior, disconnections, and transaction
negotiation can still fail after the command begins.

## CLN partial-millisatoshi splice bug

Core Lightning v26.04.1 contains a `spenderp` bug triggered when a channel's
available balance contains a fractional satoshi. The plugin tried to convert an
exact millisatoshi amount to satoshis; a value not divisible by 1,000 caused the
plugin to terminate. Because `spenderp` is an important plugin, its termination
could cause `lightningd` to shut down after the channel entered quiescence.

Upstream fixed this in PR #9097 and commit
`30a6b4c41a01c8830a5e7553202bf1ad6d63b1fc` by rounding the available channel
balance down to whole satoshis. The fix is included in v26.06 and later.

Operational rules:

- Do not retry this failure on the affected build merely because the requested
  splice amount is expressed in whole sats; the problematic value is the
  existing channel balance.
- Upgrade to v26.06 or newer before retrying.
- After a crash, verify the service, channel state, inflight splice candidates,
  wallet transactions, and whether the original funding output remains unspent.
- STFU/quiescence alone is not evidence that a splice transaction was signed or
  broadcast.

Upstream references:

- <https://github.com/ElementsProject/lightning/pull/9097>
- <https://github.com/ElementsProject/lightning/commit/30a6b4c41a01c8830a5e7553202bf1ad6d63b1fc>

## Snapshot and dashboard data principles

The analytical snapshot should be the contract between node collection and the
dashboard. Dashboard code should not query the node or independently reproduce
financial formulas.

For every exported metric, snapshot metadata should be the single source of
truth for:

- Description and economic meaning.
- Unit and nullability.
- Source RPC or dataset.
- Formula and aggregation rule.
- Important warnings and interpretation caveats.

Dashboard column descriptions and tooltips should be generated from that
metadata, not duplicated in HTML or JavaScript. Contract changes require a
schema-version increment so stale dashboards cannot silently misinterpret new
data.

Keep successful forwards separate from noisy failed and offered attempts.
Successful forwards measure realized economics. Failed attempts are useful for
diagnosing constraints but can contain spam, probes, retries, and failures
outside the node's control.

## Decision checklist

Before adding capital to a channel, answer these questions:

- Is the peer capable of the requested operation now?
- Is outbound liquidity actually constrained?
- What did the channel earn in the last 30, 90, and 360 days?
- How much of that income was forwarding, leasing, and rebalance-adjusted?
- What incoming-side value does the channel provide?
- Has capital circulated, or did it drain once?
- What is the expected revenue on the next marginal tranche?
- What are the on-chain, rebalance, and opportunity costs?
- What observation would justify a second tranche?
- What observation would cause the experiment to stop?

The best expansion target is not necessarily the channel with the highest
historical return, lowest balance, or most failed attempts. It is the channel
where additional local capital has the strongest evidence of producing
repeatable net income without degrading the balance and optionality of the rest
of the node.
