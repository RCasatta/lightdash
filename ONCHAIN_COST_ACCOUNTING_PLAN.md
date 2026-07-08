# Onchain Cost Accounting Plan

## Goal

Track onchain costs without adding a separate database:

- Locally paid channel opening fees should be charged to the opened channel.
- Closing transaction costs paid by us should be included in global net ROIC.
- Existing rebalance cost accounting should remain based on Core Lightning bookkeeper data.

## Primary Data Source

Use Core Lightning `bkpr-listaccountevents`.

The fixture already contains useful tags:

- `channel_open`
- `channel_close`
- `onchain_fee`
- `to_wallet`
- `anchor`

Prefer bookkeeper `onchain_fee` rows over reconstructing Bitcoin transaction fees from raw transactions. Bookkeeper appears to split fees across channel accounts, which is especially useful for batched opens.

## Required Parsed Fields

Extend `BkprAccountEvent` parsing to include fields needed for onchain attribution:

- `account`
- `tag`
- `credit_msat`
- `debit_msat`
- `timestamp`
- `outpoint`
- `txid`
- `blockheight`

For costs, use signed net accounting:

```text
net_fee_msat = credit_msat - debit_msat
```

Only positive net fees should be treated as costs. This avoids double-counting wallet bookkeeping rows that can cancel out.

## Account Mapping

Build a stronger account-to-channel map using:

- `listfunds.channels` for active channels
- `listclosedchannels` for closed channels
- `bkpr-listbalances` if needed for `we_opened`, closed/resolved state, and account metadata

The map should connect:

```text
bookkeeper account/channel_id -> short_channel_id, funding_txid, opener, closed status
```

This is needed because current mapping mainly covers active channels.

## Opening Fee Attribution

For each channel:

1. Determine whether it was locally opened.
2. If locally opened, sum positive net `onchain_fee` rows for that channel account that correspond to the funding transaction/opening flow.
3. Attribute that opening cost to the channel.
4. If remotely opened, opening cost for us should be zero.

Batch opens should work by summing the bookkeeper fee rows already attributed to each channel account.

## Closing Fee Attribution

For closed channels:

1. Find `channel_close` rows for the channel account.
2. Find related positive net `onchain_fee` rows on the same channel account, preferably by close `txid`.
3. Count those as closing costs paid by us.

For the first implementation, closing costs only need to affect global net ROIC. Preserve per-channel close fee data internally so it can be displayed later.

## ROIC Changes

Global net ROIC should use:

```text
forwarding fees
- rebalance costs
- opening onchain costs
- closing onchain costs paid by us
```

Do not include unrelated wallet withdrawals/deposits unless they are clearly tied to channel opens/closes.

Per-channel net routing revenue can later use:

```text
forwarding fees
- attributed rebalance cost
- local opening onchain cost
```

Closed-channel close costs can be displayed separately if a closed-channel detail view is added.

## UI Changes

Channel page:

- Opening Onchain Cost
- Net Routing Revenue After Opening Cost

APY / ROIC page:

- Total Forwarding Fees
- Total Rebalance Cost
- Total Opening Onchain Cost
- Total Closing Onchain Cost
- Net After All Costs
- Net ROIC

## Tests

Add focused fixtures/tests for:

- Locally opened channel with `channel_open` plus `onchain_fee`
- Remotely opened channel has zero opening cost
- Batch open where bookkeeper splits fees across two channel accounts
- Closing fee on a channel account
- Wallet `onchain_fee` credit/debit pair nets to zero or is ignored
- Closed channel still maps account to `short_channel_id`

## Validation

Use the project standard validation commands:

```sh
direnv exec . cargo fmt
direnv exec . cargo test --quiet
direnv exec . cargo test --quiet --features large-fixture-tests
direnv exec . cargo clippy --quiet -- -D warnings
```

