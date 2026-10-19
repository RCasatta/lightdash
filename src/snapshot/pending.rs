//! Funds of closing channels that have not yet returned to the on-chain wallet.
//!
//! `listfunds.channels[].our_amount_msat` keeps reporting the last channel balance until
//! Core Lightning forgets the channel, while swept outputs already show up in
//! `listfunds.outputs`. Tracing the closing transactions avoids counting those funds twice.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::cmd::{Fund, Output, Transaction};

/// Index of wallet transactions and outputs used to trace channel closing transactions.
pub(super) struct ClosingFunds<'a> {
    /// Wallet-owned outputs, spent or not, keyed by outpoint.
    wallet_outputs: HashMap<(&'a str, u32), u64>,
    /// Total wallet-owned output value created by each transaction.
    wallet_credit_msat: HashMap<&'a str, u64>,
    /// Transactions spending each outpoint, grouped by the spent txid.
    spenders: HashMap<&'a str, Vec<(u32, &'a Transaction)>>,
}

impl<'a> ClosingFunds<'a> {
    pub(super) fn new(transactions: &'a [Transaction], wallet_outputs: &'a [Output]) -> Self {
        let mut wallet_credit_msat: HashMap<&str, u64> = HashMap::new();
        for output in wallet_outputs {
            *wallet_credit_msat.entry(output.txid.as_str()).or_default() += output.amount_msat;
        }
        let wallet_outputs = wallet_outputs
            .iter()
            .map(|output| ((output.txid.as_str(), output.output), output.amount_msat))
            .collect();
        let mut spenders: HashMap<&str, Vec<(u32, &Transaction)>> = HashMap::new();
        for transaction in transactions {
            for input in &transaction.inputs {
                spenders
                    .entry(input.txid.as_str())
                    .or_default()
                    .push((input.index, transaction));
            }
        }
        Self {
            wallet_outputs,
            wallet_credit_msat,
            spenders,
        }
    }

    /// Estimated channel balance still waiting to return to the wallet.
    pub(super) fn pending_msat(&self, channel: &Fund) -> u64 {
        if channel.state == "CHANNELD_AWAITING_SPLICE" {
            // A splice spends the funding output without closing the channel.
            return channel.our_amount_msat;
        }
        channel
            .our_amount_msat
            .saturating_sub(self.returned_msat(channel))
    }

    /// Wallet value created by the transactions descending from the funding output.
    ///
    /// The walk follows only outputs the wallet does not own, such as commitment and
    /// second-stage HTLC outputs, so later spends of swept funds are not attributed to the
    /// channel. Wallet inputs added to pay fees are netted out of each transaction's credit.
    fn returned_msat(&self, channel: &Fund) -> u64 {
        let mut returned_msat = 0;
        let mut visited = HashSet::new();
        let mut queue: VecDeque<&Transaction> = self
            .spenders_of(&channel.funding_txid, channel.funding_output)
            .collect();
        while let Some(transaction) = queue.pop_front() {
            if !visited.insert(transaction.hash.as_str()) {
                continue;
            }
            let credit_msat = self
                .wallet_credit_msat
                .get(transaction.hash.as_str())
                .copied()
                .unwrap_or_default();
            let debit_msat: u64 = transaction
                .inputs
                .iter()
                .filter_map(|input| self.wallet_outputs.get(&(input.txid.as_str(), input.index)))
                .sum();
            returned_msat += credit_msat.saturating_sub(debit_msat);

            for (index, spender) in self
                .spenders
                .get(transaction.hash.as_str())
                .into_iter()
                .flatten()
            {
                if !self
                    .wallet_outputs
                    .contains_key(&(transaction.hash.as_str(), *index))
                {
                    queue.push_back(spender);
                }
            }
        }
        returned_msat
    }

    fn spenders_of(&self, txid: &str, index: u32) -> impl Iterator<Item = &'a Transaction> + '_ {
        self.spenders
            .get(txid)
            .into_iter()
            .flatten()
            .filter(move |(spent_index, _)| *spent_index == index)
            .map(|(_, transaction)| *transaction)
    }
}

#[cfg(test)]
mod tests {
    use super::ClosingFunds;
    use crate::cmd::{Fund, Output, Transaction, TransactionInput};

    fn transaction(hash: &str, inputs: &[(&str, u32)]) -> Transaction {
        Transaction {
            hash: hash.to_string(),
            inputs: inputs
                .iter()
                .map(|(txid, index)| TransactionInput {
                    txid: txid.to_string(),
                    index: *index,
                })
                .collect(),
        }
    }

    fn output(txid: &str, output: u32, amount_msat: u64) -> Output {
        Output {
            txid: txid.to_string(),
            output,
            amount_msat,
        }
    }

    fn closing_channel(state: &str, our_amount_msat: u64) -> Fund {
        Fund {
            peer_id: "peer".to_string(),
            connected: false,
            state: state.to_string(),
            channel_id: "channel".to_string(),
            short_channel_id: Some("1x1x0".to_string()),
            our_amount_msat,
            amount_msat: 4_000_000_000,
            funding_txid: "funding".to_string(),
            funding_output: 0,
        }
    }

    /// Mirrors a unilateral close of our commitment with one outgoing HTLC: the to_local
    /// sweep reached the wallet, while the HTLC-timeout output is still under its delay.
    #[test]
    fn unilateral_close_counts_only_unswept_claims() {
        let transactions = vec![
            transaction("commitment", &[("funding", 0)]),
            transaction("to-local-sweep", &[("commitment", 4)]),
            // HTLC-timeout paying its fee with a wallet input and returning change.
            transaction("htlc-timeout", &[("commitment", 2), ("wallet-utxo", 0)]),
            // Peer sweeping its own to_remote output.
            transaction("peer-sweep", &[("commitment", 3)]),
            // A later channel open that spends the HTLC-timeout change.
            transaction("new-funding", &[("htlc-timeout", 1)]),
        ];
        // Wallet outputs come from `listfunds true`, so spent ones are included.
        let wallet_outputs = vec![
            output("wallet-utxo", 0, 20_000_000),
            output("to-local-sweep", 0, 3_417_239_000),
            output("htlc-timeout", 1, 9_715_000),
            output("new-funding", 1, 1_062_284_000),
        ];
        let closing = ClosingFunds::new(&transactions, &wallet_outputs);

        assert_eq!(
            closing.pending_msat(&closing_channel("ONCHAIN", 3_499_710_478)),
            82_471_478
        );
    }

    #[test]
    fn spent_mutual_close_output_is_not_pending() {
        let transactions = vec![
            transaction("mutual-close", &[("funding", 0)]),
            transaction("reuse", &[("mutual-close", 1)]),
        ];
        let wallet_outputs = vec![output("mutual-close", 1, 2_538_833_000)];
        let closing = ClosingFunds::new(&transactions, &wallet_outputs);

        assert_eq!(
            closing.pending_msat(&closing_channel("ONCHAIN", 2_539_000_000)),
            167_000
        );
    }

    #[test]
    fn channel_without_closing_transactions_keeps_its_balance() {
        let closing = ClosingFunds::new(&[], &[]);

        assert_eq!(
            closing.pending_msat(&closing_channel("CLOSINGD_SIGEXCHANGE", 1_000_000)),
            1_000_000
        );
    }

    #[test]
    fn splice_wallet_change_is_not_treated_as_returned_funds() {
        let transactions = vec![transaction("splice", &[("funding", 0)])];
        let wallet_outputs = vec![output("splice", 1, 500_000_000)];
        let closing = ClosingFunds::new(&transactions, &wallet_outputs);

        assert_eq!(
            closing.pending_msat(&closing_channel("CHANNELD_AWAITING_SPLICE", 1_000_000_000)),
            1_000_000_000
        );
    }
}
