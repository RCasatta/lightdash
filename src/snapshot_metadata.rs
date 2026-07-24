use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::snapshot::SnapshotFiles;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct FieldMetadata {
    pub json_type: String,
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formula: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DatasetMetadata {
    pub path: String,
    pub schema_path: String,
    pub format: String,
    pub description: String,
    pub record_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<String>,
    pub fields: BTreeMap<String, FieldMetadata>,
}

#[derive(Clone, Copy)]
pub(crate) struct DatasetCounts {
    pub channels: usize,
    pub closed_channels: usize,
    pub settled_forwards: usize,
    pub other_forwards: usize,
    pub rebalances: usize,
    pub rebalance_status: usize,
}

pub(crate) fn build_dataset_metadata(
    files: &SnapshotFiles,
    counts: DatasetCounts,
) -> BTreeMap<String, DatasetMetadata> {
    BTreeMap::from([
        (
            "summary".to_string(),
            dataset(
                &files.summary,
                "summary.schema.json",
                "json-object",
                "Node-level balances, counts, routing revenue, and ROIC metrics.",
                1,
                None,
                summary_fields(),
            ),
        ),
        (
            "channels".to_string(),
            dataset(
                &files.channels,
                "channels.schema.json",
                "json-array",
                "Current local channels enriched with peer, routing, rebalance, and return metrics.",
                counts.channels,
                Some("channel_id"),
                channel_fields(),
            ),
        ),
        (
            "closed_channels".to_string(),
            dataset(
                &files.closed_channels,
                "closed-channels.schema.json",
                "json-array",
                "Closed local channels with final balances and lifetime return attribution.",
                counts.closed_channels,
                Some("channel_id"),
                closed_channel_fields(),
            ),
        ),
        (
            "settled_forwards".to_string(),
            dataset(
                &files.settled_forwards,
                "settled-forwards.schema.json",
                "jsonl",
                "Successfully settled forwarding attempts, one JSON object per line.",
                counts.settled_forwards,
                None,
                forward_fields(),
            ),
        ),
        (
            "other_forwards".to_string(),
            dataset(
                &files.other_forwards,
                "other-forwards.schema.json",
                "jsonl",
                "Non-settled forwarding attempts such as failed, local_failed, offered, or pending records.",
                counts.other_forwards,
                None,
                forward_fields(),
            ),
        ),
        (
            "rebalances".to_string(),
            dataset(
                &files.rebalances,
                "rebalances.schema.json",
                "jsonl",
                "Matched Core Lightning bookkeeper debit and credit events representing rebalance payment parts.",
                counts.rebalances,
                None,
                rebalance_fields(),
            ),
        ),
        (
            "rebalance_status".to_string(),
            dataset(
                &files.rebalance_status,
                "rebalance-status.schema.json",
                "json-array",
                "Latest per-channel Sling rebalance status and outcome summary.",
                counts.rebalance_status,
                Some("short_channel_id"),
                rebalance_status_fields(),
            ),
        ),
    ])
}

pub(crate) fn field_tooltip(dataset_name: &str, field_name: &str) -> Option<String> {
    let fields = match dataset_name {
        "summary" => summary_fields(),
        "channels" => channel_fields(),
        "closed_channels" => closed_channel_fields(),
        "settled_forwards" | "other_forwards" => forward_fields(),
        "rebalances" => rebalance_fields(),
        "rebalance_status" => rebalance_status_fields(),
        _ => return None,
    };
    let metadata = fields.get(field_name)?;
    let mut parts = vec![metadata.description.clone()];
    if let Some(unit) = &metadata.unit {
        parts.push(format!("Unit: {unit}"));
    }
    if let Some(formula) = &metadata.formula {
        parts.push(format!("Formula: {formula}"));
    }
    if let Some(source) = &metadata.source {
        parts.push(format!("Source: {source}"));
    }
    if let Some(aggregation) = &metadata.aggregation {
        parts.push(format!("Aggregation: {aggregation}"));
    }
    if let Some(warning) = &metadata.warning {
        parts.push(format!("Warning: {warning}"));
    }
    Some(parts.join("\n"))
}

fn dataset(
    path: &str,
    schema_path: &str,
    format: &str,
    description: &str,
    record_count: usize,
    primary_key: Option<&str>,
    fields: BTreeMap<String, FieldMetadata>,
) -> DatasetMetadata {
    DatasetMetadata {
        path: path.to_string(),
        schema_path: schema_path.to_string(),
        format: format.to_string(),
        description: description.to_string(),
        record_count,
        primary_key: primary_key.map(str::to_string),
        fields,
    }
}

fn field(json_type: &str, nullable: bool, unit: Option<&str>, description: &str) -> FieldMetadata {
    FieldMetadata {
        json_type: json_type.to_string(),
        nullable,
        unit: unit.map(str::to_string),
        description: description.to_string(),
        formula: None,
        source: None,
        aggregation: None,
        warning: None,
    }
}

fn formula(mut field: FieldMetadata, value: &str) -> FieldMetadata {
    field.formula = Some(value.to_string());
    field
}

fn source(mut field: FieldMetadata, value: &str) -> FieldMetadata {
    field.source = Some(value.to_string());
    field
}

fn aggregation(mut field: FieldMetadata, value: &str) -> FieldMetadata {
    field.aggregation = Some(value.to_string());
    field
}

fn warning(mut field: FieldMetadata, value: &str) -> FieldMetadata {
    field.warning = Some(value.to_string());
    field
}

fn summary_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("node_id".into(), source(field("string", false, None, "Public key identifying the local Core Lightning node."), "getinfo.id")),
        ("block_height".into(), source(field("integer", false, Some("block"), "Bitcoin block height reported when the snapshot was collected."), "getinfo.blockheight")),
        ("peer_count".into(), source(field("integer", false, Some("peer"), "Number of entries returned by listpeers."), "listpeers")),
        ("network_channel_count".into(), source(field("integer", false, Some("channel_direction"), "Number of directed public channel announcements returned by listchannels."), "listchannels")),
        ("current_channel_count".into(), source(field("integer", false, Some("channel"), "Number of current local channel records in listfunds, including non-normal states."), "listfunds.channels")),
        ("normal_channel_count".into(), field("integer", false, Some("channel"), "Number of current local channels whose state is CHANNELD_NORMAL.")),
        ("closed_channel_count".into(), source(field("integer", false, Some("channel"), "Number of channels returned by listclosedchannels."), "listclosedchannels")),
        ("forward_attempt_count".into(), source(field("integer", false, Some("attempt"), "Total number of forwarding records across every status."), "listforwards")),
        ("settled_forward_count".into(), source(field("integer", false, Some("forward"), "Number of forwarding records whose status is settled."), "listforwards filtered to status=settled")),
        ("onchain_balance_msat".into(), source(field("integer", false, Some("msat"), "Sum of spendable on-chain outputs reported by listfunds."), "sum(listfunds.outputs.amount_msat)")),
        ("channel_funds_sat".into(), warning(formula(field("integer", false, Some("sat"), "Current local balance held in CHANNELD_NORMAL channels."), "sum(normal_channel.local_balance_msat) / 1000"), "This is local balance, not the sum of full channel capacities.")),
        ("normal_channel_capacity_sat".into(), formula(field("integer", false, Some("sat"), "Full capacity of all current CHANNELD_NORMAL channels."), "sum(normal_channel.capacity_msat) / 1000")),
        ("channel_funds_percent_of_capacity".into(), formula(field("number", true, Some("percent"), "Current local balance as a percentage of the full capacity of CHANNELD_NORMAL channels."), "channel_funds_sat / normal_channel_capacity_sat * 100")),
        ("channel_balance_target_stddev_percentage_points".into(), aggregation(formula(field("number", false, Some("percentage_point"), "Root-mean-square distance of normal-channel local-balance percentages from the 50% target."), "sqrt(mean((normal_channel.local_balance_msat / normal_channel.capacity_msat - 0.5)^2)) * 100"), "Each normal channel has equal weight; this measures distance from a fixed target, not dispersion around the observed mean.")),
        ("network_average_fee_ppm".into(), aggregation(source(field("number", false, Some("ppm"), "Arithmetic mean variable fee across qualifying announced Lightning Network channel directions."), "listchannels filtered to base_fee_millisatoshi = 0 and fee_per_millionth <= 10000"), "Each qualifying directed announcement has equal weight.")),
        ("network_median_fee_ppm".into(), aggregation(source(field("number", false, Some("ppm"), "Median variable fee across qualifying announced Lightning Network channel directions."), "listchannels filtered to base_fee_millisatoshi = 0 and fee_per_millionth <= 10000"), "Each qualifying directed announcement has equal weight.")),
        ("node_average_fee_ppm".into(), aggregation(source(field("number", false, Some("ppm"), "Arithmetic mean of the local node's announced outbound variable fees for CHANNELD_NORMAL channels with resolvable announcements."), "listfunds CHANNELD_NORMAL channels joined to listchannels where source = local node"), "Each resolved normal channel has equal weight; channels without an announced outbound direction are omitted.")),
        ("node_median_fee_ppm".into(), aggregation(source(field("number", false, Some("ppm"), "Median of the local node's announced outbound variable fees for CHANNELD_NORMAL channels with resolvable announcements."), "listfunds CHANNELD_NORMAL channels joined to listchannels where source = local node"), "Each resolved normal channel has equal weight; channels without an announced outbound direction are omitted.")),
        ("total_forwarding_fees_sat".into(), warning(formula(field("integer", false, Some("sat"), "All-time fees earned by settled forwards, truncated from millisatoshis to satoshis per forward."), "sum(floor(settled_forward.fee_msat / 1000))"), "Sub-satoshi fee precision is discarded before summing.")),
        ("total_rebalance_cost_msat".into(), source(field("integer", false, Some("msat"), "All matched rebalance fees from Core Lightning bookkeeper events."), "bkpr-listaccountevents matched debit/credit parts")),
        ("net_routing_revenue_msat".into(), formula(field("integer", false, Some("msat"), "All-time forwarding revenue after subtracting matched rebalance costs."), "total_forwarding_fees_sat * 1000 - total_rebalance_cost_msat")),
        ("roic.periods[].months".into(), warning(field("integer", false, Some("30_day_month"), "Lookback length used for the period ROIC record."), "Each month is approximated as 30 days.")),
        ("roic.periods[].forwarding_fees_sat".into(), field("integer", false, Some("sat"), "Settled forwarding fees earned inside the period.")),
        ("roic.periods[].lease_fee_earnings_msat".into(), source(field("integer", false, Some("msat"), "Liquidity-ad lease fees credited to the node inside the period."), "bkpr-listincome lease_fee credit_msat")),
        ("roic.periods[].average_channel_funds_sat".into(), warning(formula(field("number", false, Some("sat"), "Time-weighted average local balance in CHANNELD_NORMAL channels over the covered part of the lookback period; this is the deployed-capital denominator for period ROIC."), "sum(channel_funds_msat * seconds_until_next_change) / covered_seconds / 1000"), "When processed history has no coverage, this falls back to the current channel balance and capital_history_coverage_ratio is zero.")),
        ("roic.periods[].capital_history_coverage_ratio".into(), formula(field("number", false, Some("ratio"), "Fraction of the requested lookback period covered by channel-funds history."), "covered_seconds / (months * 30 * 86400)")),
        ("roic.periods[].annualized_gross_roic_percent".into(), aggregation(formula(field("number", false, Some("percent"), "Annualized gross node return from forwarding and earned lease fees, using time-weighted average local channel funds as deployed capital."), "(period_forwarding_fees_sat * 1000 + period_lease_fee_earnings_msat) * (12 / months) / (average_channel_funds_sat * 1000) * 100"), "Do not average this value with channel-level capacity-return values.")),
        ("roic.routed_12_months_sat".into(), field("integer", false, Some("sat"), "Outgoing amount of settled forwards resolved within the trailing 360 days.")),
        ("roic.capital_velocity_12_months".into(), formula(field("number", false, Some("ratio"), "Trailing routed volume divided by time-weighted average local channel funds for the twelve-month period."), "routed_12_months_sat / periods[months = 12].average_channel_funds_sat")),
        ("roic.effective_fee_rate_12_months_bps".into(), warning(formula(field("number", false, Some("bps"), "Forwarding fees earned per routed amount over the trailing 360 days."), "forwarding_fees_12_months_sat * 10000 / routed_12_months_sat"), "Lease fees are included in ROIC but excluded from this routing-price metric.")),
        ("roic.lease_fee_earnings_12_months_msat".into(), source(warning(field("integer", false, Some("msat"), "Liquidity-ad lease fees credited to the node during the trailing 360 days."), "The twelve-month window is implemented as 12 × 30 days."), "bkpr-listincome lease_fee credit_msat")),
        ("roic.lease_fee_cost_12_months_msat".into(), source(warning(field("integer", false, Some("msat"), "Liquidity-ad lease fees debited from the node during the trailing 360 days."), "The twelve-month window is implemented as 12 × 30 days."), "bkpr-listincome lease_fee debit_msat")),
        ("roic.rebalance_cost_12_months_msat".into(), warning(field("integer", false, Some("msat"), "Matched rebalance fees whose timestamp falls within the trailing 360 days."), "The twelve-month window is implemented as 12 × 30 days.")),
        ("roic.net_roic_12_months_percent".into(), formula(field("number", false, Some("percent"), "Annualized trailing net return from forwarding and lease fees after subtracting lease-fee and rebalance costs, divided by time-weighted average local channel funds."), "(forwarding_fees_12_months_sat * 1000 + lease_fee_earnings_12_months_msat - lease_fee_cost_12_months_msat - rebalance_cost_12_months_msat) / (periods[months = 12].average_channel_funds_sat * 1000) * 100")),
    ])
}

fn channel_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("channel_id".into(), source(field("string", false, None, "Full local channel identifier."), "listfunds.channels.channel_id")),
        ("short_channel_id".into(), source(field("string", true, None, "Block-height based short channel identifier when assigned."), "listfunds.channels.short_channel_id")),
        ("funding_txid".into(), source(field("string", false, None, "Transaction ID of the channel funding transaction."), "listfunds.channels.funding_txid")),
        ("funding_output".into(), source(field("integer", false, Some("vout"), "Output index of the channel funding transaction."), "listfunds.channels.funding_output")),
        ("peer_id".into(), source(field("string", false, None, "Public key of the remote channel peer."), "listfunds.channels.peer_id")),
        ("peer_alias".into(), source(field("string", false, None, "Public gossip alias for the remote peer, or an abbreviated node ID when unavailable."), "listnodes.alias")),
        ("connected".into(), source(field("boolean", false, None, "Whether Core Lightning reports the peer connection as active."), "listfunds.channels.connected")),
        ("peer_supports_splicing".into(), source(warning(field("boolean", true, None, "Whether the peer's negotiated INIT feature bitmap includes BOLT 9 option_splice (required bit 62 or optional bit 63)."), "Null means the peer INIT features were unavailable. This connection-negotiated capability can change after a reconnect or software upgrade and is not derived from listchannels channel features."), "listpeers.features")),
        ("state".into(), source(field("string", false, None, "Core Lightning channel state."), "listfunds.channels.state")),
        ("is_normal".into(), formula(field("boolean", false, None, "Whether the channel state equals CHANNELD_NORMAL."), "state == 'CHANNELD_NORMAL'")),
        ("capacity_msat".into(), source(field("integer", false, Some("msat"), "Full channel capacity owned jointly by both sides."), "listfunds.channels.amount_msat")),
        ("local_balance_msat".into(), source(field("integer", false, Some("msat"), "Current channel balance controlled by the local node."), "listfunds.channels.our_amount_msat")),
        ("local_balance_percent".into(), formula(field("number", true, Some("percent"), "Local balance as a percentage of full channel capacity."), "local_balance_msat / capacity_msat * 100")),
        ("age_days".into(), warning(formula(field("integer", true, Some("day"), "Approximate channel age inferred from the short channel ID opening block."), "(snapshot_block_height - opening_block_height) / 144"), "Uses an assumed average of 144 Bitcoin blocks per day.")),
        ("uptime_ratio".into(), source(field("number", true, Some("ratio_0_to_1"), "Peer availability ratio measured by the Summars availability database."), "Summars availdb avail field")),
        ("outbound_fee_ppm".into(), source(field("integer", true, Some("ppm"), "Current proportional fee configured by the local node for outbound forwarding."), "listchannels direction sourced by the local node")),
        ("inbound_fee_ppm".into(), source(field("integer", true, Some("ppm"), "Current proportional fee advertised by the remote peer toward the local node."), "listchannels direction sourced by the peer")),
        ("outbound_base_fee_msat".into(), source(field("integer", true, Some("msat"), "Current fixed fee configured by the local node for outbound forwarding."), "listchannels direction sourced by the local node")),
        ("outbound_htlc_min_msat".into(), source(field("integer", true, Some("msat"), "Current minimum HTLC accepted by the local outbound policy."), "listchannels direction sourced by the local node")),
        ("outbound_htlc_max_msat".into(), source(field("integer", true, Some("msat"), "Current maximum HTLC accepted by the local outbound policy."), "listchannels direction sourced by the local node")),
        ("outbound_delay_blocks".into(), source(field("integer", true, Some("block"), "Current CLTV delta required by the local outbound policy."), "listchannels direction sourced by the local node")),
        ("last_fee_adjustment_at".into(), source(field("string", true, Some("rfc3339_utc"), "Most recent time Lightdash recorded changing this channel's outbound policy."), "datastore lightdash/last_setchannel/<short_channel_id>")),
        ("settled_forward_count".into(), aggregation(field("integer", false, Some("forward"), "Number of settled forwards where this channel was either incoming or outgoing."), "Do not sum across channels to obtain node forward count because a forward normally touches two channels.")),
        ("routed_out_sat".into(), field("integer", false, Some("sat"), "All-time outgoing amount of settled forwards using this channel as the outgoing side.")),
        ("forwarding_fees_sat".into(), warning(field("integer", false, Some("sat"), "All-time fees earned when this channel was the outgoing side of settled forwards."), "Each fee is truncated from millisatoshis to satoshis before aggregation.")),
        ("indirect_fees_sat".into(), warning(aggregation(field("integer", false, Some("sat"), "Fees attributed to this channel when it was the incoming side of settled forwards."), "Do not aggregate across the node as additional revenue; the same fee is earned on the paired outgoing channel."), "This is attribution, not revenue earned by the incoming channel.")),
        ("historical_effective_fee_ppm".into(), formula(field("number", true, Some("ppm"), "All-time effective outbound fee rate."), "forwarding_fees_sat * 1000000 / routed_out_sat")),
        ("time_decayed_variable_fee_ppm".into(), warning(formula(field("number", true, Some("ppm"), "Amount-weighted outbound fee rate with recent forwards weighted more heavily and an assumed one-sat base fee removed from every forward."), "sum(max(fee_sat - 1, 0) * 0.5^(age_seconds / 604800)) * 1000000 / sum(out_sat * 0.5^(age_seconds / 604800))"), "Uses a seven-day half-life and assumes the local base fee is one satoshi.")),
        ("rebalance_target_cost_msat".into(), field("integer", false, Some("msat"), "All matched rebalance fees attributed to payments targeting this channel.")),
        ("rebalance_target_credit_msat".into(), field("integer", false, Some("msat"), "Liquidity credited into this channel by matched rebalance parts.")),
        ("rebalance_effective_fee_ppm".into(), formula(field("number", true, Some("ppm"), "Effective cost of rebalancing liquidity into this channel."), "rebalance_target_cost_msat * 1000000 / rebalance_target_credit_msat")),
        ("rebalance_source_cost_msat".into(), warning(field("integer", false, Some("msat"), "Rebalance fees for payments where this channel was identified as the source."), "Source cost is informational and is not subtracted from this channel's net routing revenue.")),
        ("lease_fee_earnings_msat".into(), source(field("integer", false, Some("msat"), "All lease fees credited to this channel account by Core Lightning bookkeeper."), "bkpr-listincome lease_fee credit_msat joined by channel_id")),
        ("lease_fee_cost_msat".into(), source(field("integer", false, Some("msat"), "All lease fees debited from this channel account by Core Lightning bookkeeper."), "bkpr-listincome lease_fee debit_msat joined by channel_id")),
        ("net_routing_revenue_msat".into(), formula(field("integer", false, Some("msat"), "Outbound forwarding revenue after target-attributed rebalance cost."), "forwarding_fees_sat * 1000 - rebalance_target_cost_msat")),
        ("net_revenue_msat".into(), formula(field("integer", false, Some("msat"), "Forwarding and lease revenue after target-attributed rebalance and lease-fee costs."), "net_routing_revenue_msat + lease_fee_earnings_msat - lease_fee_cost_msat")),
        ("gross_capacity_return_percent".into(), aggregation(formula(field("number", true, Some("percent"), "Annualized lifetime forwarding and earned-lease return relative to full channel capacity."), "(forwarding_fees_sat * 1000 + lease_fee_earnings_msat) / capacity_msat * 365 / age_days * 100"), "Use capacity-weighting rather than a simple average when combining channel returns.")),
        ("net_capacity_return_percent".into(), aggregation(formula(field("number", true, Some("percent"), "Annualized lifetime capacity return after target-attributed rebalance and lease-fee costs."), "net_revenue_msat / capacity_msat * 365 / age_days * 100"), "Use capacity-weighting rather than a simple average when combining channel returns.")),
        ("indirect_capacity_contribution_percent".into(), warning(aggregation(formula(field("number", true, Some("percent"), "Annualized incoming-side fee attribution relative to full channel capacity."), "indirect_fees_sat * 1000 / capacity_msat * 365 / age_days * 100"), "Do not aggregate across the node as revenue."), "Revenue is earned on the paired outgoing channel; this metric describes the incoming channel's contribution.")),
    ])
}

fn closed_channel_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("channel_id".into(), field("string", false, None, "Full closed channel identifier.")),
        ("short_channel_id".into(), field("string", true, None, "Short channel identifier when known.")),
        ("peer_id".into(), field("string", true, None, "Remote peer public key when known.")),
        ("peer_alias".into(), field("string", true, None, "Remote peer gossip alias or abbreviated node ID.")),
        ("opener".into(), field("string", false, None, "Side reported by Core Lightning as channel opener.")),
        ("closer".into(), field("string", true, None, "Side reported by Core Lightning as channel closer.")),
        ("capacity_msat".into(), field("integer", false, Some("msat"), "Full channel capacity before closure.")),
        ("final_local_balance_msat".into(), field("integer", false, Some("msat"), "Final amount attributed to the local node.")),
        ("total_htlcs_sent".into(), field("integer", true, Some("htlc"), "Total HTLCs sent as reported by listclosedchannels.")),
        ("funding_txid".into(), field("string", false, None, "Funding transaction ID.")),
        ("last_commitment_txid".into(), field("string", true, None, "Last commitment transaction ID when reported.")),
        ("last_stable_connection_at".into(), field("string", true, Some("rfc3339_utc"), "Timestamp of the last stable peer connection, used as an approximate close time.")),
        ("close_cause".into(), field("string", false, None, "Core Lightning close cause.")),
        ("age_days".into(), warning(field("integer", true, Some("day"), "Approximate lifetime from the short-channel opening block to the last stable connection; null when no closure-time proxy is available."), "Opening age assumes 144 blocks per day; last stable connection is only a proxy for closure time.")),
        ("lease_fee_earnings_msat".into(), source(field("integer", false, Some("msat"), "All lease fees credited to this closed channel account."), "bkpr-listincome lease_fee credit_msat joined by channel_id")),
        ("lease_fee_cost_msat".into(), source(field("integer", false, Some("msat"), "All lease fees debited from this closed channel account."), "bkpr-listincome lease_fee debit_msat joined by channel_id")),
        ("net_revenue_msat".into(), formula(field("integer", false, Some("msat"), "Forwarding and lease revenue after target-attributed rebalance and lease-fee costs."), "forwarding_fees_sat * 1000 + lease_fee_earnings_msat - lease_fee_cost_msat - rebalance_target_cost_msat")),
        ("net_capacity_return_percent".into(), formula(field("number", true, Some("percent"), "Annualized lifetime capacity return after target-attributed rebalance and lease-fee costs."), "net_revenue_msat / capacity_msat * 365 / age_days * 100")),
        ("indirect_capacity_contribution_percent".into(), warning(field("number", true, Some("percent"), "Annualized incoming-side fee attribution relative to full capacity over the closed channel lifetime."), "Do not aggregate as additional node revenue.")),
    ])
}

fn forward_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("in_channel".into(), source(field("string", false, None, "Short channel ID of the incoming side of the forwarding attempt."), "listforwards.in_channel")),
        ("out_channel".into(), source(field("string", true, None, "Short channel ID selected as the outgoing side, when available."), "listforwards.out_channel")),
        ("in_peer_id".into(), source(field("string", true, None, "Public key of the peer connected through the incoming channel, when resolvable."), "listfunds, listclosedchannels, or listchannels joined by in_channel")),
        ("in_peer_alias".into(), source(field("string", true, None, "Gossip alias of the incoming-channel peer, or an abbreviated peer ID when no alias is advertised."), "listnodes joined through in_peer_id")),
        ("out_peer_id".into(), source(field("string", true, None, "Public key of the peer connected through the outgoing channel, when resolvable."), "listfunds, listclosedchannels, or listchannels joined by out_channel")),
        ("out_peer_alias".into(), source(field("string", true, None, "Gossip alias of the outgoing-channel peer, or an abbreviated peer ID when no alias is advertised."), "listnodes joined through out_peer_id")),
        ("status".into(), source(field("string", false, None, "Core Lightning forwarding status, such as settled, failed, local_failed, offered, or pending."), "listforwards.status")),
        ("in_msat".into(), source(field("integer", false, Some("msat"), "Amount received on the incoming channel."), "listforwards.in_msat")),
        ("out_msat".into(), source(field("integer", true, Some("msat"), "Amount offered on the outgoing channel, when assigned."), "listforwards.out_msat")),
        ("fee_msat".into(), source(field("integer", true, Some("msat"), "Forwarding fee earned by the local node for settled attempts."), "listforwards.fee_msat")),
        ("fee_ppm".into(), formula(field("number", true, Some("ppm"), "Effective proportional fee for this attempt."), "fee_msat * 1000000 / out_msat")),
        ("received_at".into(), field("string", true, Some("rfc3339_utc"), "UTC time when the forwarding attempt was received.")),
        ("resolved_at".into(), field("string", true, Some("rfc3339_utc"), "UTC time when the forwarding attempt reached its final status.")),
        ("elapsed_seconds".into(), formula(field("number", true, Some("second"), "Elapsed time between receipt and resolution."), "resolved_at - received_at")),
        ("fail_reason".into(), source(field("string", true, None, "Wire or local failure reason for non-settled attempts."), "listforwards.failreason")),
        ("fail_code".into(), source(field("integer", true, None, "Numeric failure code for non-settled attempts."), "listforwards.failcode")),
    ])
}

fn rebalance_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        (
            "payment_id".into(),
            field(
                "string",
                false,
                None,
                "Payment identifier shared by all parts of a rebalance payment.",
            ),
        ),
        (
            "part_id".into(),
            field("integer", false, None, "Multipart payment part identifier."),
        ),
        (
            "source_account".into(),
            field(
                "string",
                false,
                None,
                "Bookkeeper account debited by the matched rebalance part.",
            ),
        ),
        (
            "target_account".into(),
            field(
                "string",
                false,
                None,
                "Bookkeeper account credited by the matched rebalance part.",
            ),
        ),
        (
            "source_channel_id".into(),
            field(
                "string",
                true,
                None,
                "Resolved short channel ID for the source account.",
            ),
        ),
        (
            "target_channel_id".into(),
            field(
                "string",
                true,
                None,
                "Resolved short channel ID for the target account.",
            ),
        ),
        (
            "source_peer_alias".into(),
            source(
                field(
                    "string",
                    true,
                    None,
                    "Gossip alias of the peer on the source channel, or an abbreviated peer ID when no alias is advertised.",
                ),
                "listfunds, listclosedchannels, or listchannels joined through source_channel_id",
            ),
        ),
        (
            "target_peer_alias".into(),
            source(
                field(
                    "string",
                    true,
                    None,
                    "Gossip alias of the peer on the target channel, or an abbreviated peer ID when no alias is advertised.",
                ),
                "listfunds, listclosedchannels, or listchannels joined through target_channel_id",
            ),
        ),
        (
            "debit_msat".into(),
            field(
                "integer",
                false,
                Some("msat"),
                "Amount debited from the source account.",
            ),
        ),
        (
            "credit_msat".into(),
            field(
                "integer",
                false,
                Some("msat"),
                "Amount credited to the target account.",
            ),
        ),
        (
            "fees_msat".into(),
            field(
                "integer",
                false,
                Some("msat"),
                "Bookkeeper fees attributed to this rebalance part.",
            ),
        ),
        (
            "fee_ppm".into(),
            formula(
                field(
                    "number",
                    true,
                    Some("ppm"),
                    "Effective fee rate paid for this rebalance part.",
                ),
                "fees_msat * 1000000 / credit_msat",
            ),
        ),
        (
            "target_historical_fee_ppm".into(),
            formula(
                field(
                    "number",
                    true,
                    Some("ppm"),
                    "All-time effective outbound forwarding fee rate of the credited channel at snapshot time.",
                ),
                "target_channel.forwarding_fees_sat * 1000000 / target_channel.routed_out_sat",
            ),
        ),
        (
            "timestamp".into(),
            field(
                "integer",
                true,
                Some("unix_second"),
                "Raw bookkeeper event timestamp.",
            ),
        ),
        (
            "resolved_at".into(),
            field(
                "string",
                true,
                Some("rfc3339_utc"),
                "UTC representation of timestamp.",
            ),
        ),
    ])
}

fn rebalance_status_fields() -> BTreeMap<String, FieldMetadata> {
    BTreeMap::from([
        ("short_channel_id".into(), source(field("string", false, None, "Channel managed by the Sling job."), "sling-stats.scid")),
        ("peer_id".into(), source(field("string", false, None, "Public key of the channel peer."), "sling-stats.pubkey")),
        ("peer_alias".into(), source(field("string", false, None, "Alias reported for the channel peer."), "sling-stats.alias")),
        ("last_channel_partner_id".into(), source(field("string", true, None, "Short channel ID of the other channel used by the most recent successful rebalance."), "sling-stats <scid> true successes_in_time_window.last_channel_partner")),
        ("last_channel_partner_alias".into(), source(field("string", true, None, "Gossip alias of the peer on the other channel used by the most recent successful rebalance, or an abbreviated peer ID when no alias is advertised."), "listfunds, listclosedchannels, or listchannels joined through last_channel_partner_id")),
        ("statuses".into(), source(field("array", false, None, "Current Sling job status strings."), "sling-stats.status")),
        ("is_balanced".into(), formula(field("boolean", false, None, "Whether any current status contains `Balanced`."), "any(statuses contains 'Balanced')")),
        ("has_no_cheap_route".into(), formula(field("boolean", false, None, "Whether any current status contains `NoCheapRoute`."), "any(statuses contains 'NoCheapRoute')")),
        ("rebalance_amount_sat".into(), source(field("integer", false, Some("sat"), "Latest rebalance amount reported by Sling, normalized from its formatted string."), "sling-stats.rebamount")),
        ("weighted_fee_ppm".into(), source(field("integer", false, Some("ppm"), "Weighted fee rate reported by Sling."), "sling-stats.w_feeppm")),
        ("last_route_at".into(), source(field("string", true, Some("rfc3339_utc"), "Time of the most recent route attempt, or null when Sling reports `Never`."), "sling-stats.last_route_taken")),
        ("last_success_at".into(), source(field("string", true, Some("rfc3339_utc"), "Time of the most recent successful rebalance, or null when Sling reports `Never`."), "sling-stats.last_success_reb")),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_tooltip_is_derived_from_snapshot_metadata() {
        let tooltip = field_tooltip("channels", "net_capacity_return_percent").unwrap();

        assert!(tooltip.contains("Annualized lifetime capacity return"));
        assert!(tooltip.contains("Formula: net_revenue_msat / capacity_msat"));
        assert!(tooltip.contains("Aggregation: Use capacity-weighting"));
        assert_eq!(field_tooltip("channels", "unknown"), None);
    }
}
