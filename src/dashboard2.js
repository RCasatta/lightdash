(() => {
    "use strict";

    const channelRoot = document.querySelector("[data-channel-root]");
    if (channelRoot) {
        initializeChannelDetail();
        return;
    }

    const root = document.querySelector("[data-table-root]");
    if (!root) return;

    const snapshotTime = Date.parse(document.body.dataset.snapshotTime || "") || Date.now();
    const config = tableConfig(root.dataset.tableKind, snapshotTime);
    if (!config) return;

    const columns = config.columns;
    const presets = config.presets;
    const defaultVisible = columns.filter(item => item.visible).map(item => item.key);
    const state = {
        rows: [],
        currentRows: [],
        visible: loadVisibleColumns(),
        query: "",
        view: "all",
        filters: {},
        sort: config.defaultSort,
        direction: config.defaultDirection,
        page: 1,
        pageSize: config.pageSize
    };

    const table = document.querySelector("#data-table");
    const tableHead = table.querySelector("thead");
    const tableBody = table.querySelector("tbody");
    const searchInput = document.querySelector("#table-search");
    const filterPanel = document.querySelector("#filter-panel");
    const columnPanel = document.querySelector("#column-panel");
    const status = document.querySelector("#table-status");
    const errorBanner = document.querySelector("#table-error");
    const previousPage = document.querySelector("#previous-page");
    const nextPage = document.querySelector("#next-page");
    const pageStatus = document.querySelector("#page-status");
    const pageSizeSelect = document.querySelector("#page-size");
    let searchTimer;

    initialize();

    async function initialize() {
        await loadMetadata();
        initializeFromUrl();
        buildFilterPanel();
        buildColumnPanel();
        bindControls();
        await loadRows();
    }

    async function initializeChannelDetail() {
        const error = document.querySelector("#channel-error");
        const content = document.querySelector("#channel-content");
        const channelKey = new URLSearchParams(location.search).get("channel");
        if (!channelKey) {
            showChannelError("No channel was selected. Open a channel from the channels table.");
            return;
        }

        try {
            const manifest = await fetchJson("data/manifest.json");
            const channels = await fetchJson("data/channels.json");
            const channel = channels.find(row => row.short_channel_id === channelKey || row.channel_id === channelKey);
            if (!channel) throw new Error(`Channel ${channelKey} is not present in this snapshot`);

            const [forwards, rebalances] = await Promise.all([
                fetchJsonLines("data/settled-forwards.jsonl"),
                fetchJsonLines("data/rebalances.jsonl")
            ]);
            const channelFields = manifest.datasets?.channels?.fields || {};
            const channelForwards = forwards
                .filter(row => involvesChannel(row, channel))
                .sort(newestFirst("received_at"));
            const channelRebalances = rebalances
                .filter(row => row.source_channel_id === channel.short_channel_id || row.target_channel_id === channel.short_channel_id)
                .sort(newestFirst("resolved_at"));

            renderChannelHeader(channel);
            renderChannelMetrics(channel);
            renderChannelDetails(channel, channelForwards, channelRebalances, channelFields);
            renderForwardTable("channel-forwards", channelForwards, channel);
            renderRebalanceTable("channel-rebalances", channelRebalances, channel);
            content.hidden = false;
            try {
                await renderChannelHistory(manifest, channel);
            } catch (historyError) {
                document.querySelector("#history-note").textContent = `Historical data could not be loaded: ${historyError.message}`;
                renderEmptyChart("liquidity-chart");
                renderEmptyChart("fee-chart");
                renderEmptyChart("htlc-chart");
            }
        } catch (caught) {
            showChannelError(`${caught.message}. Serve Dashboard2 over HTTP and regenerate it from a current snapshot.`);
        }

        function showChannelError(message) {
            error.textContent = message;
            error.hidden = false;
        }
    }

    function renderChannelHeader(channel) {
        document.querySelector("#channel-title").textContent = channel.peer_alias;
        document.querySelector("#channel-subtitle").textContent = channel.short_channel_id || channel.channel_id;
        document.title = `${channel.peer_alias} · Channel · Lightdash`;
    }

    function renderChannelMetrics(channel) {
        const metrics = [
            ["Local balance", formatNumber(channel.local_balance_percent, 1, "%"), `${formatMsat(channel.local_balance_msat)} locally controlled`],
            ["Capacity", formatMsat(channel.capacity_msat), `${formatMsat(channel.capacity_msat - channel.local_balance_msat)} remote balance`],
            ["Forwards", formatNumber(channel.settled_forward_count, 0), `${formatSat(channel.forwarding_fees_sat)} earned`],
            ["Net capacity return", formatNumber(channel.net_capacity_return_percent, 2, "%"), `${formatMsat(channel.net_routing_revenue_msat)} net routing revenue`]
        ];
        const fragment = document.createDocumentFragment();
        metrics.forEach(([label, value, note]) => {
            const card = document.createElement("article");
            card.className = "metric-card";
            card.append(textElement("p", label, "metric-label"), textElement("p", value, "metric-value"), textElement("p", note, "metric-note"));
            fragment.appendChild(card);
        });
        document.querySelector("#channel-metrics").replaceChildren(fragment);
    }

    function renderChannelDetails(channel, forwards, rebalances, fields) {
        const identity = document.querySelector("#channel-identity");
        const peerLink = document.createElement("a");
        peerLink.href = `https://mempool.space/lightning/node/${encodeURIComponent(channel.peer_id)}`;
        peerLink.target = "_blank";
        peerLink.rel = "noreferrer";
        peerLink.textContent = abbreviateValue(channel.peer_id);
        peerLink.title = channel.peer_id;
        peerLink.className = "monospace";
        appendDetail(identity, "Channel ID", channel.channel_id, fields.channel_id, true);
        appendDetail(identity, "Short channel ID", channel.short_channel_id, fields.short_channel_id, true);
        appendDetail(identity, "Peer ID", peerLink, fields.peer_id);
        appendDetail(identity, "State", channel.state, fields.state);
        appendDetail(identity, "Connected", channel.connected ? "Yes" : "No", fields.connected);
        appendDetail(identity, "Uptime", channel.uptime_ratio == null ? null : formatNumber(channel.uptime_ratio * 100, 2, "%"), fields.uptime_ratio);
        appendDetail(identity, "Age", channel.age_days == null ? null : `${formatNumber(channel.age_days, 0)} days`, fields.age_days);
        appendDetail(identity, "Funding outpoint", `${channel.funding_txid}:${channel.funding_output}`, fields.funding_txid, true);

        const policy = document.querySelector("#channel-policy");
        appendDetail(policy, "Fee rate", formatNumber(channel.outbound_fee_ppm, 0, " ppm"), fields.outbound_fee_ppm);
        appendDetail(policy, "Inbound fee rate", formatNumber(channel.inbound_fee_ppm, 0, " ppm"), fields.inbound_fee_ppm);
        appendDetail(policy, "Base fee", formatMsat(channel.outbound_base_fee_msat), fields.outbound_base_fee_msat);
        appendDetail(policy, "Minimum HTLC", formatMsat(channel.outbound_htlc_min_msat), fields.outbound_htlc_min_msat);
        appendDetail(policy, "Maximum HTLC", formatMsat(channel.outbound_htlc_max_msat), fields.outbound_htlc_max_msat);
        appendDetail(policy, "CLTV delta", channel.outbound_delay_blocks == null ? null : `${channel.outbound_delay_blocks} blocks`, fields.outbound_delay_blocks);
        appendDetail(policy, "Last fee adjustment", channel.last_fee_adjustment_at, fields.last_fee_adjustment_at);
        appendDetail(policy, "Historical fee rate", formatNumber(channel.historical_effective_fee_ppm, 0, " ppm"), fields.historical_effective_fee_ppm);
        appendDetail(policy, "Time-decayed fee rate", formatNumber(channel.time_decayed_variable_fee_ppm, 0, " ppm"), fields.time_decayed_variable_fee_ppm);

        const inbound = forwards.filter(row => row.in_channel === channel.short_channel_id);
        const outbound = forwards.filter(row => row.out_channel === channel.short_channel_id);
        const targetRebalances = rebalances.filter(row => row.target_channel_id === channel.short_channel_id);
        const activity = document.querySelector("#channel-activity");
        appendDetail(activity, "Last inbound forward", inbound[0]?.received_at, fields.settled_forward_count);
        appendDetail(activity, "Last outbound forward", outbound[0]?.received_at, fields.settled_forward_count);
        appendDetail(activity, "Routed outbound", formatSat(channel.routed_out_sat), fields.routed_out_sat);
        appendDetail(activity, "Forwarding fees", formatSat(channel.forwarding_fees_sat), fields.forwarding_fees_sat);
        appendDetail(activity, "Indirect fees", formatSat(channel.indirect_fees_sat), fields.indirect_fees_sat);
        appendDetail(activity, "Rebalance cost", formatMsat(channel.rebalance_target_cost_msat), fields.rebalance_target_cost_msat);
        appendDetail(activity, "Rebalance fee rate", formatNumber(channel.rebalance_effective_fee_ppm, 0, " ppm"), fields.rebalance_effective_fee_ppm);
        appendDetail(activity, "Rebalance parts", formatNumber(targetRebalances.length, 0), null);
        appendDetail(activity, "Rebalance payments", formatNumber(new Set(targetRebalances.map(row => row.payment_id)).size, 0), null);
        appendDetail(activity, "First rebalance", targetRebalances.at(-1)?.resolved_at, null);
        appendDetail(activity, "Last rebalance", targetRebalances[0]?.resolved_at, null);
        appendDetail(activity, "Net routing revenue", formatMsat(channel.net_routing_revenue_msat), fields.net_routing_revenue_msat);
    }

    async function renderChannelHistory(manifest, channel) {
        const policyMetadata = manifest.datasets?.channel_policy_history;
        const liquidityMetadata = manifest.datasets?.channel_liquidity_history;
        const note = document.querySelector("#history-note");
        if (!policyMetadata || !liquidityMetadata) {
            note.textContent = "Historical archives were not included in this snapshot.";
            renderEmptyChart("liquidity-chart");
            renderEmptyChart("fee-chart");
            renderEmptyChart("htlc-chart");
            return;
        }

        const [policies, liquidity] = await Promise.all([
            fetchJsonLines(`data/${policyMetadata.path}`, policyMetadata.format === "gzip-jsonl"),
            fetchJsonLines(`data/${liquidityMetadata.path}`, liquidityMetadata.format === "gzip-jsonl")
        ]);
        const policyRows = policies.filter(row => row.short_channel_id === channel.short_channel_id);
        const liquidityRows = liquidity.filter(row => row.channel_id === channel.channel_id);
        lineChart("liquidity-chart", [{ label: "Local", color: "#f6a723", rows: liquidityRows, value: row => row.local_balance_percent }], "%");
        lineChart("fee-chart", [
            { label: "Local", color: "#f6a723", rows: policyRows.filter(row => row.direction === "local"), value: row => row.fee_ppm },
            { label: "Remote", color: "#64b5f6", rows: policyRows.filter(row => row.direction === "remote"), value: row => row.fee_ppm }
        ], " ppm");
        lineChart("htlc-chart", [{ label: "Local", color: "#50d890", rows: policyRows.filter(row => row.direction === "local"), value: row => row.htlc_max_msat / 1000 }], " sats");
        note.textContent = `Change-point history: ${formatNumber(liquidityRows.length, 0)} liquidity observations and ${formatNumber(policyRows.length, 0)} policy observations.`;
    }

    function lineChart(id, series, suffix) {
        const host = document.querySelector(`#${id}`);
        const points = series.flatMap(item => item.rows.map(row => ({ x: Date.parse(row.observed_at), y: item.value(row) }))).filter(point => Number.isFinite(point.x) && Number.isFinite(point.y));
        if (points.length === 0) return renderEmptyChart(id);
        const width = 760;
        const height = 260;
        const pad = { left: 58, right: 18, top: 24, bottom: 34 };
        const minX = Math.min(...points.map(point => point.x));
        const maxX = Math.max(...points.map(point => point.x));
        const minY = Math.min(0, ...points.map(point => point.y));
        const maxY = Math.max(...points.map(point => point.y));
        const x = value => pad.left + (value - minX) / Math.max(1, maxX - minX) * (width - pad.left - pad.right);
        const y = value => height - pad.bottom - (value - minY) / Math.max(1, maxY - minY) * (height - pad.top - pad.bottom);
        const svg = svgElement("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Historical line chart" });
        svg.appendChild(svgElement("line", { x1: pad.left, y1: y(minY), x2: width - pad.right, y2: y(minY), class: "chart-axis" }));
        svg.appendChild(svgText(pad.left - 8, y(maxY) + 4, formatCompact(maxY, suffix), "end"));
        svg.appendChild(svgText(pad.left - 8, y(minY) + 4, formatCompact(minY, suffix), "end"));
        svg.appendChild(svgText(pad.left, height - 10, new Date(minX).toISOString().slice(0, 10), "start"));
        svg.appendChild(svgText(width - pad.right, height - 10, new Date(maxX).toISOString().slice(0, 10), "end"));
        series.forEach(item => {
            const rows = item.rows.map(row => ({ x: Date.parse(row.observed_at), y: item.value(row) })).filter(point => Number.isFinite(point.x) && Number.isFinite(point.y)).sort((a, b) => a.x - b.x);
            if (!rows.length) return;
            const path = rows.map((point, index) => `${index ? "L" : "M"}${x(point.x).toFixed(1)},${y(point.y).toFixed(1)}`).join(" ");
            svg.appendChild(svgElement("path", { d: path, fill: "none", stroke: item.color, "stroke-width": 2.5, "stroke-linejoin": "round" }));
        });
        const legend = document.createElement("div");
        legend.className = "chart-legend";
        series.filter(item => item.rows.length).forEach(item => {
            const entry = document.createElement("span");
            const swatch = document.createElement("i");
            swatch.style.background = item.color;
            entry.append(swatch, document.createTextNode(item.label));
            legend.appendChild(entry);
        });
        host.replaceChildren(svg, legend);
    }

    function renderForwardTable(id, rows, channel) {
        renderSimpleTable(id, ["Direction", "Other channel", "Amount", "Fee", "Fee PPM", "Received", "Elapsed"], rows.slice(0, 100).map(row => [
            row.out_channel === channel.short_channel_id ? "Outbound" : "Inbound",
            row.out_channel === channel.short_channel_id ? row.in_channel : row.out_channel,
            formatMsat(row.out_msat),
            formatMsat(row.fee_msat),
            formatNumber(row.fee_ppm, 1, " ppm"),
            row.received_at,
            formatNumber(row.elapsed_seconds, 1, " s")
        ]), rows.length);
    }

    function renderRebalanceTable(id, rows, channel) {
        renderSimpleTable(id, ["Direction", "Payment", "Debit", "Credit", "Fees", "Resolved"], rows.slice(0, 100).map(row => [
            row.target_channel_id === channel.short_channel_id ? "Inbound" : "Outbound",
            row.payment_id,
            formatMsat(row.debit_msat),
            formatMsat(row.credit_msat),
            formatMsat(row.fees_msat),
            row.resolved_at
        ]), rows.length);
    }

    function renderSimpleTable(id, headings, rows, total) {
        const table = document.querySelector(`#${id}`);
        const header = document.createElement("tr");
        headings.forEach(label => header.appendChild(textElement("th", label)));
        const body = document.createDocumentFragment();
        rows.forEach(values => {
            const row = document.createElement("tr");
            values.forEach(value => row.appendChild(textElement("td", value ?? "—")));
            body.appendChild(row);
        });
        table.querySelector("thead").replaceChildren(header);
        table.querySelector("tbody").replaceChildren(body);
        table.hidden = total === 0;
        document.querySelector(`#${id}-empty`).hidden = total !== 0;
        document.querySelector(`#${id}-status`).textContent = total > 100 ? `Showing the latest 100 of ${formatNumber(total, 0)} records.` : `${formatNumber(total, 0)} records.`;
    }

    function appendDetail(list, label, value, metadata, monospace = false) {
        const wrapper = document.createElement("div");
        const term = textElement("dt", label);
        if (metadata) term.title = metadataTitle(metadata);
        const detail = document.createElement("dd");
        if (monospace) detail.classList.add("monospace");
        detail.append(value instanceof Node ? value : document.createTextNode(value ?? "—"));
        wrapper.append(term, detail);
        list.appendChild(wrapper);
    }

    async function fetchJson(path) {
        const response = await fetch(path, { cache: "no-store" });
        if (!response.ok) throw new Error(`Loading ${path} returned HTTP ${response.status}`);
        return response.json();
    }

    async function fetchJsonLines(path, gzip = false) {
        const response = await fetch(path, { cache: "no-store" });
        if (!response.ok) throw new Error(`Loading ${path} returned HTTP ${response.status}`);
        let text;
        if (gzip) {
            if (!("DecompressionStream" in window)) throw new Error("This browser cannot decompress snapshot history");
            const bytes = new Uint8Array(await response.arrayBuffer());
            const isGzip = bytes[0] === 0x1f && bytes[1] === 0x8b;
            text = isGzip
                ? await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"))).text()
                : new TextDecoder().decode(bytes);
        } else {
            text = await response.text();
        }
        return text.split("\n").filter(Boolean).map(line => JSON.parse(line));
    }

    function involvesChannel(row, channel) {
        return row.in_channel === channel.short_channel_id || row.out_channel === channel.short_channel_id;
    }

    function newestFirst(key) {
        return (left, right) => Date.parse(right[key] || 0) - Date.parse(left[key] || 0);
    }

    function textElement(tag, text, className = "") {
        const element = document.createElement(tag);
        element.textContent = text ?? "—";
        if (className) element.className = className;
        return element;
    }

    function svgElement(tag, attributes) {
        const element = document.createElementNS("http://www.w3.org/2000/svg", tag);
        Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, value));
        return element;
    }

    function svgText(x, y, value, anchor) {
        const element = svgElement("text", { x, y, "text-anchor": anchor, class: "chart-label" });
        element.textContent = value;
        return element;
    }

    function renderEmptyChart(id) {
        document.querySelector(`#${id}`).replaceChildren(textElement("p", "No history available", "chart-empty"));
    }

    function formatMsat(value) {
        return value == null ? "—" : `${formatNumber(Number(value) / 1000, Number(value) % 1000 ? 3 : 0)} sats`;
    }

    function formatSat(value) {
        return value == null ? "—" : `${formatNumber(value, 0)} sats`;
    }

    function formatNumber(value, decimals = 0, suffix = "") {
        if (value == null || !Number.isFinite(Number(value))) return "—";
        return `${new Intl.NumberFormat(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals }).format(Number(value))}${suffix}`;
    }

    function formatCompact(value, suffix) {
        return `${new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value)}${suffix}`;
    }

    function abbreviateValue(value) {
        if (!value || value.length <= 28) return value;
        return `${value.slice(0, 14)}…${value.slice(-10)}`;
    }

    function tableConfig(kind, referenceTime) {
        const configs = {
            channels: {
                datasetKey: "channels",
                source: "data/channels.json",
                format: "json",
                itemLabel: "channels",
                fileBase: "lightdash-channels",
                storageKey: "lightdash.dashboard2.channelColumns",
                defaultSort: "local_balance_percent",
                defaultDirection: "asc",
                pageSize: 0,
                emptyMessage: "No channels match the current filters.",
                prepare: row => row,
                presets: {
                    all: {},
                    mature: { age_days: { min: 365 } },
                    "low-balance": { local_balance_percent: { max: 20 } },
                    "negative-capacity-return": { net_capacity_return_percent: { lt: 0 } },
                    disconnected: { connected: { eq: "false" } },
                    "no-forwards": { settled_forward_count: { max: 0 } }
                },
                columns: channelColumns()
            },
            forwards: {
                datasetKey: "settled_forwards",
                source: "data/settled-forwards.jsonl",
                format: "jsonl",
                itemLabel: "settled forwards",
                fileBase: "lightdash-forwards",
                storageKey: "lightdash.dashboard2.forwardColumns",
                defaultSort: "received_at",
                defaultDirection: "desc",
                pageSize: 100,
                emptyMessage: "No forward attempts match the current filters.",
                prepare: prepareForward,
                presets: {
                    all: {},
                    "last-day": { received_at: { min: dateInputValue(referenceTime - 24 * 60 * 60 * 1000) } },
                    "last-week": { received_at: { min: dateInputValue(referenceTime - 7 * 24 * 60 * 60 * 1000) } },
                    "last-month": { received_at: { min: dateInputValue(referenceTime - 30 * 24 * 60 * 60 * 1000) } },
                    "last-year": { received_at: { min: dateInputValue(referenceTime - 365 * 24 * 60 * 60 * 1000) } }
                },
                columns: forwardColumns()
            }
        };
        return configs[kind];
    }

    function channelColumns() {
        return [
            column("short_channel_id", "Channel", "text", { visible: true, monospace: true, value: row => row.short_channel_id || row.channel_id.slice(0, 16) }),
            column("peer_alias", "Peer", "text", { visible: true }),
            column("connected", "Connected", "boolean", { visible: true }),
            column("age_days", "Age", "number", { visible: true, suffix: " d", decimals: 0 }),
            column("local_balance_percent", "Local balance", "number", { visible: true, suffix: "%", decimals: 1 }),
            column("capacity_msat", "Capacity", "number", { visible: true, transform: value => value / 1000, suffix: " sats", decimals: 0 }),
            column("uptime_ratio", "Uptime", "number", { visible: true, transform: value => value * 100, suffix: "%", decimals: 1 }),
            column("outbound_fee_ppm", "My PPM", "number", { visible: true, suffix: " ppm", decimals: 0 }),
            column("historical_effective_fee_ppm", "Historical PPM", "number", { visible: true, suffix: " ppm", decimals: 0 }),
            column("time_decayed_variable_fee_ppm", "TPPM", "number", { visible: true, suffix: " ppm", decimals: 0 }),
            column("rebalance_effective_fee_ppm", "Rebalance PPM", "number", { suffix: " ppm", decimals: 0 }),
            column("settled_forward_count", "Forwards", "number", { visible: true, decimals: 0 }),
            column("routed_out_sat", "Routed out", "number", { suffix: " sats", decimals: 0 }),
            column("forwarding_fees_sat", "Fees", "number", { suffix: " sats", decimals: 0 }),
            column("indirect_fees_sat", "Indirect fees", "number", { suffix: " sats", decimals: 0 }),
            column("gross_capacity_return_percent", "Gross capacity return", "number", { suffix: "%", decimals: 2, signedClass: true }),
            column("net_capacity_return_percent", "Net capacity return", "number", { visible: true, suffix: "%", decimals: 2, signedClass: true }),
            column("indirect_capacity_contribution_percent", "Indirect capacity contribution", "number", { visible: true, suffix: "%", decimals: 2, signedClass: true }),
            column("rebalance_target_cost_msat", "Rebalance cost", "number", { transform: value => value / 1000, suffix: " sats", decimals: 0 }),
            column("net_routing_revenue_msat", "Net revenue", "number", { transform: value => value / 1000, suffix: " sats", decimals: 0, signedClass: true }),
            column("inbound_fee_ppm", "Inbound PPM", "number", { suffix: " ppm", decimals: 0 }),
            column("state", "State", "text")
        ];
    }

    function forwardColumns() {
        return [
            column("received_at", "Received", "date", { visible: true, value: row => row._receivedAt }),
            column("in_channel", "In channel", "text", { visible: true, monospace: true }),
            column("out_channel", "Out channel", "text", { visible: true, monospace: true }),
            column("out_msat", "Out amount", "number", { visible: true, transform: value => value / 1000, suffix: " sats", decimals: 0 }),
            column("fee_msat", "Fee", "number", { visible: true, transform: value => value / 1000, suffix: " sats", decimals: 3 }),
            column("fee_ppm", "Fee PPM", "number", { visible: true, suffix: " ppm", decimals: 1 }),
            column("elapsed_seconds", "Elapsed", "number", { visible: true, suffix: " s", decimals: 1 }),
            column("resolved_at", "Resolved", "date", { value: row => row._resolvedAt }),
            column("in_msat", "In amount", "number", { transform: value => value / 1000, suffix: " sats", decimals: 0 })
        ];
    }

    function column(key, label, type, options = {}) {
        return {
            key,
            label,
            type,
            visible: options.visible ?? false,
            decimals: options.decimals ?? 0,
            suffix: options.suffix ?? "",
            transform: options.transform ?? (value => value),
            value: options.value ?? (row => row[key]),
            monospace: options.monospace ?? false,
            signedClass: options.signedClass ?? false,
            options: options.options ?? [],
            badge: options.badge ?? false,
            metadata: null
        };
    }

    function prepareForward(row) {
        row._receivedAt = parseDate(row.received_at);
        row._resolvedAt = parseDate(row.resolved_at);
        return row;
    }

    async function loadMetadata() {
        try {
            const response = await fetch("data/manifest.json", { cache: "no-store" });
            if (!response.ok) return;
            const manifest = await response.json();
            const fields = manifest.datasets?.[config.datasetKey]?.fields || {};
            columns.forEach(item => {
                item.metadata = fields[item.key] || null;
            });
        } catch {
            // Metadata enriches the UI but is not required to render the table.
        }
    }

    async function loadRows() {
        try {
            const response = await fetch(config.source, { cache: "no-store" });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const rows = config.format === "jsonl"
                ? await readJsonLines(response)
                : await response.json();
            if (!Array.isArray(rows)) throw new Error(`${config.source} is not an array`);
            state.rows = rows.map(config.prepare);
            render();
        } catch (error) {
            errorBanner.hidden = false;
            errorBanner.textContent = `Unable to load table data: ${error.message}. Serve dashboard2 over HTTP rather than opening it through file://.`;
            status.textContent = "Table data unavailable";
        }
    }

    async function readJsonLines(response) {
        if (!response.body) {
            return (await response.text())
                .split("\n")
                .filter(Boolean)
                .map(line => JSON.parse(line));
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        const rows = [];
        let buffer = "";
        while (true) {
            const { value, done } = await reader.read();
            buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
            const lines = buffer.split("\n");
            buffer = lines.pop() || "";
            for (const line of lines) {
                if (line) rows.push(JSON.parse(line));
            }
            if (rows.length && rows.length % 5000 < lines.length) {
                status.textContent = `Loaded ${formatInteger(rows.length)} ${config.itemLabel}…`;
            }
            if (done) break;
        }
        if (buffer.trim()) rows.push(JSON.parse(buffer));
        return rows;
    }

    function bindControls() {
        searchInput.value = state.query;
        searchInput.addEventListener("input", () => {
            clearTimeout(searchTimer);
            searchTimer = setTimeout(() => {
                state.query = searchInput.value.trim();
                state.view = "custom";
                state.page = 1;
                render();
            }, 160);
        });

        document.querySelectorAll("[data-view]").forEach(button => {
            button.addEventListener("click", () => {
                state.view = button.dataset.view;
                state.filters = cloneFilters(presets[state.view] || {});
                state.page = 1;
                syncFilterControls();
                render();
            });
        });

        document.querySelector("#reset-table").addEventListener("click", () => {
            state.query = "";
            state.view = "all";
            state.filters = {};
            state.sort = config.defaultSort;
            state.direction = config.defaultDirection;
            state.visible = [...defaultVisible];
            state.page = 1;
            state.pageSize = config.pageSize;
            searchInput.value = "";
            if (pageSizeSelect) pageSizeSelect.value = String(config.pageSize);
            removeStoredColumns();
            syncFilterControls();
            syncColumnControls();
            render();
        });

        document.querySelector("#export-csv").addEventListener("click", exportCsv);
        document.querySelector("#export-json").addEventListener("click", exportJson);

        previousPage?.addEventListener("click", () => {
            state.page = Math.max(1, state.page - 1);
            render();
        });
        nextPage?.addEventListener("click", () => {
            state.page += 1;
            render();
        });
        pageSizeSelect?.addEventListener("change", () => {
            state.pageSize = Number(pageSizeSelect.value);
            state.page = 1;
            render();
        });
        if (pageSizeSelect) pageSizeSelect.value = String(state.pageSize);
    }

    function buildFilterPanel() {
        const fragment = document.createDocumentFragment();
        columns.forEach(item => {
            const wrapper = document.createElement("label");
            wrapper.className = "filter-control";
            const title = document.createElement("span");
            title.textContent = item.label;
            title.title = metadataTitle(item.metadata);
            wrapper.appendChild(title);

            if (item.type === "number" || item.type === "date") {
                const range = document.createElement("span");
                range.className = "range-fields";
                range.appendChild(filterInput(item, "min", item.type === "date" ? "After" : "Min"));
                range.appendChild(filterInput(item, "max", item.type === "date" ? "Before" : "Max"));
                wrapper.appendChild(range);
            } else if (item.type === "boolean" || item.type === "enum") {
                const select = document.createElement("select");
                select.dataset.filterKey = item.key;
                select.dataset.filterPart = "eq";
                const options = item.type === "boolean"
                    ? [["", "Any"], ["true", "Yes"], ["false", "No"]]
                    : [["", "Any"], ...item.options.map(value => [value, humanize(value)])];
                options.forEach(([value, label]) => {
                    const option = document.createElement("option");
                    option.value = value;
                    option.textContent = label;
                    select.appendChild(option);
                });
                select.addEventListener("change", updateFilter);
                wrapper.appendChild(select);
            } else {
                wrapper.appendChild(filterInput(item, "value", "Contains"));
            }
            fragment.appendChild(wrapper);
        });
        filterPanel.appendChild(fragment);
        syncFilterControls();
    }

    function filterInput(item, part, placeholder) {
        const input = document.createElement("input");
        input.type = item.type === "number" ? "number" : item.type === "date" ? "datetime-local" : "text";
        if (item.type === "number") input.step = "any";
        input.placeholder = placeholder;
        input.dataset.filterKey = item.key;
        input.dataset.filterPart = part;
        input.addEventListener("input", updateFilter);
        return input;
    }

    function updateFilter(event) {
        const { filterKey: key, filterPart: part } = event.target.dataset;
        const value = event.target.value.trim();
        state.filters[key] ||= {};
        if (value === "") {
            delete state.filters[key][part];
            if (Object.keys(state.filters[key]).length === 0) delete state.filters[key];
        } else {
            state.filters[key][part] = value;
        }
        state.view = "custom";
        state.page = 1;
        render();
    }

    function buildColumnPanel() {
        const fragment = document.createDocumentFragment();
        columns.forEach(item => {
            const label = document.createElement("label");
            label.className = "column-option";
            const checkbox = document.createElement("input");
            checkbox.type = "checkbox";
            checkbox.value = item.key;
            checkbox.addEventListener("change", () => {
                if (checkbox.checked) {
                    if (!state.visible.includes(item.key)) state.visible.push(item.key);
                } else if (state.visible.length > 1) {
                    state.visible = state.visible.filter(key => key !== item.key);
                } else {
                    checkbox.checked = true;
                    return;
                }
                storeColumns();
                render();
            });
            const text = document.createElement("span");
            text.textContent = item.label;
            text.title = metadataTitle(item.metadata);
            label.append(checkbox, text);
            fragment.appendChild(label);
        });
        columnPanel.appendChild(fragment);
        syncColumnControls();
    }

    function render() {
        const visibleColumns = columns.filter(item => state.visible.includes(item.key));
        const rows = sortedRows(filteredRows());
        state.currentRows = rows;
        const pageCount = state.pageSize ? Math.max(1, Math.ceil(rows.length / state.pageSize)) : 1;
        state.page = Math.min(Math.max(1, state.page), pageCount);
        const pageRows = state.pageSize
            ? rows.slice((state.page - 1) * state.pageSize, state.page * state.pageSize)
            : rows;

        renderHeader(visibleColumns);
        renderBody(pageRows, visibleColumns);
        renderStatus(rows.length, pageRows.length);
        renderPagination(pageCount);
        document.querySelectorAll("[data-view]").forEach(button => {
            button.classList.toggle("is-active", button.dataset.view === state.view);
        });
        updateUrl();
    }

    function renderStatus(matchCount, pageCount) {
        if (!state.pageSize) {
            status.textContent = `Showing ${formatInteger(matchCount)} of ${formatInteger(state.rows.length)} ${config.itemLabel}`;
            return;
        }
        const start = matchCount === 0 ? 0 : (state.page - 1) * state.pageSize + 1;
        const end = start + pageCount - 1;
        status.textContent = `Showing ${formatInteger(start)}–${formatInteger(end)} of ${formatInteger(matchCount)} matching ${config.itemLabel} (${formatInteger(state.rows.length)} total)`;
    }

    function renderPagination(pageCount) {
        if (!pageStatus) return;
        pageStatus.textContent = `Page ${formatInteger(state.page)} of ${formatInteger(pageCount)}`;
        previousPage.disabled = state.page <= 1;
        nextPage.disabled = state.page >= pageCount;
    }

    function renderHeader(visibleColumns) {
        const row = document.createElement("tr");
        visibleColumns.forEach(item => {
            const header = document.createElement("th");
            header.scope = "col";
            header.setAttribute("aria-sort", state.sort === item.key ? (state.direction === "asc" ? "ascending" : "descending") : "none");
            const button = document.createElement("button");
            button.type = "button";
            button.textContent = `${item.label}${state.sort === item.key ? (state.direction === "asc" ? " ↑" : " ↓") : ""}`;
            button.title = metadataTitle(item.metadata);
            button.addEventListener("click", () => {
                if (state.sort === item.key) {
                    state.direction = state.direction === "asc" ? "desc" : "asc";
                } else {
                    state.sort = item.key;
                    state.direction = "asc";
                }
                state.page = 1;
                render();
            });
            header.appendChild(button);
            row.appendChild(header);
        });
        tableHead.replaceChildren(row);
    }

    function renderBody(rows, visibleColumns) {
        const fragment = document.createDocumentFragment();
        if (rows.length === 0) {
            const row = document.createElement("tr");
            row.className = "empty-row";
            const cell = document.createElement("td");
            cell.colSpan = visibleColumns.length;
            cell.textContent = config.emptyMessage;
            row.appendChild(cell);
            fragment.appendChild(row);
        } else {
            rows.forEach(data => {
                const row = document.createElement("tr");
                visibleColumns.forEach(item => row.appendChild(renderCell(item, data)));
                fragment.appendChild(row);
            });
        }
        tableBody.replaceChildren(fragment);
    }

    function renderCell(item, row) {
        const cell = document.createElement("td");
        const rawValue = item.value(row);
        if (item.type === "number") cell.classList.add("number");
        if (item.monospace) cell.classList.add("mono");
        if (item.signedClass && typeof rawValue === "number") {
            cell.classList.add(rawValue < 0 ? "negative" : "positive");
        }
        if (item.type === "boolean") {
            appendBadge(cell, rawValue ? "Yes" : "No", rawValue ? "connected" : "disconnected");
        } else if (item.badge) {
            appendBadge(cell, humanize(rawValue), `status-${rawValue || "unknown"}`);
        } else if (config.datasetKey === "channels" && item.key === "short_channel_id") {
            const link = document.createElement("a");
            link.href = `channel.html?channel=${encodeURIComponent(row.short_channel_id || row.channel_id)}`;
            link.textContent = formatValue(item, rawValue);
            cell.appendChild(link);
        } else if (config.datasetKey === "settled_forwards" && ["in_channel", "out_channel"].includes(item.key) && rawValue) {
            const link = document.createElement("a");
            link.href = `channel.html?channel=${encodeURIComponent(rawValue)}`;
            link.textContent = String(rawValue);
            cell.appendChild(link);
        } else {
            cell.textContent = formatValue(item, rawValue);
        }
        return cell;
    }

    function appendBadge(cell, label, className) {
        const badge = document.createElement("span");
        badge.className = `status-badge ${className}`;
        badge.textContent = label;
        cell.appendChild(badge);
    }

    function formatValue(item, value) {
        if (value === null || value === undefined || value === "") return "—";
        if (item.type === "date") return new Date(value).toISOString().replace("T", " ").replace(".000Z", "Z");
        if (item.type !== "number") return String(value);
        const transformed = item.transform(Number(value));
        return `${new Intl.NumberFormat(undefined, {
            minimumFractionDigits: item.decimals,
            maximumFractionDigits: item.decimals
        }).format(transformed)}${item.suffix}`;
    }

    function filteredRows() {
        const query = state.query.toLocaleLowerCase();
        return state.rows.filter(row => {
            if (query && !columns.some(item => searchValue(item, row).includes(query))) return false;
            return Object.entries(state.filters).every(([key, filter]) => {
                const item = columns.find(candidate => candidate.key === key);
                if (!item) return true;
                const rawValue = item.value(row);
                if (filter.in !== undefined && !filter.in.includes(rawValue)) return false;
                if (filter.eq !== undefined) {
                    if (item.type === "boolean") {
                        if (String(Boolean(rawValue)) !== filter.eq) return false;
                    } else if (String(rawValue ?? "") !== filter.eq) {
                        return false;
                    }
                }
                if (item.type === "number" || item.type === "date") {
                    if (rawValue === null || rawValue === undefined) return false;
                    const value = item.type === "number" ? item.transform(Number(rawValue)) : Number(rawValue);
                    const min = filter.min === undefined ? null : filterBoundary(item, filter.min);
                    const max = filter.max === undefined ? null : filterBoundary(item, filter.max);
                    if (min !== null && value < min) return false;
                    if (max !== null && value > max) return false;
                    if (filter.gt !== undefined && value <= Number(filter.gt)) return false;
                    if (filter.lt !== undefined && value >= Number(filter.lt)) return false;
                } else if (filter.value !== undefined
                    && !String(rawValue ?? "").toLocaleLowerCase().includes(String(filter.value).toLocaleLowerCase())) {
                    return false;
                }
                return true;
            });
        });
    }

    function filterBoundary(item, value) {
        const boundary = item.type === "date" ? Date.parse(value) : Number(value);
        return Number.isFinite(boundary) ? boundary : null;
    }

    function searchValue(item, row) {
        const value = item.value(row);
        if (item.type === "date" && value !== null) return formatValue(item, value).toLocaleLowerCase();
        return String(value ?? "").toLocaleLowerCase();
    }

    function sortedRows(rows) {
        const item = columns.find(candidate => candidate.key === state.sort) || columns[0];
        const multiplier = state.direction === "asc" ? 1 : -1;
        return [...rows].sort((left, right) => {
            const a = item.value(left);
            const b = item.value(right);
            if (a === null || a === undefined) return b === null || b === undefined ? 0 : 1;
            if (b === null || b === undefined) return -1;
            if (item.type === "number") return (item.transform(Number(a)) - item.transform(Number(b))) * multiplier;
            if (item.type === "date") return (Number(a) - Number(b)) * multiplier;
            if (item.type === "boolean") return (Number(a) - Number(b)) * multiplier;
            return String(a).localeCompare(String(b)) * multiplier;
        });
    }

    function syncFilterControls() {
        filterPanel.querySelectorAll("[data-filter-key]").forEach(control => {
            const { filterKey: key, filterPart: part } = control.dataset;
            control.value = state.filters[key]?.[part] ?? "";
        });
    }

    function syncColumnControls() {
        columnPanel.querySelectorAll("input[type=checkbox]").forEach(checkbox => {
            checkbox.checked = state.visible.includes(checkbox.value);
        });
    }

    function initializeFromUrl() {
        const params = new URLSearchParams(location.search);
        const view = params.get("view");
        if (view && presets[view]) {
            state.view = view;
            state.filters = cloneFilters(presets[view]);
        }
        state.query = params.get("q") || "";
        const sort = params.get("sort");
        if (sort && columns.some(item => item.key === sort)) state.sort = sort;
        if (params.get("dir") === "asc" || params.get("dir") === "desc") state.direction = params.get("dir");
        const visible = params.get("columns")?.split(",").filter(key => columns.some(item => item.key === key));
        if (visible?.length) state.visible = visible;
        const page = Number(params.get("page"));
        if (page > 0) state.page = page;
        const pageSize = Number(params.get("page_size"));
        if (config.pageSize && [50, 100, 250, 500].includes(pageSize)) state.pageSize = pageSize;

        columns.forEach(item => {
            ["min", "max", "value", "eq", "in"].forEach(part => {
                const value = params.get(`f_${item.key}_${part}`);
                if (value !== null) {
                    state.filters[item.key] ||= {};
                    state.filters[item.key][part] = part === "in" ? value.split(",") : value;
                    state.view = "custom";
                }
            });
        });
    }

    function updateUrl() {
        const params = new URLSearchParams();
        if (state.view !== "all" && state.view !== "custom") params.set("view", state.view);
        if (state.query) params.set("q", state.query);
        if (state.sort !== config.defaultSort) params.set("sort", state.sort);
        if (state.direction !== config.defaultDirection) params.set("dir", state.direction);
        if (state.visible.join(",") !== defaultVisible.join(",")) params.set("columns", state.visible.join(","));
        if (state.page > 1) params.set("page", state.page);
        if (config.pageSize && state.pageSize !== config.pageSize) params.set("page_size", state.pageSize);
        if (state.view === "custom") {
            Object.entries(state.filters).forEach(([key, filter]) => {
                Object.entries(filter).forEach(([part, value]) => {
                    params.set(`f_${key}_${part}`, Array.isArray(value) ? value.join(",") : value);
                });
            });
        }
        const query = params.toString();
        history.replaceState(null, "", `${location.pathname}${query ? `?${query}` : ""}`);
    }

    function loadVisibleColumns() {
        try {
            const stored = localStorage.getItem(config.storageKey);
            const visible = stored?.split(",").filter(key => columns.some(item => item.key === key));
            return visible?.length ? visible : [...defaultVisible];
        } catch {
            return [...defaultVisible];
        }
    }

    function storeColumns() {
        try {
            localStorage.setItem(config.storageKey, state.visible.join(","));
        } catch {
            // URL state still preserves the selection for this page.
        }
    }

    function removeStoredColumns() {
        try {
            localStorage.removeItem(config.storageKey);
        } catch {
            // Storage may be unavailable in privacy-restricted contexts.
        }
    }

    function exportCsv() {
        const visibleColumns = columns.filter(item => state.visible.includes(item.key));
        const csv = [
            visibleColumns.map(item => csvValue(item.label)).join(","),
            ...state.currentRows.map(row => visibleColumns.map(item => {
                const value = item.value(row);
                return csvValue(item.type === "number" && value !== null && value !== undefined
                    ? item.transform(Number(value))
                    : item.type === "date" && value !== null
                        ? new Date(value).toISOString()
                        : value);
            }).join(","))
        ].join("\n");
        download(`${config.fileBase}.csv`, "text/csv;charset=utf-8", csv);
    }

    function exportJson() {
        download(`${config.fileBase}.json`, "application/json", JSON.stringify(state.currentRows, jsonReplacer, 2));
    }

    function jsonReplacer(key, value) {
        return key.startsWith("_") ? undefined : value;
    }

    function csvValue(value) {
        const text = value === null || value === undefined ? "" : String(value);
        return `"${text.replaceAll("\"", "\"\"")}"`;
    }

    function download(fileName, type, content) {
        const url = URL.createObjectURL(new Blob([content], { type }));
        const link = document.createElement("a");
        link.href = url;
        link.download = fileName;
        link.click();
        URL.revokeObjectURL(url);
    }

    function cloneFilters(filters) {
        return JSON.parse(JSON.stringify(filters));
    }

    function parseDate(value) {
        if (!value) return null;
        const timestamp = Date.parse(value);
        return Number.isFinite(timestamp) ? timestamp : null;
    }

    function dateInputValue(timestamp) {
        const date = new Date(timestamp);
        return new Date(timestamp - date.getTimezoneOffset() * 60 * 1000)
            .toISOString()
            .slice(0, 16);
    }

    function humanize(value) {
        if (value === null || value === undefined || value === "") return "Unknown";
        return String(value)
            .replaceAll("_", " ")
            .replace(/\b\w/g, character => character.toUpperCase());
    }

    function metadataTitle(metadata) {
        if (!metadata) return "";
        return [
            metadata.description,
            metadata.unit ? `Unit: ${metadata.unit}` : "",
            metadata.formula ? `Formula: ${metadata.formula}` : "",
            metadata.source ? `Source: ${metadata.source}` : "",
            metadata.aggregation ? `Aggregation: ${metadata.aggregation}` : "",
            metadata.warning ? `Warning: ${metadata.warning}` : ""
        ].filter(Boolean).join("\n");
    }

    function formatInteger(value) {
        return new Intl.NumberFormat().format(value);
    }
})();
