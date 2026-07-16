(() => {
    "use strict";

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

    initializeFromUrl();
    buildFilterPanel();
    buildColumnPanel();
    bindControls();
    loadRows();

    function tableConfig(kind, referenceTime) {
        const configs = {
            channels: {
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
                    "negative-roic": { net_roic_percent: { lt: 0 } },
                    disconnected: { connected: { eq: "false" } },
                    "no-forwards": { settled_forward_count: { max: 0 } }
                },
                columns: channelColumns()
            },
            forwards: {
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
            column("gross_roic_percent", "Gross ROIC", "number", { suffix: "%", decimals: 2, signedClass: true }),
            column("net_roic_percent", "Net ROIC", "number", { visible: true, suffix: "%", decimals: 2, signedClass: true }),
            column("indirect_roic_percent", "Indirect ROIC", "number", { visible: true, suffix: "%", decimals: 2, signedClass: true }),
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
            column("fee_ppm", "Fee PPM", "number", { visible: true, value: row => row._feePpm, suffix: " ppm", decimals: 1 }),
            column("elapsed_seconds", "Elapsed", "number", { visible: true, value: row => row._elapsedSeconds, suffix: " s", decimals: 1 }),
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
            badge: options.badge ?? false
        };
    }

    function prepareForward(row) {
        row._receivedAt = parseDate(row.received_at);
        row._resolvedAt = parseDate(row.resolved_at);
        row._feePpm = row.fee_msat !== null && row.out_msat
            ? row.fee_msat * 1_000_000 / row.out_msat
            : null;
        row._elapsedSeconds = row._receivedAt !== null && row._resolvedAt !== null
            ? (row._resolvedAt - row._receivedAt) / 1000
            : null;
        return row;
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

    function formatInteger(value) {
        return new Intl.NumberFormat().format(value);
    }
})();
