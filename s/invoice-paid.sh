#!/bin/sh

CMD=${TEST_CMD:-lightning-cli listinvoices}  # eg: `TEST_CMD="cat test-json/listinvoices" ./paid-invoice.sh`

$CMD | jq '.invoices.[] | select(.status=="paid") | {label, description, amount_received_msat, paid_at: (.paid_at | todate) }'

