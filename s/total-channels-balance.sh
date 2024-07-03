#!/bin/sh

./channels-balance.sh | jq '{our_sats:[.[].our_sats] | add, tot_sats:[.[].tot_sats] | add} | {our_sats,tot_sats,diff:(.our_sats-(.tot_sats/2) | floor), perc:(.our_sats/.tot_sats * 100 | floor)}'
