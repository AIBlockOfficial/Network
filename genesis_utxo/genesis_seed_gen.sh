#!/bin/bash

# Step 1: Ingest the contents of "genesis_pools.json"
genesis_pools=$(cat genesis_pools.json)

# Step 2: Translate each X_structure into an array of Y_structures
y_structures=$(echo $genesis_pools | jq '[.[] | . as $x | range(.total_intervals) | 
    { 
        public_key: $x.address, 
        amount: ($x.interval_amount * 72072000), 
        locktime: ($x.initial_locktime + (86400 * .))
    }]')

# Create a new JSON structure with "compute_seed_utxo" as the key and the Y_structures array as its value
new_block=$(echo '{}' | jq --argjson ys "$y_structures" '.compute_seed_utxo = $ys')

# Save the new JSON structure to "mempool_seed.json"
echo $new_block > mempool_seed.json

echo "Created mempool_seed.json with Genesis UTXO transactions..."
