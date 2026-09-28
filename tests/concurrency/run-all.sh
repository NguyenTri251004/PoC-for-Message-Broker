#!/usr/bin/env bash
set -euo pipefail

for i in $(seq 1 200); do
  curl -sS -X POST http://127.0.0.1:3000/orders \
    -H 'Content-Type: application/json' \
    -d '{"customerId":"customer-'"$i"'","productId":"sku-001","quantity":1,"totalAmount":20}' >/dev/null &
done
wait
printf 'concurrency test complete\n'
