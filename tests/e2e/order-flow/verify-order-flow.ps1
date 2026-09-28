$ErrorActionPreference = 'Stop'

$resultsDir = 'tests/results/e2e'
if (-not (Test-Path -Path $resultsDir)) {
  New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null
}

$results = [ordered]@{
  timestamp = (Get-Date).ToUniversalTime().ToString('o')
  health = @()
  orderResponse = $null
  postgres = $null
  rabbitmq = $null
  consumerState = [ordered]@{}
}

$healthUrls = @(
  'http://127.0.0.1:3000/health',
  'http://127.0.0.1:3001/health',
  'http://127.0.0.1:3002/health',
  'http://127.0.0.1:3003/health',
  'http://127.0.0.1:3004/health',
  'http://127.0.0.1:3005/health'
)

foreach ($url in $healthUrls) {
  $results.health += Invoke-RestMethod -Uri $url
}

$payload = @{customerId='cust-e2e-001'; productId='sku-001'; quantity=2; totalAmount=149.99} | ConvertTo-Json -Compress
$results.orderResponse = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:3000/orders' -ContentType 'application/json' -Body $payload

Start-Sleep -Seconds 4

$results.postgres = (& docker compose exec -T postgres psql -U postgres -d ecommerce -c "SELECT id, customer_id, product_id, quantity, total_amount, status, created_at FROM orders ORDER BY created_at DESC LIMIT 5;" 2>$null) -join "`n"
$results.rabbitmq = (& docker compose exec -T rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged 2>$null) -join "`n"

$results.consumerState.payment = Invoke-RestMethod -Uri 'http://127.0.0.1:3002/debug/processed'
$results.consumerState.inventory = Invoke-RestMethod -Uri 'http://127.0.0.1:3003/inventory/stock'
$results.consumerState.notification = Invoke-RestMethod -Uri 'http://127.0.0.1:3004/debug/sent'
$results.consumerState.analytics = Invoke-RestMethod -Uri 'http://127.0.0.1:3005/debug/tracked'

$outputPath = Join-Path $resultsDir 'order-flow-evidence.json'
$results | ConvertTo-Json -Depth 20 | Set-Content -Path $outputPath

$results | ConvertTo-Json -Depth 20
