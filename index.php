<?php
// index.php
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/inc/LakehouseClient.php';

$env = getenv('APP_ENV') ?: 'dev';
$lakehouse = new LakehouseClient($env);

echo "--- Lakehouse Explorer [$env] ---\n";

try {
    // Test 1: Show all catalogs available in Trino
    $sql = "SELECT merchantID, channel, bill_amt FROM iceberg.rds_master_onlinepayment1.transaction";
    echo "Executing: $sql\n";
    $results = $lakehouse->query($sql);

    echo "Found " . count($results) . " rows:\n";
    $columns = array_keys($results[0]);
    echo implode("\t| ", $columns) . "\n";
    echo str_repeat("-", 50) . "\n";
    foreach ($results as $row) {
        echo implode("\t| ", array_values($row)) . "\n";
    }

} catch (Exception $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
}