<?php
// index.php
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/inc/LakehouseClient.php';

$env = getenv('APP_ENV') ?: 'dev';
$lakehouse = new LakehouseClient($env);

try {
    echo "--- Connected to Lakehouse [$env] ---\n";

    // Test Query: List schemas in our catalog
    $catalog = $lakehouse->getCatalog();
    $sql = "SHOW SCHEMAS FROM $catalog";
    
    echo "Executing: $sql\n";
    $results = $lakehouse->query($sql);

    echo "Found " . count($results) . " schemas:\n";
    foreach ($results as $row) {
        // The library returns an associative array
        echo " - " . ($row['Schema'] ?? $row['schema'] ?? implode(',', $row)) . "\n";
    }

} catch (Exception $e) {
    echo "❌ ERROR: " . $e->getMessage() . "\n";
}