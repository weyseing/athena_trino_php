<?php
// index.php
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/inc/LakehouseClient.php';

$env = getenv('ATHENA_ENV');
$lakehouse = new LakehouseClient($env);

try {
    $targetDb = "glue_db";
    $table = "sink_table2";

    // list databases
    echo "\n[Databases]\n";
    $databases = $lakehouse->query("SHOW SCHEMAS FROM awsdatacatalog");
    foreach ($databases as $db) {
        echo "- " . current($db) . "\n";
    }

    // list tables
    echo "\n[Tables in $targetDb]\n";
    $tables = $lakehouse->query("SHOW TABLES FROM awsdatacatalog.$targetDb");
    foreach ($tables as $t) {
        echo "- " . current($t) . "\n";
    }

    // describe table
    echo "\n[Table Structure]\n";
    $details = $lakehouse->query("SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '$targetDb' 
                AND table_name = '$table'
                ORDER BY ordinal_position");
    foreach ($details as $row) {
        echo trim(current($row)) . " (" . trim(end($row)) . ")\n";
    }

    // query data
    echo "\n[Query Data]\n";
    $sql = "SELECT * FROM awsdatacatalog.$targetDb.$table";
    $results = $lakehouse->query($sql);
    $columns = array_keys($results[0]);
    echo implode("\t| ", $columns) . "\n";
    echo str_repeat("-", 50) . "\n";
    foreach ($results as $row) {
        echo implode("\t| ", array_values($row)) . "\n";
    }
} catch (Exception $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
}