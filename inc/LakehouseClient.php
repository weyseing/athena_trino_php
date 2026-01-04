<?php
// inc/LakehouseClient.php

use Aws\Athena\AthenaClient;
use Clouding\Presto\Presto;

class LakehouseClient {
    private $client;
    private $env;
    private $catalog;

    public function __construct($env = 'dev') {
        $this->env = $env;

        if ($this->env === 'production') {
            $this->client = new AthenaClient([
                'region'  => 'us-east-1', // Change to your region
                'version' => 'latest'
            ]);
            $this->catalog = 'awsdatacatalog';
        } else {
            // Local Trino Setup using the 'clouding' library
            $this->client = new Presto();
            $this->client->addConnection([
                'host'    => 'trino:8080',
                'user'    => 'admin',
                'catalog' => 'iceberg', // Matches your iceberg.properties
                'schema'  => 'default',
            ]);
            $this->catalog = 'iceberg';
        }
    }

    public function query(string $sql) {
        if ($this->env === 'production') {
            return $this->queryAthena($sql);
        } else {
            return $this->queryTrino($sql);
        }
    }

    private function queryTrino($sql) {
        // The library handles the HTTP protocol for us
        return $this->client->connection()->query($sql)->get();
    }

    private function queryAthena($sql) {
        // 1. Start execution
        $execution = $this->client->startQueryExecution([
            'QueryString' => $sql,
            'ResultConfiguration' => [
                'OutputLocation' => 's3://your-athena-query-results-bucket/' 
            ]
        ]);

        $id = $execution['QueryExecutionId'];

        // 2. Wait for completion (Polling)
        while (true) {
            $status = $this->client->getQueryExecution(['QueryExecutionId' => $id]);
            $state = $status['QueryExecution']['Status']['State'];

            if ($state === 'SUCCEEDED') break;
            if (in_array($state, ['FAILED', 'CANCELLED'])) {
                throw new Exception("Athena Query $state: " . $status['QueryExecution']['Status']['StateChangeReason']);
            }
            sleep(1); // Wait 1 second before checking again
        }

        // 3. Fetch and Format Results
        $results = $this->client->getQueryResults(['QueryExecutionId' => $id]);
        return $this->formatAthenaRows($results['ResultSet']['Rows']);
    }

    private function formatAthenaRows($rows) {
        if (empty($rows)) return [];
        
        $data = [];
        $headers = array_column($rows[0]['Data'], 'VarCharValue');

        for ($i = 1; $i < count($rows); $i++) {
            $rowValues = array_column($rows[$i]['Data'], 'VarCharValue');
            $data[] = array_combine($headers, $rowValues);
        }
        return $data;
    }

    public function getCatalog() {
        return $this->catalog;
    }
}