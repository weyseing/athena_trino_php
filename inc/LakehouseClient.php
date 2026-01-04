<?php
// inc/LakehouseClient.php

use Aws\Athena\AthenaClient;
use GuzzleHttp\Client as GuzzleClient;

class LakehouseClient {
    private $env;
    private $catalog;
    private $guzzle;

    public function __construct($env = 'dev') {
        $this->env = $env;
        if ($this->env === 'production') {
            // Athena setup
            $this->catalog = 'awsdatacatalog';
        } else {
            // Local Trino Setup
            $this->guzzle = new GuzzleClient([
                'base_uri' => 'http://lakehouse-athena:8080',
                'timeout'  => 60.0,
            ]);
            $this->catalog = 'iceberg';
        }
    }

    public function getCatalog() {
        return $this->catalog;
    }

    public function query(string $sql): array {
        if ($this->env === 'production') {
            // Add your Athena query logic here
            return [];
        }

        // 1. Start the query
        $response = $this->guzzle->post('/v1/statement', [
            'body' => $sql,
            'headers' => [
                'X-Trino-User'    => 'admin',
                'X-Trino-Catalog' => $this->catalog,
                'X-Trino-Schema'  => 'default',
            ]
        ]);

        $contents = json_decode($response->getBody()->getContents(), true);

        // 2. Poll nextUri until data is available or query finishes
        // Trino often sends back a 'queued' or 'planning' status first with no data
        while (isset($contents['nextUri']) && !isset($contents['data'])) {
            $response = $this->guzzle->get($contents['nextUri']);
            $contents = json_decode($response->getBody()->getContents(), true);

            if (isset($contents['error'])) {
                throw new Exception("Trino SQL Error: " . $contents['error']['message']);
            }
            
            // Wait briefly to avoid overloading the coordinator
            usleep(100000); // 100ms
        }

        return $this->formatResults($contents);
    }

    private function formatResults(array $contents): array {
        if (!isset($contents['data']) || !isset($contents['columns'])) {
            return [];
        }

        $columns = array_column($contents['columns'], 'name');
        $finalData = [];

        foreach ($contents['data'] as $row) {
            // Combine column names with row values
            $finalData[] = array_combine($columns, $row);
        }

        return $finalData;
    }
}