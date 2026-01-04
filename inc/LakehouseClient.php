<?php
// inc/LakehouseClient.php

use Aws\Athena\AthenaClient;
use GuzzleHttp\Client as GuzzleClient;

class LakehouseClient {
    private $env;
    private $catalog;
    private $guzzle;
    private $athena;

    public function __construct($env = 'dev') {
        $this->env = $env;
        $this->catalog = 'awsdatacatalog';

        if ($this->env === 'production') {
            $this->athena = new AthenaClient([
                'region'  => 'ap-southeast-1',
                'version' => 'latest',
            ]);
        } else {
            $this->guzzle = new GuzzleClient([
                'base_uri' => 'http://lakehouse-athena:8080',
                'timeout'  => 60.0,
            ]);
        }
    }

    public function query(string $sql): array {
        if ($this->env === 'production') {
            return $this->queryAthena($sql);
        }
        return $this->queryTrino($sql);
    }

    private function queryTrino(string $sql): array {
        $response = $this->guzzle->post('/v1/statement', [
            'body' => $sql,
            'headers' => [
                'X-Trino-User'    => 'admin',
                'X-Trino-Catalog' => $this->catalog,
                'X-Trino-Schema'  => 'default',
            ]
        ]);
        $contents = json_decode($response->getBody()->getContents(), true);
        while (isset($contents['nextUri'])) {
            // check complete or error
            if (isset($contents['data'])) {
                return $this->formatTrinoResults($contents);
            }
            if (isset($contents['error'])) {
                throw new Exception("Trino Error: " . $contents['error']['message']);
            }
            usleep(100000); 
            $response = $this->guzzle->get($contents['nextUri']);
            $contents = json_decode($response->getBody()->getContents(), true);
        }
        return [];
    }

    private function queryAthena(string $sql): array {
        $execution = $this->athena->startQueryExecution([
            'QueryString' => $sql,
            'QueryExecutionContext' => [
                'Catalog' => $this->catalog,
                'Database' => 'default'
            ],
            'ResultConfiguration' => [
                'OutputLocation' => getenv('ATHENA_OUTPUT_S3_PATH'), 
            ],
        ]);
        $queryExecutionId = $execution['QueryExecutionId'];

        // poll for completion
        $finished = false;
        while (!$finished) {
            $status = $this->athena->getQueryExecution([
                'QueryExecutionId' => $queryExecutionId
            ]);
            $state = $status['QueryExecution']['Status']['State'];
            switch ($state) {
                case 'SUCCEEDED':
                    $finished = true;
                    break;
                case 'FAILED':
                    $reason = $status['QueryExecution']['Status']['StateChangeReason'];
                    throw new Exception("Athena Query Failed: $reason");
                case 'CANCELLED':
                    throw new Exception("Athena Query was cancelled.");
                default:
                    sleep(1); 
                    break;
            }
        }

        $results = $this->athena->getQueryResults([
            'QueryExecutionId' => $queryExecutionId
        ]);
        return $this->formatAthenaResults($results);
    }

    private function formatTrinoResults(array $contents): array {
        if (!isset($contents['data']) || !isset($contents['columns'])) {
            return [];
        }
        $columns = array_column($contents['columns'], 'name');
        $finalData = [];
        foreach ($contents['data'] as $row) {
            $finalData[] = array_combine($columns, $row);
        }
        return $finalData;
    }

    private function formatAthenaResults($results): array {
        $rows = $results['ResultSet']['Rows'];
        if (empty($rows)) return [];

        // header
        $columnHeaderRow = array_shift($rows); 
        $columnNames = array_map(function($col) {
            return $col['VarCharValue'] ?? 'unknown';
        }, $columnHeaderRow['Data']);

        // data rows
        $finalData = [];
        foreach ($rows as $row) {
            $rowData = [];
            foreach ($row['Data'] as $index => $cell) {
                $key = $columnNames[$index];
                $rowData[$key] = $cell['VarCharValue'] ?? null;
            }
            $finalData[] = $rowData;
        }

        return $finalData;
    }
}