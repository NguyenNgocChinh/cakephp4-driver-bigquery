<?php

return [
    'debug' => true,
    'Datasources' => [
        /*
         * The connect to bigquery
         */
        'bigquery' => [
            'className' => 'Chinh\BigQuery\BigQueryConnection',
            'driver' => 'Chinh\BigQuery\BigQueryDriver',
            'projectId' => env('GOOGLE_PROJECT_ID', null),
            'keyFilePath' => './' . env('GOOGLE_CREDENTIALS_PATH', null),
            'dataSet' => env('BIGQUERY_DATASET', null),
        ],
    ],
];
