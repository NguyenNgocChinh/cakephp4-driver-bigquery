BigQuery Driver for Cakephp4
========

An BigQuery for CakePHP 4.4

## Installing via composer

Install [composer](http://getcomposer.org) and run:

```bash
composer require nguyenngocchinh/cakephp4-driver-bigquery
```

## Defining a connection
Now, you need to set the connection in your config/app.php file:

```php
 'Datasources' => [
...
    'bigquery' => [
        'className' => 'Chinh\BigQuery\BigQueryConnection',
        'driver' => 'Chinh\BigQuery\BigQueryDriver',
        'projectId' => env('BIGQUERY_PROJECT_ID', 'project_id'),
        'dataSet' => env('BIGQUERY_DATASET', 'dataset'),
        'keyFilePath' => null, //The full path to your service account credentials .json file retrieved.
    ],
],
```

## Models
...

### Table
...

### Entity
...

## Controllers
...

## LICENSE

[The MIT License (MIT) Copyright (c) 2021](http://opensource.org/licenses/MIT)