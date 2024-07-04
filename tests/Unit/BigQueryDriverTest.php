<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Chinh\BigQuery\BigQueryDriver;

class BigQueryDriverTest extends TestCase
{
    public function testInitialization(): void
    {
        $config = ['projectId' => 'my-project'];
        $driver = new BigQueryDriver($config);
        $this->assertInstanceOf(BigQueryDriver::class, $driver);
    }
}
