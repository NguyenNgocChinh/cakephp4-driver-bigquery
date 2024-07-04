<?php
// Load composer autoload.
require dirname(__DIR__) . '/vendor/autoload.php';

// Use the test app config file.
use Cake\Core\Configure;
use Cake\Core\Configure\Engine\PhpConfig;
use Cake\Routing\Router;

if (file_exists(dirname(__DIR__) . '/config/.env')) {
    $dotenv = new \josegonzalez\Dotenv\Loader([dirname(__DIR__) . '/config/.env']);
    $dotenv->parse()
        ->putenv()
        ->toEnv()
        ->toServer();
}

// Define CONFIG constant if not already defined
if (!defined('CONFIG')) {
    define('CONFIG', dirname(__DIR__) . '/config/');
}

Configure::config('default', new PhpConfig());
Configure::load('app', 'default', false);
Configure::load('app_test', 'default', true);

// Set the router to the test environment.
Router::reload();

