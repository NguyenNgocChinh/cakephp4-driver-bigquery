<?php

declare(strict_types=1);

namespace Chinh\BigQuery;

use Cake\Event\Event;
use Cake\ORM\Behavior;
use Cake\ORM\Entity;
use Cake\I18n\FrozenTime;

class BigQueryDateTimeBehavior extends Behavior
{
    protected $_defaultConfig = [
        'created' => 'created',
        'modified' => 'modified',
    ];

    /**
     * {@inheritdoc}
     * 
     * This method is called before an entity is saved. It updates the 'created' and 'modified' fields
     * with the current timestamp in BigQuery DATETIME format.
     *
     * @param \Cake\Event\Event $event The event that triggered this method.
     * @param \Cake\ORM\Entity $entity The entity being saved.
     * @param \ArrayObject $options Additional options passed to the save method.
     * @return void
     */
    public function beforeSave(Event $event, Entity $entity, \ArrayObject $options)
    {
        $config = $this->getConfig();

        if ($entity->isNew() && $config['created']) {
            $entity->set($config['created'], $this->_getCurrentDatetime());
        }

        if ($config['modified']) {
            $entity->set($config['modified'], $this->_getCurrentDatetime());
        }
    }

    /**
     * Returns the current timestamp in BigQuery DATETIME format.
     *
     * @return string The current time in 'Y-m-d H:i:s' format.
     */
    protected function _getCurrentDatetime(): string
    {
        // Return the current time in BigQuery DATETIME format
        return (new FrozenTime())->format('Y-m-d H:i:s');
    }
}
