<?php
declare(strict_types=1);

namespace Chinh\BigQuery;

use Cake\Event\Event;
use Cake\ORM\Behavior;
use Cake\ORM\Entity;
use Cake\I18n\FrozenTime;

class BigQueryTimestampBehavior extends Behavior
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
            $entity->set($config['created'], $this->_getCurrentTimestamp());
        }

        if ($config['modified']) {
            $entity->set($config['modified'], $this->_getCurrentTimestamp());
        }
    }

    /**
     * Returns the current timestamp in BigQuery DATETIME format.
     *
     * @return string The current time in 'Y-m-d H:i:s' format.
     */
    protected function _getCurrentTimestamp(): string
    {
        // Return the current time in BigQuery format
        return (new FrozenTime())->format('Y-m-d\TH:i:s.u\Z');
    }
}
