package org.example.eventsystem.distributed;

import org.example.eventsystem.bus.EventBus;
import org.example.eventsystem.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages distributed event processing.
 * <p>
 * This class provides an abstraction layer for integrating with message brokers
 * like Kafka or RabbitMQ for distributed event processing. It allows events to be
 * published to and consumed from remote systems.
 * </p>
 */
public class DistributedEventManager {

    private static final Logger logger = LoggerFactory.getLogger(DistributedEventManager.class);

    private final EventBus localEventBus;
    private final Map<String, MessageBrokerAdapter> brokerAdapters;
    private final AtomicBoolean running;

    /**
     * Creates a new distributed event manager.
     *
     * @param localEventBus the local event bus
     */
    public DistributedEventManager(EventBus localEventBus) {
        this.localEventBus = localEventBus;
        this.brokerAdapters = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
    }

    /**
     * Registers a message broker adapter.
     *
     * @param name the name of the adapter
     * @param adapter the adapter to register
     * @return true if the adapter was registered, false if an adapter with the same name already exists
     */
    public boolean registerBrokerAdapter(String name, MessageBrokerAdapter adapter) {
        if (brokerAdapters.containsKey(name)) {
            logger.warn("Message broker adapter with name '{}' already exists", name);
            return false;
        }
        
        brokerAdapters.put(name, adapter);
        logger.info("Registered message broker adapter: {}", name);
        
        // If already running, start the adapter
        if (running.get()) {
            adapter.start();
        }
        
        return true;
    }

    /**
     * Unregisters a message broker adapter.
     *
     * @param name the name of the adapter to unregister
     * @return true if the adapter was unregistered, false if no adapter with the given name exists
     */
    public boolean unregisterBrokerAdapter(String name) {
        MessageBrokerAdapter adapter = brokerAdapters.remove(name);
        
        if (adapter != null) {
            // Stop the adapter if it's running
            if (running.get()) {
                adapter.stop();
            }
            
            logger.info("Unregistered message broker adapter: {}", name);
            return true;
        }
        
        return false;
    }

    /**
     * Starts the distributed event manager.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting DistributedEventManager");
            
            // Start all registered adapters
            for (Map.Entry<String, MessageBrokerAdapter> entry : brokerAdapters.entrySet()) {
                try {
                    entry.getValue().start();
                    logger.info("Started message broker adapter: {}", entry.getKey());
                } catch (Exception e) {
                    logger.error("Failed to start message broker adapter: {}", entry.getKey(), e);
                }
            }
        }
    }

    /**
     * Stops the distributed event manager.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping DistributedEventManager");
            
            // Stop all registered adapters
            for (Map.Entry<String, MessageBrokerAdapter> entry : brokerAdapters.entrySet()) {
                try {
                    entry.getValue().stop();
                    logger.info("Stopped message broker adapter: {}", entry.getKey());
                } catch (Exception e) {
                    logger.error("Failed to stop message broker adapter: {}", entry.getKey(), e);
                }
            }
        }
    }

    /**
     * Publishes an event to a specific message broker.
     *
     * @param brokerName the name of the message broker
     * @param event the event to publish
     * @param <T> the type of the event
     * @return true if the event was published, false otherwise
     */
    public <T extends Event> boolean publishToRemote(String brokerName, T event) {
        if (!running.get()) {
            logger.warn("Cannot publish event: DistributedEventManager is not running");
            return false;
        }
        
        MessageBrokerAdapter adapter = brokerAdapters.get(brokerName);
        if (adapter == null) {
            logger.warn("Cannot publish event: no message broker adapter found with name '{}'", brokerName);
            return false;
        }
        
        try {
            adapter.publishEvent(event);
            logger.debug("Published event {} to remote broker {}", event, brokerName);
            return true;
        } catch (Exception e) {
            logger.error("Failed to publish event to remote broker {}", brokerName, e);
            return false;
        }
    }

    /**
     * Publishes an event to all registered message brokers.
     *
     * @param event the event to publish
     * @param <T> the type of the event
     * @return the number of brokers the event was successfully published to
     */
    public <T extends Event> int publishToAllRemotes(T event) {
        if (!running.get()) {
            logger.warn("Cannot publish event: DistributedEventManager is not running");
            return 0;
        }
        
        int successCount = 0;
        
        for (Map.Entry<String, MessageBrokerAdapter> entry : brokerAdapters.entrySet()) {
            try {
                entry.getValue().publishEvent(event);
                logger.debug("Published event {} to remote broker {}", event, entry.getKey());
                successCount++;
            } catch (Exception e) {
                logger.error("Failed to publish event to remote broker {}", entry.getKey(), e);
            }
        }
        
        return successCount;
    }

    /**
     * Gets the local event bus.
     *
     * @return the local event bus
     */
    public EventBus getLocalEventBus() {
        return localEventBus;
    }

    /**
     * Checks if the distributed event manager is running.
     *
     * @return true if the manager is running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }
}
