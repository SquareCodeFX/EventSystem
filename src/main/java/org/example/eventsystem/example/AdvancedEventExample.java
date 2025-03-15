package org.example.eventsystem.example;

import org.example.eventsystem.async.AsyncEventProcessor;
import org.example.eventsystem.bus.DefaultEventBus;
import org.example.eventsystem.bus.EventBus;
import org.example.eventsystem.distributed.DistributedEventManager;
import org.example.eventsystem.distributed.kafka.KafkaMessageBrokerAdapter;
import org.example.eventsystem.distributed.rabbitmq.RabbitMQMessageBrokerAdapter;
import org.example.eventsystem.event.AbstractEvent;
import org.example.eventsystem.listener.AbstractEventListener;
import org.example.eventsystem.persistence.PersistentEventManager;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An advanced example demonstrating the more complex features of the event system.
 * <p>
 * This example shows how to use:
 * - AsyncEventProcessor for asynchronous event processing
 * - PersistentEventManager for event persistence
 * - DistributedEventManager for distributed event processing
 * </p>
 */
public class AdvancedEventExample {

    /**
     * Main method to run the example.
     *
     * @param args command line arguments (not used)
     */
    public static void main(String[] args) {
        // Create an event bus
        EventBus eventBus = new DefaultEventBus();

        // Register a listener for our events
        eventBus.register(new AbstractEventListener<AdvancedEvent>() {
            @Override
            public void onEvent(AdvancedEvent event) {
                System.out.println("Received event: " + event.getMessage() + " at " + event.getTimestamp());
            }
        });

        // Demonstrate AsyncEventProcessor
        demonstrateAsyncEventProcessor(eventBus);

        // Demonstrate PersistentEventManager
        demonstratePersistentEventManager();

        // Demonstrate DistributedEventManager
        demonstrateDistributedEventManager(eventBus);

        // Shutdown the event bus
        eventBus.shutdown();
    }

    /**
     * Demonstrates the use of AsyncEventProcessor.
     *
     * @param eventBus the event bus to use
     */
    private static void demonstrateAsyncEventProcessor(EventBus eventBus) {
        System.out.println("\n=== AsyncEventProcessor Example ===");

        // Create an async event processor
        AsyncEventProcessor asyncProcessor = new AsyncEventProcessor(eventBus);
        asyncProcessor.start();

        // Queue events for asynchronous processing
        System.out.println("Queuing events for async processing...");
        asyncProcessor.queueEvent(new AdvancedEvent("Queued event 1"));
        asyncProcessor.queueEvent(new AdvancedEvent("Queued event 2"));

        // Schedule an event to be published after a delay
        System.out.println("Scheduling a delayed event...");
        asyncProcessor.scheduleEvent(new AdvancedEvent("Delayed event"), 2, TimeUnit.SECONDS);

        // Schedule a repeating event
        System.out.println("Scheduling a repeating event...");
        Runnable cancelTask = asyncProcessor.scheduleRepeatingEvent(
                () -> new AdvancedEvent("Repeating event at " + Instant.now()),
                0, 1, TimeUnit.SECONDS
        );

        // Wait a bit to see the events being processed
        try {
            System.out.println("Waiting for events to be processed...");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Cancel the repeating event
        System.out.println("Cancelling repeating event...");
        cancelTask.run();

        // Stop the processor
        System.out.println("Stopping AsyncEventProcessor...");
        asyncProcessor.stop();
    }

    /**
     * Demonstrates the use of PersistentEventManager.
     */
    private static void demonstratePersistentEventManager() {
        System.out.println("\n=== PersistentEventManager Example ===");

        // Create a persistent event manager
        PersistentEventManager persistentManager = new PersistentEventManager("./event_storage", 1);
        persistentManager.start();

        // Store events for persistence
        System.out.println("Storing events for persistence...");
        persistentManager.storeEvent(new AdvancedEvent("Persistent event 1"));
        persistentManager.storeEvent(new AdvancedEvent("Persistent event 2"));

        // Save events to disk
        System.out.println("Saving events to disk...");
        int savedCount = persistentManager.saveAllEvents();
        System.out.println("Saved " + savedCount + " events.");

        // Load events from disk
        System.out.println("Loading events from disk...");
        List<AdvancedEvent> loadedEvents = persistentManager.loadEvents(AdvancedEvent.class);
        System.out.println("Loaded " + loadedEvents.size() + " events:");
        for (AdvancedEvent event : loadedEvents) {
            System.out.println("  - " + event.getMessage() + " at " + event.getTimestamp());
        }

        // Clear events
        System.out.println("Clearing events...");
        int clearedCount = persistentManager.clearAllEvents();
        System.out.println("Cleared " + clearedCount + " events.");

        // Stop the manager
        System.out.println("Stopping PersistentEventManager...");
        persistentManager.stop();
    }

    /**
     * Demonstrates the use of DistributedEventManager.
     *
     * @param eventBus the event bus to use
     */
    private static void demonstrateDistributedEventManager(EventBus eventBus) {
        System.out.println("\n=== DistributedEventManager Example ===");

        // Create a distributed event manager
        DistributedEventManager distributedManager = new DistributedEventManager(eventBus);

        // Register message broker adapters
        // Note: These are simulated adapters and won't actually connect to Kafka or RabbitMQ
        System.out.println("Registering message broker adapters...");
        distributedManager.registerBrokerAdapter("kafka", 
                new KafkaMessageBrokerAdapter("localhost:9092", eventBus));
        distributedManager.registerBrokerAdapter("rabbitmq", 
                new RabbitMQMessageBrokerAdapter("localhost", "guest", "guest", eventBus));

        // Start the distributed event manager
        System.out.println("Starting DistributedEventManager...");
        distributedManager.start();

        // Publish events to remote brokers
        try {
            System.out.println("Publishing events to remote brokers...");
            distributedManager.publishToRemote("kafka", new AdvancedEvent("Kafka event"));
            distributedManager.publishToRemote("rabbitmq", new AdvancedEvent("RabbitMQ event"));
            distributedManager.publishToAllRemotes(new AdvancedEvent("Broadcast event"));
        } catch (Exception e) {
            System.out.println("Error publishing to remote brokers: " + e.getMessage());
        }

        // Stop the manager
        System.out.println("Stopping DistributedEventManager...");
        distributedManager.stop();
    }

    /**
     * An advanced example event that is serializable for persistence.
     */
    public static class AdvancedEvent extends AbstractEvent implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final String message;

        public AdvancedEvent(String message) {
            super("AdvancedEvent", AdvancedEventExample.class, true);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }
}
