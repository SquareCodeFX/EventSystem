package org.example.eventsystem.example;

import org.example.eventsystem.bus.DefaultEventBus;
import org.example.eventsystem.bus.EventBus;
import org.example.eventsystem.event.AbstractEvent;
import org.example.eventsystem.event.EventPriority;
import org.example.eventsystem.listener.AbstractEventListener;
import org.example.eventsystem.listener.CascadingEventListener;
import org.example.eventsystem.listener.EventListener;
import org.example.eventsystem.listener.FilteringEventListener;

/**
 * A simple example demonstrating the basic usage of the event system.
 */
public class SimpleEventExample {

    /**
     * Main method to run the example.
     *
     * @param args command line arguments (not used)
     */
    public static void main(String[] args) {
        // Create an event bus
        EventBus eventBus = new DefaultEventBus();

        // Create and register a basic listener
        EventListener<ExampleEvent> basicListener = new AbstractEventListener<ExampleEvent>() {
            @Override
            public void onEvent(ExampleEvent event) {
                System.out.println("Basic listener received event: " + event.getMessage());
            }
        };
        eventBus.register(basicListener);

        // Create and register listeners with different priorities
        EventListener<ExampleEvent> highPriorityListener = new AbstractEventListener<ExampleEvent>(EventPriority.HIGH) {
            @Override
            public void onEvent(ExampleEvent event) {
                System.out.println("High priority listener received event: " + event.getMessage());
            }
        };
        eventBus.register(highPriorityListener);

        EventListener<ExampleEvent> lowPriorityListener = new AbstractEventListener<ExampleEvent>(EventPriority.LOW) {
            @Override
            public void onEvent(ExampleEvent event) {
                System.out.println("Low priority listener received event: " + event.getMessage());
            }
        };
        eventBus.register(lowPriorityListener);

        // Create and register a filtering listener
        FilteringEventListener<ExampleEvent> filteringListener = new FilteringEventListener<ExampleEvent>() {
            @Override
            protected boolean filterEvent(ExampleEvent event) {
                // Only allow events with messages longer than 5 characters
                boolean allowed = event.getMessage().length() > 5;
                System.out.println("Filtering event: " + event.getMessage() + ", allowed: " + allowed);
                return allowed;
            }
        };
        eventBus.register(filteringListener);

        // Create and register a cascading listener
        CascadingEventListener<ExampleEvent, CascadedEvent> cascadingListener = 
            new CascadingEventListener<ExampleEvent, CascadedEvent>(eventBus) {
                @Override
                protected CascadedEvent processEvent(ExampleEvent event) {
                    return new CascadedEvent("Cascaded from: " + event.getMessage());
                }
            };
        eventBus.register(cascadingListener);

        // Register a listener for the cascaded events
        eventBus.register(new AbstractEventListener<CascadedEvent>() {
            @Override
            public void onEvent(CascadedEvent event) {
                System.out.println("Received cascaded event: " + event.getMessage());
            }
        });

        // Publish some events
        System.out.println("\n--- Publishing a basic event ---");
        eventBus.publish(new ExampleEvent("Hello, world!"));

        System.out.println("\n--- Publishing a short event (will be filtered) ---");
        eventBus.publish(new ExampleEvent("Short"));

        System.out.println("\n--- Publishing an event asynchronously ---");
        eventBus.publishAsync(new ExampleEvent("Async event")).thenAccept(e -> {
            System.out.println("Async event processing completed: " + e.getMessage());
        });

        // Wait a bit for async processing to complete
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Unregister listeners and shutdown
        System.out.println("\n--- Cleaning up ---");
        eventBus.unregister(basicListener);
        eventBus.unregister(highPriorityListener);
        eventBus.unregister(lowPriorityListener);
        eventBus.unregister(filteringListener);
        eventBus.unregister(cascadingListener);
        eventBus.shutdown();
    }

    /**
     * A simple example event.
     */
    public static class ExampleEvent extends AbstractEvent {
        private final String message;

        public ExampleEvent(String message) {
            super("ExampleEvent", SimpleEventExample.class, true);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * A simple cascaded event.
     */
    public static class CascadedEvent extends AbstractEvent {
        private final String message;

        public CascadedEvent(String message) {
            super("CascadedEvent", SimpleEventExample.class, false);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }
}
