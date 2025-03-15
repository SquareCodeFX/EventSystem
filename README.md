# Event System

A well-structured subscription-based event system with support for synchronous and asynchronous operations, event priorities, cancellation, and more.

## Features

- **Synchronous and Asynchronous Event Processing**: Support for both blocking and non-blocking event publishing.
- **Event Priorities**: Define the order in which listeners receive events.
- **Event Cancellation**: Ability to cancel events to prevent further processing.
- **Thread Safety**: Designed for concurrent use with proper synchronization.
- **Event Cascading**: Events can trigger further events in a chain.
- **Event Modification**: Listeners can modify events before they are passed on.
- **Multithreaded Processing**: Parallel event processing for improved performance.
- **Event Queues and Buffering**: Batch processing of events for high-throughput scenarios.
- **Logging and Debugging**: Comprehensive logging throughout the system.
- **Persistent Events**: Optional storage of events to disk for later retrieval.
- **Distributed Event Processing**: Integration with Kafka and RabbitMQ for distributed systems.

## Architecture

The event system is built around the following core components:

- **Event**: The base interface for all events in the system.
- **EventListener**: The interface for classes that want to listen for and handle events.
- **EventBus**: The central component that manages event publishing and listener registration.
- **AsyncEventProcessor**: Provides advanced asynchronous event processing capabilities.
- **PersistentEventManager**: Manages the persistence of events to disk.
- **DistributedEventManager**: Manages distributed event processing with message brokers.

## Getting Started

### Basic Usage

```java
// Create an event bus
EventBus eventBus = new DefaultEventBus();

// Create and register a listener
EventListener<MyEvent> listener = new AbstractEventListener<MyEvent>() {
    @Override
    public void onEvent(MyEvent event) {
        System.out.println("Received event: " + event);
    }
};
eventBus.register(listener);

// Create and publish an event
MyEvent event = new MyEvent("Hello, world!");
eventBus.publish(event);

// Publish an event asynchronously
eventBus.publishAsync(event).thenAccept(e -> {
    System.out.println("Event processed asynchronously: " + e);
});

// Unregister the listener when done
eventBus.unregister(listener);

// Shutdown the event bus
eventBus.shutdown();
```

### Using Event Priorities

```java
// Create listeners with different priorities
EventListener<MyEvent> highPriorityListener = new AbstractEventListener<MyEvent>(EventPriority.HIGH) {
    @Override
    public void onEvent(MyEvent event) {
        System.out.println("High priority listener received event: " + event);
    }
};

EventListener<MyEvent> lowPriorityListener = new AbstractEventListener<MyEvent>(EventPriority.LOW) {
    @Override
    public void onEvent(MyEvent event) {
        System.out.println("Low priority listener received event: " + event);
    }
};

// Register the listeners
eventBus.register(highPriorityListener);
eventBus.register(lowPriorityListener);

// Publish an event - high priority listener will be called first
eventBus.publish(new MyEvent("Priority test"));
```

### Event Cascading

```java
// Create a cascading event listener
CascadingEventListener<MyEvent, MyOtherEvent> cascadingListener = 
    new CascadingEventListener<MyEvent, MyOtherEvent>(eventBus) {
        @Override
        protected MyOtherEvent processEvent(MyEvent event) {
            return new MyOtherEvent("Cascaded from: " + event.getMessage());
        }
    };

// Register the cascading listener and a listener for the cascaded event
eventBus.register(cascadingListener);
eventBus.register(new AbstractEventListener<MyOtherEvent>() {
    @Override
    public void onEvent(MyOtherEvent event) {
        System.out.println("Received cascaded event: " + event);
    }
});

// Publish an event - will trigger the cascaded event
eventBus.publish(new MyEvent("Cascade test"));
```

### Event Filtering

```java
// Create a filtering event listener
FilteringEventListener<MyEvent> filteringListener = new FilteringEventListener<MyEvent>() {
    @Override
    protected boolean filterEvent(MyEvent event) {
        // Only allow events with messages longer than 5 characters
        boolean allowed = event.getMessage().length() > 5;
        System.out.println("Filtering event: " + event + ", allowed: " + allowed);
        return allowed;
    }
};

// Register the filtering listener
eventBus.register(filteringListener);

// These events will be filtered differently
eventBus.publish(new MyEvent("Short")); // Will be cancelled
eventBus.publish(new MyEvent("Long enough")); // Will be allowed
```

### Asynchronous Event Processing

```java
// Create an async event processor
AsyncEventProcessor asyncProcessor = new AsyncEventProcessor(eventBus);
asyncProcessor.start();

// Queue events for asynchronous processing
asyncProcessor.queueEvent(new MyEvent("Queued event 1"));
asyncProcessor.queueEvent(new MyEvent("Queued event 2"));

// Schedule an event to be published after a delay
asyncProcessor.scheduleEvent(new MyEvent("Delayed event"), 5, TimeUnit.SECONDS);

// Schedule a repeating event
asyncProcessor.scheduleRepeatingEvent(
    () -> new MyEvent("Repeating event at " + Instant.now()),
    0, 10, TimeUnit.SECONDS
);

// Stop the processor when done
asyncProcessor.stop();
```

### Persistent Events

```java
// Create a persistent event manager
PersistentEventManager persistentManager = new PersistentEventManager();
persistentManager.start();

// Store events for persistence
persistentManager.storeEvent(new MyEvent("Persistent event 1"));
persistentManager.storeEvent(new MyEvent("Persistent event 2"));

// Save events to disk
persistentManager.saveAllEvents();

// Load events from disk
List<MyEvent> loadedEvents = persistentManager.loadEvents(MyEvent.class);
for (MyEvent event : loadedEvents) {
    System.out.println("Loaded event: " + event);
}

// Stop the manager when done
persistentManager.stop();
```

### Distributed Event Processing

```java
// Create a distributed event manager
DistributedEventManager distributedManager = new DistributedEventManager(eventBus);

// Register a Kafka adapter
distributedManager.registerBrokerAdapter("kafka", 
    new KafkaMessageBrokerAdapter("localhost:9092", eventBus));

// Register a RabbitMQ adapter
distributedManager.registerBrokerAdapter("rabbitmq", 
    new RabbitMQMessageBrokerAdapter("localhost", "guest", "guest", eventBus));

// Start the distributed event manager
distributedManager.start();

// Publish an event to a specific broker
distributedManager.publishToRemote("kafka", new MyEvent("Kafka event"));

// Publish an event to all brokers
distributedManager.publishToAllRemotes(new MyEvent("Broadcast event"));

// Stop the manager when done
distributedManager.stop();
```

## Creating Custom Events

To create a custom event, extend the `AbstractEvent` class:

```java
public class MyEvent extends AbstractEvent {
    private final String message;

    public MyEvent(String message) {
        super("MyEvent", null, true); // Name, source, cancellable
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
