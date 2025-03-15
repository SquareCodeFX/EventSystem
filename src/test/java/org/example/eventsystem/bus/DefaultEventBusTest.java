package org.example.eventsystem.bus;

import org.example.eventsystem.event.AbstractEvent;
import org.example.eventsystem.event.Event;
import org.example.eventsystem.event.EventPriority;
import org.example.eventsystem.listener.AbstractEventListener;
import org.example.eventsystem.listener.EventListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the DefaultEventBus class.
 */
class DefaultEventBusTest {

    private EventBus eventBus;
    private TestEvent testEvent;
    private TestListener testListener;

    @BeforeEach
    void setUp() {
        eventBus = new DefaultEventBus();
        testEvent = new TestEvent("Test Event");
        testListener = new TestListener();
    }

    @AfterEach
    void tearDown() {
        eventBus.shutdown();
    }

    @Test
    void testRegisterAndUnregister() {
        // Register the listener
        boolean registered = eventBus.register(testListener);
        assertTrue(registered, "Listener should be registered successfully");
        assertTrue(eventBus.hasListenersFor(TestEvent.class), "EventBus should have listeners for TestEvent");
        assertEquals(1, eventBus.getListenerCount(TestEvent.class), "EventBus should have 1 listener for TestEvent");

        // Unregister the listener
        boolean unregistered = eventBus.unregister(testListener);
        assertTrue(unregistered, "Listener should be unregistered successfully");
        assertFalse(eventBus.hasListenersFor(TestEvent.class), "EventBus should not have listeners for TestEvent");
        assertEquals(0, eventBus.getListenerCount(TestEvent.class), "EventBus should have 0 listeners for TestEvent");
    }

    @Test
    void testPublishSynchronous() {
        // Register the listener
        eventBus.register(testListener);

        // Publish the event
        Event result = eventBus.publish(testEvent);

        // Verify the event was received
        assertEquals(1, testListener.getReceivedEvents().size(), "Listener should receive 1 event");
        assertSame(testEvent, testListener.getReceivedEvents().get(0), "Listener should receive the test event");
        assertSame(testEvent, result, "publish() should return the published event");
    }

    @Test
    void testPublishAsynchronous() throws ExecutionException, InterruptedException, TimeoutException {
        // Register the listener
        eventBus.register(testListener);

        // Publish the event asynchronously
        CompletableFuture<Event> future = eventBus.publishAsync(testEvent);
        Event result = future.get(5, TimeUnit.SECONDS);

        // Verify the event was received
        assertEquals(1, testListener.getReceivedEvents().size(), "Listener should receive 1 event");
        assertSame(testEvent, testListener.getReceivedEvents().get(0), "Listener should receive the test event");
        assertSame(testEvent, result, "publishAsync() should return the published event");
    }

    @Test
    void testPublishAsynchronousWithCallback() throws ExecutionException, InterruptedException, TimeoutException {
        // Register the listener
        eventBus.register(testListener);

        // Create a callback
        List<Event> callbackEvents = new ArrayList<>();
        CompletableFuture<Event> future = eventBus.publishAsync(testEvent, callbackEvents::add);
        Event result = future.get(5, TimeUnit.SECONDS);

        // Verify the event was received by the listener and the callback
        assertEquals(1, testListener.getReceivedEvents().size(), "Listener should receive 1 event");
        assertSame(testEvent, testListener.getReceivedEvents().get(0), "Listener should receive the test event");
        assertEquals(1, callbackEvents.size(), "Callback should receive 1 event");
        assertSame(testEvent, callbackEvents.get(0), "Callback should receive the test event");
        assertSame(testEvent, result, "publishAsync() should return the published event");
    }

    @Test
    void testEventPriorities() {
        // Create listeners with different priorities
        List<String> executionOrder = new ArrayList<>();
        
        EventListener<TestEvent> highPriorityListener = new AbstractEventListener<TestEvent>(EventPriority.HIGH) {
            @Override
            public void onEvent(TestEvent event) {
                executionOrder.add("HIGH");
            }
        };
        
        EventListener<TestEvent> normalPriorityListener = new AbstractEventListener<TestEvent>(EventPriority.NORMAL) {
            @Override
            public void onEvent(TestEvent event) {
                executionOrder.add("NORMAL");
            }
        };
        
        EventListener<TestEvent> lowPriorityListener = new AbstractEventListener<TestEvent>(EventPriority.LOW) {
            @Override
            public void onEvent(TestEvent event) {
                executionOrder.add("LOW");
            }
        };
        
        // Register the listeners (in reverse priority order to ensure sorting works)
        eventBus.register(lowPriorityListener);
        eventBus.register(normalPriorityListener);
        eventBus.register(highPriorityListener);
        
        // Publish an event
        eventBus.publish(testEvent);
        
        // Verify the execution order
        assertEquals(3, executionOrder.size(), "All listeners should be executed");
        assertEquals("HIGH", executionOrder.get(0), "High priority listener should execute first");
        assertEquals("NORMAL", executionOrder.get(1), "Normal priority listener should execute second");
        assertEquals("LOW", executionOrder.get(2), "Low priority listener should execute third");
    }

    @Test
    void testEventCancellation() {
        // Create a cancelling listener with high priority
        EventListener<TestEvent> cancellingListener = new AbstractEventListener<TestEvent>(EventPriority.HIGH) {
            @Override
            public void onEvent(TestEvent event) {
                event.cancel();
            }
        };
        
        // Create a normal listener that should not receive cancelled events
        List<TestEvent> receivedEvents = new ArrayList<>();
        EventListener<TestEvent> normalListener = new AbstractEventListener<TestEvent>() {
            @Override
            public void onEvent(TestEvent event) {
                receivedEvents.add(event);
            }
        };
        
        // Register the listeners
        eventBus.register(cancellingListener);
        eventBus.register(normalListener);
        
        // Publish an event
        eventBus.publish(testEvent);
        
        // Verify the event was cancelled and not received by the normal listener
        assertTrue(testEvent.isCancelled(), "Event should be cancelled");
        assertTrue(receivedEvents.isEmpty(), "Normal listener should not receive cancelled event");
    }

    /**
     * A test event for unit testing.
     */
    private static class TestEvent extends AbstractEvent {
        private final String message;

        public TestEvent(String message) {
            super("TestEvent", DefaultEventBusTest.class, true);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * A test listener for unit testing.
     */
    private static class TestListener extends AbstractEventListener<TestEvent> {
        private final List<TestEvent> receivedEvents = new ArrayList<>();

        @Override
        public void onEvent(TestEvent event) {
            receivedEvents.add(event);
        }

        public List<TestEvent> getReceivedEvents() {
            return receivedEvents;
        }
    }
}
