package org.example.eventsystem.bus;

import org.example.eventsystem.event.Event;
import org.example.eventsystem.listener.EventListener;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Interface for the event bus, which is the central component of the event system.
 * <p>
 * The event bus is responsible for registering listeners and publishing events to them.
 * It supports both synchronous and asynchronous event publishing.
 * </p>
 */
public interface EventBus {

    /**
     * Registers an event listener with this event bus.
     *
     * @param listener the listener to register
     * @param <T> the type of event the listener handles
     * @return true if the listener was registered, false if it was already registered
     */
    <T extends Event> boolean register(EventListener<T> listener);

    /**
     * Unregisters an event listener from this event bus.
     *
     * @param listener the listener to unregister
     * @param <T> the type of event the listener handles
     * @return true if the listener was unregistered, false if it wasn't registered
     */
    <T extends Event> boolean unregister(EventListener<T> listener);

    /**
     * Publishes an event to all registered listeners synchronously.
     * <p>
     * This method will block until all listeners have processed the event.
     * </p>
     *
     * @param event the event to publish
     * @param <T> the type of the event
     * @return the published event, which may have been modified by listeners
     */
    <T extends Event> T publish(T event);

    /**
     * Publishes an event to all registered listeners asynchronously.
     * <p>
     * This method will return immediately and the event will be processed in the background.
     * </p>
     *
     * @param event the event to publish
     * @param <T> the type of the event
     * @return a CompletableFuture that will be completed when all listeners have processed the event
     */
    <T extends Event> CompletableFuture<T> publishAsync(T event);

    /**
     * Publishes an event to all registered listeners asynchronously and executes a callback when complete.
     * <p>
     * This method will return immediately and the event will be processed in the background.
     * The callback will be executed when all listeners have processed the event.
     * </p>
     *
     * @param event the event to publish
     * @param callback the callback to execute when all listeners have processed the event
     * @param <T> the type of the event
     * @return a CompletableFuture that will be completed when all listeners have processed the event
     */
    <T extends Event> CompletableFuture<T> publishAsync(T event, Consumer<T> callback);

    /**
     * Checks if there are any listeners registered for a specific event type.
     *
     * @param eventType the event type to check
     * @return true if there are listeners registered for the event type, false otherwise
     */
    boolean hasListenersFor(Class<? extends Event> eventType);

    /**
     * Gets the number of listeners registered for a specific event type.
     *
     * @param eventType the event type to check
     * @return the number of listeners registered for the event type
     */
    int getListenerCount(Class<? extends Event> eventType);

    /**
     * Shuts down this event bus, releasing any resources it holds.
     * <p>
     * After calling this method, the event bus should not be used anymore.
     * </p>
     */
    void shutdown();
}
