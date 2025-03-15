package org.example.eventsystem.listener;

import org.example.eventsystem.event.Event;
import org.example.eventsystem.event.EventPriority;

/**
 * Interface for event listeners.
 * <p>
 * Event listeners are responsible for handling events of a specific type.
 * They can be registered with an event bus to receive notifications when events occur.
 * </p>
 *
 * @param <T> the type of event this listener handles
 */
public interface EventListener<T extends Event> {

    /**
     * Handles an event.
     *
     * @param event the event to handle
     */
    void onEvent(T event);

    /**
     * Gets the type of event this listener handles.
     *
     * @return the event type class
     */
    Class<T> getEventType();

    /**
     * Gets the priority of this listener.
     * <p>
     * Listeners with higher priority will receive events before listeners with lower priority.
     * </p>
     *
     * @return the listener priority
     */
    default EventPriority getPriority() {
        return EventPriority.NORMAL;
    }

    /**
     * Checks if this listener should receive cancelled events.
     * <p>
     * By default, listeners do not receive events that have been cancelled.
     * Override this method to return true if this listener should receive cancelled events.
     * </p>
     *
     * @return true if this listener should receive cancelled events, false otherwise
     */
    default boolean receiveCancelledEvents() {
        return false;
    }
}
