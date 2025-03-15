package org.example.eventsystem.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Base interface for all events in the event system.
 * <p>
 * Events are immutable data carriers that represent something that has happened
 * in the system. They contain information about what happened and when it happened.
 * </p>
 */
public interface Event {

    /**
     * Gets the unique identifier of this event.
     *
     * @return the event's unique identifier
     */
    UUID getId();

    /**
     * Gets the timestamp when this event was created.
     *
     * @return the event creation timestamp
     */
    Instant getTimestamp();

    /**
     * Gets the name of this event.
     *
     * @return the event name
     */
    String getName();

    /**
     * Gets the source that generated this event.
     *
     * @return the event source
     */
    Object getSource();

    /**
     * Checks if this event can be cancelled.
     *
     * @return true if the event can be cancelled, false otherwise
     */
    boolean isCancellable();

    /**
     * Checks if this event has been cancelled.
     * Only applicable if {@link #isCancellable()} returns true.
     *
     * @return true if the event has been cancelled, false otherwise
     */
    boolean isCancelled();

    /**
     * Attempts to cancel this event.
     * Only applicable if {@link #isCancellable()} returns true.
     *
     * @return true if the event was successfully cancelled, false if the event cannot be cancelled
     */
    boolean cancel();
}
