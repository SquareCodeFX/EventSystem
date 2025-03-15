package org.example.eventsystem.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Abstract base implementation of the {@link Event} interface.
 * <p>
 * This class provides a common implementation for all events in the system.
 * Specific event types should extend this class rather than implementing
 * the {@link Event} interface directly.
 * </p>
 */
public abstract class AbstractEvent implements Event {
    private final UUID id;
    private final Instant timestamp;
    private final String name;
    private final Object source;
    private final boolean cancellable;
    private boolean cancelled;

    /**
     * Creates a new event with the specified name and source.
     *
     * @param name the name of the event
     * @param source the source that generated this event
     * @param cancellable whether this event can be cancelled
     */
    protected AbstractEvent(String name, Object source, boolean cancellable) {
        this.id = UUID.randomUUID();
        this.timestamp = Instant.now();
        this.name = name;
        this.source = source;
        this.cancellable = cancellable;
        this.cancelled = false;
    }

    /**
     * Creates a new event with the specified name and source.
     * The event will not be cancellable.
     *
     * @param name the name of the event
     * @param source the source that generated this event
     */
    protected AbstractEvent(String name, Object source) {
        this(name, source, false);
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getSource() {
        return source;
    }

    @Override
    public boolean isCancellable() {
        return cancellable;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean cancel() {
        if (cancellable) {
            cancelled = true;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", name='" + name + '\'' +
                ", source=" + source +
                ", cancellable=" + cancellable +
                ", cancelled=" + cancelled +
                '}';
    }
}
