package org.example.eventsystem.listener;

import org.example.eventsystem.event.Event;
import org.example.eventsystem.event.EventPriority;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Abstract base implementation of the {@link EventListener} interface.
 * <p>
 * This class provides a common implementation for event listeners, automatically
 * determining the event type from the generic type parameter.
 * </p>
 *
 * @param <T> the type of event this listener handles
 */
public abstract class AbstractEventListener<T extends Event> implements EventListener<T> {

    private final Class<T> eventType;
    private final EventPriority priority;
    private final boolean receiveCancelledEvents;

    /**
     * Creates a new event listener with the specified priority and cancelled event handling preference.
     *
     * @param priority the priority of this listener
     * @param receiveCancelledEvents whether this listener should receive cancelled events
     */
    @SuppressWarnings("unchecked")
    protected AbstractEventListener(EventPriority priority, boolean receiveCancelledEvents) {
        Type genericSuperclass = getClass().getGenericSuperclass();
        
        if (genericSuperclass instanceof ParameterizedType) {
            Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
            this.eventType = (Class<T>) typeArguments[0];
        } else {
            throw new IllegalStateException("Unable to determine event type for " + getClass().getName());
        }
        
        this.priority = priority;
        this.receiveCancelledEvents = receiveCancelledEvents;
    }

    /**
     * Creates a new event listener with the specified priority.
     * The listener will not receive cancelled events.
     *
     * @param priority the priority of this listener
     */
    protected AbstractEventListener(EventPriority priority) {
        this(priority, false);
    }

    /**
     * Creates a new event listener with normal priority.
     * The listener will not receive cancelled events.
     */
    protected AbstractEventListener() {
        this(EventPriority.NORMAL, false);
    }

    @Override
    public Class<T> getEventType() {
        return eventType;
    }

    @Override
    public EventPriority getPriority() {
        return priority;
    }

    @Override
    public boolean receiveCancelledEvents() {
        return receiveCancelledEvents;
    }
}
