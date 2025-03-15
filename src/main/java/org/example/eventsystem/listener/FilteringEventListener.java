package org.example.eventsystem.listener;

import org.example.eventsystem.event.Event;
import org.example.eventsystem.event.EventPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An event listener that can modify or filter events before they are passed on to other listeners.
 * <p>
 * This listener processes an event and can modify its state or cancel it entirely.
 * It's useful for implementing validation, authorization, or transformation logic
 * that should be applied before other listeners process the event.
 * </p>
 * <p>
 * Filtering listeners should typically have a high priority to ensure they run
 * before other listeners.
 * </p>
 *
 * @param <T> the type of event this listener handles
 */
public abstract class FilteringEventListener<T extends Event> extends AbstractEventListener<T> {

    private static final Logger logger = LoggerFactory.getLogger(FilteringEventListener.class);

    /**
     * Creates a new filtering event listener with the specified priority and cancelled event handling preference.
     *
     * @param priority the priority of this listener
     * @param receiveCancelledEvents whether this listener should receive cancelled events
     */
    protected FilteringEventListener(EventPriority priority, boolean receiveCancelledEvents) {
        super(priority, receiveCancelledEvents);
    }

    /**
     * Creates a new filtering event listener with high priority.
     * The listener will not receive cancelled events.
     */
    protected FilteringEventListener() {
        super(EventPriority.HIGH, false);
    }

    @Override
    public final void onEvent(T event) {
        try {
            // Apply the filter
            boolean shouldContinue = filterEvent(event);
            
            // If the filter returns false, cancel the event if possible
            if (!shouldContinue && event.isCancellable() && !event.isCancelled()) {
                event.cancel();
                logger.debug("Event {} cancelled by filter {}", event, this.getClass().getSimpleName());
            }
        } catch (Exception e) {
            logger.error("Error in filtering event listener", e);
        }
    }

    /**
     * Filters or modifies the event.
     * <p>
     * This method can modify the event's state and/or determine whether the event
     * should be passed on to other listeners.
     * </p>
     *
     * @param event the event to filter or modify
     * @return true if the event should be passed on to other listeners, false if it should be cancelled
     */
    protected abstract boolean filterEvent(T event);
}
