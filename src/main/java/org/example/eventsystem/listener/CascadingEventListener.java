package org.example.eventsystem.listener;

import org.example.eventsystem.bus.EventBus;
import org.example.eventsystem.event.Event;
import org.example.eventsystem.event.EventPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * An event listener that can trigger cascading events.
 * <p>
 * This listener processes an event and then publishes one or more new events
 * based on the result of the processing. This enables event cascading, where
 * one event can trigger a chain of subsequent events.
 * </p>
 *
 * @param <T> the type of event this listener handles
 * @param <R> the type of event this listener produces
 */
public abstract class CascadingEventListener<T extends Event, R extends Event> extends AbstractEventListener<T> {

    private static final Logger logger = LoggerFactory.getLogger(CascadingEventListener.class);

    private final EventBus eventBus;
    private final boolean publishAsync;
    private final List<Function<T, Event>> eventTransformers;

    /**
     * Creates a new cascading event listener.
     *
     * @param eventBus the event bus to publish cascading events to
     * @param priority the priority of this listener
     * @param receiveCancelledEvents whether this listener should receive cancelled events
     * @param publishAsync whether to publish cascading events asynchronously
     */
    protected CascadingEventListener(EventBus eventBus, EventPriority priority, 
                                    boolean receiveCancelledEvents, boolean publishAsync) {
        super(priority, receiveCancelledEvents);
        this.eventBus = eventBus;
        this.publishAsync = publishAsync;
        this.eventTransformers = new ArrayList<>();
    }

    /**
     * Creates a new cascading event listener with default settings.
     *
     * @param eventBus the event bus to publish cascading events to
     */
    protected CascadingEventListener(EventBus eventBus) {
        this(eventBus, EventPriority.NORMAL, false, false);
    }

    /**
     * Adds an event transformer that will be used to create cascading events.
     *
     * @param transformer a function that transforms the input event into a new event
     * @return this listener for method chaining
     */
    public CascadingEventListener<T, R> addEventTransformer(Function<T, Event> transformer) {
        eventTransformers.add(transformer);
        return this;
    }

    @Override
    public final void onEvent(T event) {
        try {
            // Process the event
            R result = processEvent(event);

            // Publish the result if it's not null
            if (result != null) {
                publishCascadingEvent(result);
            }

            // Apply transformers to create additional events
            for (Function<T, Event> transformer : eventTransformers) {
                Event cascadingEvent = transformer.apply(event);
                if (cascadingEvent != null) {
                    publishCascadingEvent(cascadingEvent);
                }
            }
        } catch (Exception e) {
            logger.error("Error in cascading event listener", e);
        }
    }

    /**
     * Processes the event and optionally produces a new event.
     *
     * @param event the event to process
     * @return a new event to publish, or null if no event should be published
     */
    protected abstract R processEvent(T event);

    /**
     * Publishes a cascading event.
     *
     * @param event the event to publish
     */
    protected void publishCascadingEvent(Event event) {
        if (event == null) {
            return;
        }

        try {
            if (publishAsync) {
                eventBus.publishAsync(event);
            } else {
                eventBus.publish(event);
            }
        } catch (Exception e) {
            logger.error("Error publishing cascading event", e);
        }
    }

    /**
     * Sets whether to publish cascading events asynchronously.
     *
     * @param publishAsync true to publish asynchronously, false to publish synchronously
     * @return this listener for method chaining
     */
    public CascadingEventListener<T, R> setPublishAsync(boolean publishAsync) {
        CascadingEventListener<T, R> newListener = new CascadingEventListener<T, R>(eventBus, getPriority(), receiveCancelledEvents(), publishAsync) {
            @Override
            protected R processEvent(T event) {
                return CascadingEventListener.this.processEvent(event);
            }
        };

        // Copy event transformers to the new instance to prevent memory leaks
        for (Function<T, Event> transformer : this.eventTransformers) {
            newListener.addEventTransformer(transformer);
        }

        return newListener;
    }
}
