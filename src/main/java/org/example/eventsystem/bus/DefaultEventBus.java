package org.example.eventsystem.bus;

import org.example.eventsystem.event.Event;
import org.example.eventsystem.listener.EventListener;
import org.example.eventsystem.util.LoggerFactory;
import org.example.eventsystem.util.LoggerFactory.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Default implementation of the {@link EventBus} interface.
 * <p>
 * This implementation uses a thread pool for asynchronous event processing
 * and provides thread-safe event dispatching.
 * </p>
 */
public class DefaultEventBus implements EventBus {

    private static final Logger logger = LoggerFactory.getLogger(DefaultEventBus.class);

    private final Map<Class<? extends Event>, Set<EventListener<?>>> listenersByType;
    private final Map<Class<?>, List<EventListener<?>>> sortedListenerCache;
    private final Map<Class<?>, Set<Class<?>>> supertypeCache;
    private final ExecutorService executorService;
    private final AtomicBoolean shutdown;

    /**
     * Creates a new event bus with a fixed thread pool.
     *
     * @param threadPoolSize the number of threads in the pool
     */
    public DefaultEventBus(int threadPoolSize) {
        this.listenersByType = new ConcurrentHashMap<>();
        this.sortedListenerCache = new ConcurrentHashMap<>();
        this.supertypeCache = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(threadPoolSize, r -> {
            Thread thread = new Thread(r, "EventBus-Worker");
            thread.setDaemon(true);
            return thread;
        });
        this.shutdown = new AtomicBoolean(false);
        logger.info("Event bus initialized with {} threads", threadPoolSize);
    }

    /**
     * Creates a new event bus with a thread pool sized based on available processors.
     */
    public DefaultEventBus() {
        this(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public <T extends Event> boolean register(EventListener<T> listener) {
        checkShutdown();

        Class<T> eventType = listener.getEventType();
        Set<EventListener<?>> listeners = listenersByType.computeIfAbsent(
                eventType, k -> new CopyOnWriteArraySet<>());

        boolean added = listeners.add(listener);
        if (added) {
            logger.debug("Registered listener {} for event type {}", listener, eventType.getName());

            // Invalidate sorted listener cache for this event type
            sortedListenerCache.remove(eventType);
        }
        return added;
    }

    @Override
    public <T extends Event> boolean unregister(EventListener<T> listener) {
        checkShutdown();

        Class<T> eventType = listener.getEventType();
        Set<EventListener<?>> listeners = listenersByType.get(eventType);

        if (listeners == null) {
            return false;
        }

        boolean removed = listeners.remove(listener);
        if (removed) {
            logger.debug("Unregistered listener {} for event type {}", listener, eventType.getName());

            // Invalidate sorted listener cache for this event type
            sortedListenerCache.remove(eventType);

            // Clean up empty sets
            if (listeners.isEmpty()) {
                listenersByType.remove(eventType);
            }
        }
        return removed;
    }

    @Override
    public <T extends Event> T publish(T event) {
        checkShutdown();

        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }

        Class<?> eventType = event.getClass();
        logger.debug("Publishing event {} of type {}", event, eventType.getName());

        // Dispatch to listeners for this exact event type
        dispatchEvent(event, eventType);

        // Dispatch to listeners for superclasses and interfaces
        for (Class<?> superType : getAllSuperTypes(eventType)) {
            if (Event.class.isAssignableFrom(superType)) {
                dispatchEvent(event, superType);
            }
        }

        return event;
    }

    @Override
    public <T extends Event> CompletableFuture<T> publishAsync(T event) {
        checkShutdown();

        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }

        return CompletableFuture.supplyAsync(() -> publish(event), executorService);
    }

    @Override
    public <T extends Event> CompletableFuture<T> publishAsync(T event, Consumer<T> callback) {
        CompletableFuture<T> future = publishAsync(event);
        return future.thenApply(e -> {
            callback.accept(e);
            return e;
        });
    }

    @Override
    public boolean hasListenersFor(Class<? extends Event> eventType) {
        checkShutdown();

        if (listenersByType.containsKey(eventType)) {
            return !listenersByType.get(eventType).isEmpty();
        }

        // Check for listeners registered for superclasses or interfaces
        for (Class<?> superType : getAllSuperTypes(eventType)) {
            if (Event.class.isAssignableFrom(superType) && 
                listenersByType.containsKey(superType) && 
                !listenersByType.get(superType).isEmpty()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int getListenerCount(Class<? extends Event> eventType) {
        checkShutdown();

        int count = 0;

        // Count listeners for this exact event type
        Set<EventListener<?>> listeners = listenersByType.get(eventType);
        if (listeners != null) {
            count += listeners.size();
        }

        // Count listeners for superclasses and interfaces
        for (Class<?> superType : getAllSuperTypes(eventType)) {
            if (Event.class.isAssignableFrom(superType)) {
                listeners = listenersByType.get(superType);
                if (listeners != null) {
                    count += listeners.size();
                }
            }
        }

        return count;
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            logger.info("Shutting down event bus");
            executorService.shutdown();
            listenersByType.clear();
            sortedListenerCache.clear();
            supertypeCache.clear();
        }
    }

    /**
     * Dispatches an event to all listeners registered for the specified event type.
     * Uses a cache of sorted listeners to avoid sorting for every dispatch.
     *
     * @param event the event to dispatch
     * @param eventType the type of the event
     * @param <T> the event type
     */
    @SuppressWarnings("unchecked")
    private <T extends Event> void dispatchEvent(T event, Class<?> eventType) {
        Set<EventListener<?>> listeners = listenersByType.get(eventType);

        if (listeners == null || listeners.isEmpty()) {
            return;
        }

        // Get or create sorted listeners list from cache
        List<EventListener<?>> sortedListeners = sortedListenerCache.computeIfAbsent(eventType, k -> {
            // Sort listeners by priority (highest first)
            return listeners.stream()
                    .sorted(Comparator.comparingInt(l -> -l.getPriority().getValue()))
                    .collect(Collectors.toList());
        });

        // Dispatch event to sorted listeners
        for (EventListener<?> listener : sortedListeners) {
            try {
                // Skip if event is cancelled and listener doesn't want cancelled events
                if (event.isCancelled() && !listener.receiveCancelledEvents()) {
                    continue;
                }

                // Cast is safe because we registered the listener with this event type
                ((EventListener<T>) listener).onEvent(event);
            } catch (Exception e) {
                logger.error("Error dispatching event {} to listener {}", event, listener, e);
            }
        }
    }

    /**
     * Gets all superclasses and interfaces of the specified class.
     * Uses a cache to avoid recalculating for the same class.
     *
     * @param clazz the class to get superclasses and interfaces for
     * @return a set of all superclasses and interfaces
     */
    private Set<Class<?>> getAllSuperTypes(Class<?> clazz) {
        // Check cache first
        return supertypeCache.computeIfAbsent(clazz, k -> {
            Set<Class<?>> superTypes = new CopyOnWriteArraySet<>();

            // Add superclasses
            Class<?> superclass = k.getSuperclass();
            while (superclass != null) {
                superTypes.add(superclass);
                superclass = superclass.getSuperclass();
            }

            // Add interfaces
            for (Class<?> iface : k.getInterfaces()) {
                superTypes.add(iface);
                // Use cached results for interfaces if available
                superTypes.addAll(getAllSuperTypes(iface));
            }

            return superTypes;
        });
    }

    /**
     * Checks if this event bus has been shut down.
     *
     * @throws IllegalStateException if the event bus has been shut down
     */
    private void checkShutdown() {
        if (shutdown.get()) {
            throw new IllegalStateException("Event bus has been shut down");
        }
    }
}
