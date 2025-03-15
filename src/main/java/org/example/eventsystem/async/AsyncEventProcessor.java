package org.example.eventsystem.async;

import org.example.eventsystem.bus.EventBus;
import org.example.eventsystem.event.Event;
import org.example.eventsystem.util.LoggerFactory;
import org.example.eventsystem.util.LoggerFactory.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Provides advanced asynchronous event processing capabilities.
 * <p>
 * This class supports event queuing, batching, and scheduled event publishing.
 * It can be used to optimize event processing in high-throughput scenarios.
 * </p>
 */
public class AsyncEventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AsyncEventProcessor.class);

    private final EventBus eventBus;
    private final BlockingQueue<Event> eventQueue;
    private final ExecutorService processorExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final AtomicBoolean running;
    private final int batchSize;
    private final long batchTimeoutMs;

    /**
     * Creates a new async event processor with the specified parameters.
     *
     * @param eventBus the event bus to publish events to
     * @param queueCapacity the capacity of the event queue
     * @param processorThreads the number of processor threads
     * @param batchSize the maximum number of events to process in a batch
     * @param batchTimeoutMs the maximum time to wait for a batch to fill up
     */
    public AsyncEventProcessor(EventBus eventBus, int queueCapacity, int processorThreads, 
                              int batchSize, long batchTimeoutMs) {
        this.eventBus = eventBus;
        this.eventQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.processorExecutor = Executors.newFixedThreadPool(processorThreads, r -> {
            Thread thread = new Thread(r, "AsyncEventProcessor-Worker");
            thread.setDaemon(true);
            return thread;
        });
        this.scheduledExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread thread = new Thread(r, "AsyncEventProcessor-Scheduler");
            thread.setDaemon(true);
            return thread;
        });
        this.running = new AtomicBoolean(false);
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;

        logger.info("AsyncEventProcessor initialized with queue capacity: {}, processor threads: {}, " +
                   "batch size: {}, batch timeout: {} ms", queueCapacity, processorThreads, batchSize, batchTimeoutMs);
    }

    /**
     * Creates a new async event processor with default parameters.
     *
     * @param eventBus the event bus to publish events to
     */
    public AsyncEventProcessor(EventBus eventBus) {
        this(eventBus, 10000, Runtime.getRuntime().availableProcessors(), 100, 100);
    }

    /**
     * Starts the event processor.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting AsyncEventProcessor");

            // Start processor threads
            int processorThreads = Runtime.getRuntime().availableProcessors();
            for (int i = 0; i < processorThreads; i++) {
                processorExecutor.submit(this::processEvents);
            }

            // Start batch processor
            scheduledExecutor.scheduleAtFixedRate(this::processBatch, 
                    batchTimeoutMs, batchTimeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stops the event processor.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping AsyncEventProcessor");
            processorExecutor.shutdown();
            scheduledExecutor.shutdown();

            try {
                // Process remaining events
                processBatch();

                // Wait for shutdown
                if (!processorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    processorExecutor.shutdownNow();
                }
                if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduledExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while shutting down AsyncEventProcessor", e);
            }
        }
    }

    /**
     * Queues an event for asynchronous processing.
     *
     * @param event the event to queue
     * @param <T> the type of the event
     * @return true if the event was queued, false if the queue is full
     */
    public <T extends Event> boolean queueEvent(T event) {
        if (!running.get()) {
            throw new IllegalStateException("AsyncEventProcessor is not running");
        }

        try {
            boolean added = eventQueue.offer(event);
            if (!added) {
                logger.warn("Failed to queue event: queue is full");
            }
            return added;
        } catch (Exception e) {
            logger.error("Error queuing event", e);
            return false;
        }
    }

    /**
     * Queues an event for asynchronous processing and returns a future that will be completed
     * when the event is processed.
     *
     * @param event the event to queue
     * @param <T> the type of the event
     * @return a CompletableFuture that will be completed when the event is processed
     */
    public <T extends Event> CompletableFuture<T> queueEventWithFuture(T event) {
        if (!running.get()) {
            throw new IllegalStateException("AsyncEventProcessor is not running");
        }

        CompletableFuture<T> future = new CompletableFuture<>();

        try {
            boolean added = eventQueue.offer(event);
            if (added) {
                // The future will be completed when the event is processed
                return eventBus.publishAsync(event);
            } else {
                future.completeExceptionally(new IllegalStateException("Failed to queue event: queue is full"));
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Schedules an event to be published after a delay.
     *
     * @param event the event to publish
     * @param delay the delay before publishing
     * @param unit the time unit of the delay
     * @param <T> the type of the event
     * @return a CompletableFuture that will be completed when the event is processed
     */
    public <T extends Event> CompletableFuture<T> scheduleEvent(T event, long delay, TimeUnit unit) {
        if (!running.get()) {
            throw new IllegalStateException("AsyncEventProcessor is not running");
        }

        CompletableFuture<T> future = new CompletableFuture<>();

        scheduledExecutor.schedule(() -> {
            try {
                T result = eventBus.publish(event);
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }, delay, unit);

        return future;
    }

    /**
     * Schedules an event to be published periodically.
     *
     * @param eventSupplier a supplier that creates the events to publish
     * @param initialDelay the initial delay before the first publication
     * @param period the period between publications
     * @param unit the time unit of the delay and period
     * @param <T> the type of the event
     * @return a Runnable that can be used to cancel the scheduled task
     */
    public <T extends Event> Runnable scheduleRepeatingEvent(
            java.util.function.Supplier<T> eventSupplier, long initialDelay, long period, TimeUnit unit) {
        if (!running.get()) {
            throw new IllegalStateException("AsyncEventProcessor is not running");
        }

        Runnable task = new Runnable() {
            private final AtomicBoolean cancelled = new AtomicBoolean(false);

            @Override
            public void run() {
                if (cancelled.get() || !running.get()) {
                    return;
                }

                try {
                    T event = eventSupplier.get();
                    if (event != null) {
                        eventBus.publish(event);
                    }
                } catch (Exception e) {
                    logger.error("Error in repeating event task", e);
                }
            }

            public void cancel() {
                cancelled.set(true);
            }
        };

        scheduledExecutor.scheduleAtFixedRate(task, initialDelay, period, unit);

        return task;
    }

    /**
     * Gets the number of events currently in the queue.
     *
     * @return the queue size
     */
    public int getQueueSize() {
        return eventQueue.size();
    }

    /**
     * Checks if the event processor is running.
     *
     * @return true if the processor is running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Processes events from the queue continuously.
     */
    private void processEvents() {
        while (running.get() || !eventQueue.isEmpty()) {
            try {
                Event event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    eventBus.publish(event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Event processor thread interrupted", e);
                break;
            } catch (Exception e) {
                logger.error("Error processing event", e);
            }
        }
    }

    /**
     * Processes a batch of events from the queue.
     */
    private void processBatch() {
        if (eventQueue.isEmpty()) {
            return;
        }

        List<Event> batch = new ArrayList<>(batchSize);
        eventQueue.drainTo(batch, batchSize);

        if (!batch.isEmpty()) {
            logger.debug("Processing batch of {} events", batch.size());

            for (Event event : batch) {
                try {
                    eventBus.publish(event);
                } catch (Exception e) {
                    logger.error("Error processing event in batch", e);
                }
            }
        }
    }
}
