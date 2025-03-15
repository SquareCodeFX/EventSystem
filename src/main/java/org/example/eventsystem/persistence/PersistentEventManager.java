package org.example.eventsystem.persistence;

import org.example.eventsystem.event.Event;
import org.example.eventsystem.util.LoggerFactory;
import org.example.eventsystem.util.LoggerFactory.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Manages the persistence of events.
 * <p>
 * This class provides functionality to store events to disk and reload them later.
 * It supports automatic periodic saving of events and can be configured to save
 * events to different directories based on their type.
 * </p>
 */
public class PersistentEventManager {

    private static final Logger logger = LoggerFactory.getLogger(PersistentEventManager.class);
    private static final String DEFAULT_STORAGE_DIR = "event_storage";
    private static final DateTimeFormatter FILE_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final String baseStorageDir;
    private final ConcurrentMap<Class<? extends Event>, List<Event>> eventStore;
    private final ScheduledExecutorService scheduledExecutor;
    private final AtomicBoolean running;
    private final long autoSaveIntervalSeconds;

    /**
     * Creates a new persistent event manager with the specified parameters.
     *
     * @param baseStorageDir the base directory to store events in
     * @param autoSaveIntervalSeconds the interval in seconds between automatic saves
     */
    public PersistentEventManager(String baseStorageDir, long autoSaveIntervalSeconds) {
        this.baseStorageDir = baseStorageDir;
        this.eventStore = new ConcurrentHashMap<>();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "PersistentEventManager-Scheduler");
            thread.setDaemon(true);
            return thread;
        });
        this.running = new AtomicBoolean(false);
        this.autoSaveIntervalSeconds = autoSaveIntervalSeconds;

        // Create the storage directory if it doesn't exist
        try {
            Files.createDirectories(Paths.get(baseStorageDir));
        } catch (IOException e) {
            logger.error("Failed to create storage directory: {}", baseStorageDir, e);
        }
    }

    /**
     * Creates a new persistent event manager with default parameters.
     */
    public PersistentEventManager() {
        this(DEFAULT_STORAGE_DIR, 60);
    }

    /**
     * Starts the persistent event manager.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting PersistentEventManager with auto-save interval of {} seconds", autoSaveIntervalSeconds);

            // Schedule periodic saving of events
            if (autoSaveIntervalSeconds > 0) {
                scheduledExecutor.scheduleAtFixedRate(this::saveAllEvents, 
                        autoSaveIntervalSeconds, autoSaveIntervalSeconds, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Stops the persistent event manager.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping PersistentEventManager");

            // Save all events before shutting down
            saveAllEvents();

            // Shutdown the scheduler
            scheduledExecutor.shutdown();
            try {
                if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduledExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while shutting down PersistentEventManager", e);
            }
        }
    }

    /**
     * Stores an event for persistence.
     *
     * @param event the event to store
     * @param <T> the type of the event
     */
    public <T extends Event> void storeEvent(T event) {
        if (event == null) {
            return;
        }

        Class<? extends Event> eventType = event.getClass();
        List<Event> events = eventStore.computeIfAbsent(eventType, k -> Collections.synchronizedList(new ArrayList<>()));
        events.add(event);

        logger.debug("Stored event {} for persistence", event);
    }

    /**
     * Saves all events of a specific type to disk.
     *
     * @param eventType the type of events to save
     * @return the number of events saved
     */
    public int saveEvents(Class<? extends Event> eventType) {
        List<Event> events = eventStore.get(eventType);
        if (events == null || events.isEmpty()) {
            return 0;
        }

        String dirPath = getStoragePathForType(eventType);
        String fileName = eventType.getSimpleName() + "_" + 
                Instant.now().toString().replace(':', '-') + ".events";
        Path filePath = Paths.get(dirPath, fileName);

        try {
            // Create the directory if it doesn't exist
            Files.createDirectories(Paths.get(dirPath));

            // Synchronize on the events list to prevent concurrent modification
            synchronized (events) {
                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath.toFile()))) {
                    oos.writeObject(new ArrayList<>(events));
                    logger.info("Saved {} events of type {} to {}", events.size(), eventType.getName(), filePath);
                    return events.size();
                }
            }
        } catch (IOException e) {
            logger.error("Failed to save events of type {}", eventType.getName(), e);
            return 0;
        }
    }

    /**
     * Saves all stored events to disk.
     *
     * @return the total number of events saved
     */
    public int saveAllEvents() {
        int totalSaved = 0;

        for (Class<? extends Event> eventType : eventStore.keySet()) {
            totalSaved += saveEvents(eventType);
        }

        return totalSaved;
    }

    /**
     * Loads events of a specific type from disk.
     *
     * @param eventType the type of events to load
     * @param <T> the type of the events
     * @return a list of loaded events
     */
    @SuppressWarnings("unchecked")
    public <T extends Event> List<T> loadEvents(Class<T> eventType) {
        String dirPath = getStoragePathForType(eventType);
        Path dir = Paths.get(dirPath);

        if (!Files.exists(dir)) {
            logger.info("No storage directory found for event type {}", eventType.getName());
            return Collections.emptyList();
        }

        List<T> loadedEvents = new ArrayList<>();

        try {
            // Find all event files for this type
            List<File> eventFiles = Files.list(dir)
                    .filter(path -> path.toString().endsWith(".events"))
                    .filter(path -> path.getFileName().toString().startsWith(eventType.getSimpleName()))
                    .map(Path::toFile)
                    .collect(Collectors.toList());

            // Load events from each file
            for (File file : eventFiles) {
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                    List<T> events = (List<T>) ois.readObject();
                    loadedEvents.addAll(events);
                    logger.info("Loaded {} events from {}", events.size(), file);
                } catch (ClassNotFoundException | IOException e) {
                    logger.error("Failed to load events from {}", file, e);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to list event files for type {}", eventType.getName(), e);
        }

        return loadedEvents;
    }

    /**
     * Clears all stored events of a specific type.
     *
     * @param eventType the type of events to clear
     * @param <T> the type of the events
     * @return the number of events cleared
     */
    public <T extends Event> int clearEvents(Class<T> eventType) {
        List<Event> events = eventStore.remove(eventType);
        return events != null ? events.size() : 0;
    }

    /**
     * Clears all stored events.
     *
     * @return the total number of events cleared
     */
    public int clearAllEvents() {
        int totalCleared = 0;

        for (List<Event> events : eventStore.values()) {
            totalCleared += events.size();
        }

        eventStore.clear();
        return totalCleared;
    }

    /**
     * Gets the storage path for a specific event type.
     *
     * @param eventType the event type
     * @return the storage path
     */
    private String getStoragePathForType(Class<?> eventType) {
        return Paths.get(baseStorageDir, eventType.getName().replace('.', File.separatorChar)).toString();
    }
}
