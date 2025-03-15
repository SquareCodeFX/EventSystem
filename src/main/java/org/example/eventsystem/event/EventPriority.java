package org.example.eventsystem.event;

/**
 * Defines the priority levels for event listeners.
 * <p>
 * Event listeners with higher priority will receive events before listeners with lower priority.
 * </p>
 */
public enum EventPriority {
    /**
     * Highest priority. Listeners with this priority will be called first.
     * Use this for listeners that need to monitor or modify events before any processing occurs.
     */
    HIGHEST(100),

    /**
     * High priority. Listeners with this priority will be called early in the event processing chain.
     */
    HIGH(75),

    /**
     * Normal priority. This is the default priority for listeners.
     */
    NORMAL(50),

    /**
     * Low priority. Listeners with this priority will be called late in the event processing chain.
     */
    LOW(25),

    /**
     * Lowest priority. Listeners with this priority will be called last.
     * Use this for listeners that should only be called after all other processing has occurred.
     */
    LOWEST(0);

    private final int value;

    EventPriority(int value) {
        this.value = value;
    }

    /**
     * Gets the numeric value of this priority.
     * Higher values indicate higher priority.
     *
     * @return the numeric priority value
     */
    public int getValue() {
        return value;
    }
}
