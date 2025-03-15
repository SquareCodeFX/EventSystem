package org.example.eventsystem.distributed;

import org.example.eventsystem.event.Event;

/**
 * Interface for adapters that connect to message brokers.
 * <p>
 * Implementations of this interface provide the ability to publish events to
 * and consume events from message brokers like Kafka or RabbitMQ.
 * </p>
 */
public interface MessageBrokerAdapter {

    /**
     * Starts the adapter.
     * <p>
     * This method should establish the connection to the message broker
     * and start any background threads needed for event processing.
     * </p>
     */
    void start();

    /**
     * Stops the adapter.
     * <p>
     * This method should close the connection to the message broker
     * and stop any background threads used for event processing.
     * </p>
     */
    void stop();

    /**
     * Publishes an event to the message broker.
     *
     * @param event the event to publish
     * @param <T> the type of the event
     * @throws Exception if an error occurs while publishing the event
     */
    <T extends Event> void publishEvent(T event) throws Exception;

    /**
     * Checks if the adapter is connected to the message broker.
     *
     * @return true if the adapter is connected, false otherwise
     */
    boolean isConnected();

    /**
     * Gets the name of the message broker this adapter connects to.
     *
     * @return the name of the message broker
     */
    String getBrokerName();
}
