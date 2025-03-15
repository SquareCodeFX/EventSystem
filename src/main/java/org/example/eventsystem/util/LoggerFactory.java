package org.example.eventsystem.util;

import java.util.function.Supplier;

/**
 * A factory for creating loggers that can be used with or without SLF4J being available.
 * <p>
 * This class provides a way to make logging optional in the project. If SLF4J is available
 * on the classpath, it will use SLF4J for logging. If SLF4J is not available, it will use
 * a no-op logger implementation that does nothing.
 * </p>
 */
public class LoggerFactory {

    private static final boolean SLF4J_AVAILABLE = isSLF4JAvailable();

    /**
     * Gets a logger for the specified class.
     *
     * @param clazz the class to get a logger for
     * @return a logger instance
     */
    public static Logger getLogger(Class<?> clazz) {
        if (SLF4J_AVAILABLE) {
            return new SLF4JLogger(org.slf4j.LoggerFactory.getLogger(clazz));
        } else {
            return new NoOpLogger(clazz.getName());
        }
    }

    /**
     * Gets a logger with the specified name.
     *
     * @param name the name of the logger
     * @return a logger instance
     */
    public static Logger getLogger(String name) {
        if (SLF4J_AVAILABLE) {
            return new SLF4JLogger(org.slf4j.LoggerFactory.getLogger(name));
        } else {
            return new NoOpLogger(name);
        }
    }

    /**
     * Checks if SLF4J is available on the classpath.
     *
     * @return true if SLF4J is available, false otherwise
     */
    private static boolean isSLF4JAvailable() {
        try {
            Class.forName("org.slf4j.LoggerFactory");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Interface for loggers that can be used with or without SLF4J.
     */
    public interface Logger {
        /**
         * Logs a message at the TRACE level.
         *
         * @param message the message to log
         */
        void trace(String message);

        /**
         * Logs a message with parameters at the TRACE level.
         *
         * @param message the message to log
         * @param args the parameters to the message
         */
        void trace(String message, Object... args);

        /**
         * Logs a message at the DEBUG level.
         *
         * @param message the message to log
         */
        void debug(String message);

        /**
         * Logs a message with parameters at the DEBUG level.
         *
         * @param message the message to log
         * @param args the parameters to the message
         */
        void debug(String message, Object... args);

        /**
         * Logs a message at the INFO level.
         *
         * @param message the message to log
         */
        void info(String message);

        /**
         * Logs a message with parameters at the INFO level.
         *
         * @param message the message to log
         * @param args the parameters to the message
         */
        void info(String message, Object... args);

        /**
         * Logs a message at the WARN level.
         *
         * @param message the message to log
         */
        void warn(String message);

        /**
         * Logs a message with parameters at the WARN level.
         *
         * @param message the message to log
         * @param args the parameters to the message
         */
        void warn(String message, Object... args);

        /**
         * Logs a message at the ERROR level.
         *
         * @param message the message to log
         */
        void error(String message);

        /**
         * Logs a message with parameters at the ERROR level.
         *
         * @param message the message to log
         * @param args the parameters to the message
         */
        void error(String message, Object... args);

        /**
         * Checks if the TRACE level is enabled.
         *
         * @return true if TRACE is enabled, false otherwise
         */
        boolean isTraceEnabled();

        /**
         * Checks if the DEBUG level is enabled.
         *
         * @return true if DEBUG is enabled, false otherwise
         */
        boolean isDebugEnabled();

        /**
         * Checks if the INFO level is enabled.
         *
         * @return true if INFO is enabled, false otherwise
         */
        boolean isInfoEnabled();

        /**
         * Checks if the WARN level is enabled.
         *
         * @return true if WARN is enabled, false otherwise
         */
        boolean isWarnEnabled();

        /**
         * Checks if the ERROR level is enabled.
         *
         * @return true if ERROR is enabled, false otherwise
         */
        boolean isErrorEnabled();
    }

    /**
     * Implementation of Logger that uses SLF4J.
     */
    private static class SLF4JLogger implements Logger {
        private final org.slf4j.Logger logger;

        public SLF4JLogger(org.slf4j.Logger logger) {
            this.logger = logger;
        }

        @Override
        public void trace(String message) {
            logger.trace(message);
        }

        @Override
        public void trace(String message, Object... args) {
            logger.trace(message, args);
        }

        @Override
        public void debug(String message) {
            logger.debug(message);
        }

        @Override
        public void debug(String message, Object... args) {
            logger.debug(message, args);
        }

        @Override
        public void info(String message) {
            logger.info(message);
        }

        @Override
        public void info(String message, Object... args) {
            logger.info(message, args);
        }

        @Override
        public void warn(String message) {
            logger.warn(message);
        }

        @Override
        public void warn(String message, Object... args) {
            logger.warn(message, args);
        }

        @Override
        public void error(String message) {
            logger.error(message);
        }

        @Override
        public void error(String message, Object... args) {
            logger.error(message, args);
        }

        @Override
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @Override
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @Override
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @Override
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }
    }

    /**
     * Implementation of Logger that does nothing.
     */
    private static class NoOpLogger implements Logger {
        private final String name;

        public NoOpLogger(String name) {
            this.name = name;
        }

        @Override
        public void trace(String message) {
            // No-op
        }

        @Override
        public void trace(String message, Object... args) {
            // No-op
        }

        @Override
        public void debug(String message) {
            // No-op
        }

        @Override
        public void debug(String message, Object... args) {
            // No-op
        }

        @Override
        public void info(String message) {
            // No-op
        }

        @Override
        public void info(String message, Object... args) {
            // No-op
        }

        @Override
        public void warn(String message) {
            // No-op
        }

        @Override
        public void warn(String message, Object... args) {
            // No-op
        }

        @Override
        public void error(String message) {
            // No-op
        }

        @Override
        public void error(String message, Object... args) {
            // No-op
        }

        @Override
        public boolean isTraceEnabled() {
            return false;
        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public boolean isInfoEnabled() {
            return false;
        }

        @Override
        public boolean isWarnEnabled() {
            return false;
        }

        @Override
        public boolean isErrorEnabled() {
            return false;
        }
    }
}
