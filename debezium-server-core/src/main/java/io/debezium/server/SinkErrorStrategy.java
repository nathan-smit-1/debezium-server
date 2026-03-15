/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

/**
 * Strategy interface for sink implementations to classify errors as
 * restartable.
 * <p>
 * Sink modules should provide a CDI bean implementing this interface if they
 * want to participate in the connector self-healing mechanism. When the engine
 * fails, the {@link ConnectorRestartManager} consults this strategy to decide
 * whether an automatic restart should be attempted.
 * </p>
 * <p>
 * If no {@code SinkErrorStrategy} bean is available for the configured sink,
 * all errors are treated as non-restartable (existing behavior is preserved).
 * </p>
 *
 * @author Nathan Smit
 */
public interface SinkErrorStrategy {

    /**
     * Determine whether the given error should trigger an engine restart.
     * <p>
     * This method is called only after the engine has fully stopped due to a
     * failure,
     * and only when restart is enabled via configuration.
     * </p>
     *
     * @param error the throwable that caused the engine to stop
     * @return {@code true} if the error is considered transient and a restart
     *         should be attempted; {@code false} otherwise
     */
    boolean shouldRestart(Throwable error);
}
