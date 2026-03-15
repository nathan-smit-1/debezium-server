/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Manages the connector engine lifecycle with automatic restart capability.
 * <p>
 * When the engine fails, this manager consults the configured
 * {@link SinkErrorStrategy}
 * to determine whether the error is transient and warrants a restart. If so, it
 * applies
 * exponential backoff and rebuilds/restarts the engine up to
 * {@code maxAttempts} times.
 * </p>
 * <p>
 * If restarts are disabled ({@code maxAttempts <= 0}), no strategy is
 * available,
 * or the error is classified as non-restartable, the manager falls through and
 * the provided {@code exitAction} is invoked (typically
 * {@code Quarkus.asyncExit}).
 * </p>
 *
 * @author Nathan Smit
 */
public class ConnectorRestartManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorRestartManager.class);

    private final Supplier<DebeziumEngine<?>> engineBuilder;
    private final Optional<SinkErrorStrategy> errorStrategy;
    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double backoffMultiplier;
    private final Runnable exitAction;
    private final Runnable exitWithErrorAction;

    private int attemptCount = 0;

    /**
     * Create a new restart manager.
     *
     * @param engineBuilder       supplies a freshly-built engine instance for each
     *                            (re)start
     * @param errorStrategy       optional strategy for classifying errors — if
     *                            empty, no restarts
     * @param maxAttempts         maximum number of restart attempts (0 = disabled)
     * @param initialDelayMs      initial delay before first restart (milliseconds)
     * @param maxDelayMs          maximum delay between restarts (milliseconds)
     * @param backoffMultiplier   multiplier for exponential backoff
     * @param exitAction          action to call for a clean exit (e.g.,
     *                            Quarkus.asyncExit(0))
     * @param exitWithErrorAction action to call for an error exit (e.g.,
     *                            Quarkus.asyncExit(1))
     */
    public ConnectorRestartManager(
                                   Supplier<DebeziumEngine<?>> engineBuilder,
                                   Optional<SinkErrorStrategy> errorStrategy,
                                   int maxAttempts,
                                   long initialDelayMs,
                                   long maxDelayMs,
                                   double backoffMultiplier,
                                   Runnable exitAction,
                                   Runnable exitWithErrorAction) {
        this.engineBuilder = engineBuilder;
        this.errorStrategy = errorStrategy;
        this.maxAttempts = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.backoffMultiplier = backoffMultiplier;
        this.exitAction = exitAction;
        this.exitWithErrorAction = exitWithErrorAction;
    }

    /**
     * Run the engine with restart support. This method blocks until the engine
     * completes successfully or all restart attempts are exhausted.
     */
    public void run() {
        try {
            long currentDelay = initialDelayMs;
            attemptCount = 0;

            while (true) {
                DebeziumEngine<?> engine = engineBuilder.get();
                EngineResult result = runEngine(engine);

                if (result.isSuccess()) {
                    LOGGER.info("Engine completed successfully");
                    exitAction.run();
                    return;
                }

                Throwable error = result.getError();

                if (!isRestartEnabled()) {
                    LOGGER.error("Engine failed and restart is disabled. Exiting.", error);
                    exitWithErrorAction.run();
                    return;
                }

                if (!isRestartable(error)) {
                    LOGGER.error("Engine failed with non-restartable error. Exiting.", error);
                    exitWithErrorAction.run();
                    return;
                }

                attemptCount++;
                if (attemptCount >= maxAttempts) {
                    LOGGER.error("Engine failed and maximum restart attempts ({}) exhausted. Exiting.",
                            maxAttempts, error);
                    exitWithErrorAction.run();
                    return;
                }

                LOGGER.warn("Engine failed with restartable error (attempt {}/{}). Restarting in {}ms...",
                        attemptCount, maxAttempts, currentDelay, error);

                try {
                    Metronome.sleeper(Duration.ofMillis(currentDelay), Clock.SYSTEM).pause();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Restart wait interrupted. Exiting.");
                    exitWithErrorAction.run();
                    return;
                }

                currentDelay = Math.min((long) (currentDelay * backoffMultiplier), maxDelayMs);
            }
        }
        catch (Throwable t) {
            LOGGER.error("Engine executor thread caught unexpected error. Exiting.", t);
            exitWithErrorAction.run();
        }
    }

    private EngineResult runEngine(DebeziumEngine<?> engine) {
        try {
            engine.run();
            return EngineResult.success();
        }
        catch (Throwable t) {
            return EngineResult.failure(t);
        }
    }

    private boolean isRestartEnabled() {
        return maxAttempts > 0 && errorStrategy.isPresent();
    }

    private boolean isRestartable(Throwable error) {
        return errorStrategy.map(strategy -> strategy.shouldRestart(error)).orElse(false);
    }

    /**
     * Returns the current restart attempt count (for testing).
     */
    int getAttemptCount() {
        return attemptCount;
    }

    /**
     * Encapsulates the result of an engine run.
     */
    static class EngineResult {
        private final boolean success;
        private final Throwable error;

        private EngineResult(boolean success, Throwable error) {
            this.success = success;
            this.error = error;
        }

        static EngineResult success() {
            return new EngineResult(true, null);
        }

        static EngineResult failure(Throwable error) {
            return new EngineResult(false, error);
        }

        boolean isSuccess() {
            return success;
        }

        Throwable getError() {
            return error;
        }
    }
}
