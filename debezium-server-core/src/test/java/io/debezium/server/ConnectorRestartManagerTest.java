/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.engine.DebeziumEngine;

/**
 * Unit tests for {@link ConnectorRestartManager}.
 * These are plain JUnit tests — no Quarkus context needed.
 */
public class ConnectorRestartManagerTest {

    /**
     * A mock engine that can be configured to fail a set number of times
     * before succeeding.
     */
    private static class MockEngine implements DebeziumEngine<Object> {
        private final AtomicInteger failuresRemaining;
        private final RuntimeException failureException;

        MockEngine(int failCount, RuntimeException exception) {
            this.failuresRemaining = new AtomicInteger(failCount);
            this.failureException = exception;
        }

        @Override
        public void run() {
            if (failuresRemaining.getAndDecrement() > 0) {
                throw failureException;
            }
            // success — just return
        }

        @Override
        public void close() {
        }

        @Override
        public DebeziumEngine.Signaler getSignaler() {
            return null;
        }
    }

    /**
     * A simple error strategy that treats all errors as restartable.
     */
    private static final SinkErrorStrategy ALWAYS_RESTART = error -> true;

    /**
     * A simple error strategy that never restarts.
     */
    private static final SinkErrorStrategy NEVER_RESTART = error -> false;

    @Test
    public void testNoRestartWhenDisabled() {
        // maxAttempts = 0 means restart is disabled
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        AtomicBoolean errorExitCalled = new AtomicBoolean(false);

        ConnectorRestartManager manager = new ConnectorRestartManager(
                () -> new MockEngine(1, new RuntimeException("test failure")),
                Optional.of(ALWAYS_RESTART),
                0, // disabled
                10L, 1000L, 2.0,
                () -> exitCalled.set(true),
                () -> errorExitCalled.set(true));

        manager.run();

        assertThat(exitCalled.get()).isFalse();
        assertThat(errorExitCalled.get()).isTrue();
        assertThat(manager.getAttemptCount()).isEqualTo(0);
    }

    @Test
    public void testNoRestartWhenNoStrategy() {
        // No error strategy means restart is disabled
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        AtomicBoolean errorExitCalled = new AtomicBoolean(false);

        ConnectorRestartManager manager = new ConnectorRestartManager(
                () -> new MockEngine(1, new RuntimeException("test failure")),
                Optional.empty(),
                5,
                10L, 1000L, 2.0,
                () -> exitCalled.set(true),
                () -> errorExitCalled.set(true));

        manager.run();

        assertThat(exitCalled.get()).isFalse();
        assertThat(errorExitCalled.get()).isTrue();
    }

    @Test
    public void testSuccessfulRestartAfterOneFailure() {
        // Engine fails once, then succeeds
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        AtomicBoolean errorExitCalled = new AtomicBoolean(false);
        AtomicInteger engineBuildCount = new AtomicInteger(0);

        ConnectorRestartManager manager = new ConnectorRestartManager(
                () -> {
                    engineBuildCount.incrementAndGet();
                    return new MockEngine(
                            engineBuildCount.get() == 1 ? 1 : 0,
                            new RuntimeException("transient failure"));
                },
                Optional.of(ALWAYS_RESTART),
                3,
                10L, 1000L, 2.0,
                () -> exitCalled.set(true),
                () -> errorExitCalled.set(true));

        manager.run();

        assertThat(exitCalled.get()).isTrue();
        assertThat(errorExitCalled.get()).isFalse();
        assertThat(engineBuildCount.get()).isEqualTo(2);
        assertThat(manager.getAttemptCount()).isEqualTo(1);
    }

    @Test
    public void testMaxAttemptsExhausted() {
        // Engine always fails — should stop after maxAttempts
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        AtomicBoolean errorExitCalled = new AtomicBoolean(false);
        AtomicInteger engineBuildCount = new AtomicInteger(0);

        ConnectorRestartManager manager = new ConnectorRestartManager(
                () -> {
                    engineBuildCount.incrementAndGet();
                    return new MockEngine(1, new RuntimeException("persistent failure"));
                },
                Optional.of(ALWAYS_RESTART),
                3, // max 3 restart attempts
                10L, 1000L, 2.0,
                () -> exitCalled.set(true),
                () -> errorExitCalled.set(true));

        manager.run();

        assertThat(exitCalled.get()).isFalse();
        assertThat(errorExitCalled.get()).isTrue();
        // 1 initial run + 3 restart attempts = 4 total builds, but stops at attempt 3
        // Actually: initial run (attempt becomes 1) -> restart (attempt becomes 2) ->
        // restart (attempt becomes 3, which == maxAttempts) -> exit
        assertThat(manager.getAttemptCount()).isEqualTo(3);
    }

    @Test
    public void testNonRestartableError() {
        // Strategy says don't restart
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        AtomicBoolean errorExitCalled = new AtomicBoolean(false);

        ConnectorRestartManager manager = new ConnectorRestartManager(
                () -> new MockEngine(1, new RuntimeException("fatal error")),
                Optional.of(NEVER_RESTART),
                5,
                10L, 1000L, 2.0,
                () -> exitCalled.set(true),
                () -> errorExitCalled.set(true));

        manager.run();

        assertThat(exitCalled.get()).isFalse();
        assertThat(errorExitCalled.get()).isTrue();
        assertThat(manager.getAttemptCount()).isEqualTo(0);
    }

    @Test
    public void testSuccessfulRunWithoutFailure() {
        // Engine succeeds on first run — no restart needed
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        AtomicBoolean errorExitCalled = new AtomicBoolean(false);

        ConnectorRestartManager manager = new ConnectorRestartManager(
                () -> new MockEngine(0, null),
                Optional.of(ALWAYS_RESTART),
                5,
                10L, 1000L, 2.0,
                () -> exitCalled.set(true),
                () -> errorExitCalled.set(true));

        manager.run();

        assertThat(exitCalled.get()).isTrue();
        assertThat(errorExitCalled.get()).isFalse();
        assertThat(manager.getAttemptCount()).isEqualTo(0);
    }

    @Test
    public void testMultipleRestartsBeforeSuccess() {
        // Engine fails 2 times, then succeeds on the 3rd build
        AtomicBoolean exitCalled = new AtomicBoolean(false);
        AtomicBoolean errorExitCalled = new AtomicBoolean(false);
        AtomicInteger engineBuildCount = new AtomicInteger(0);

        ConnectorRestartManager manager = new ConnectorRestartManager(
                () -> {
                    int count = engineBuildCount.incrementAndGet();
                    return new MockEngine(
                            count <= 2 ? 1 : 0,
                            new RuntimeException("transient failure"));
                },
                Optional.of(ALWAYS_RESTART),
                5,
                10L, 100L, 2.0,
                () -> exitCalled.set(true),
                () -> errorExitCalled.set(true));

        manager.run();

        assertThat(exitCalled.get()).isTrue();
        assertThat(errorExitCalled.get()).isFalse();
        assertThat(engineBuildCount.get()).isEqualTo(3);
        assertThat(manager.getAttemptCount()).isEqualTo(2);
    }
}
