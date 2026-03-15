/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.UnavailableException;

import io.debezium.DebeziumException;
import io.grpc.Status;

/**
 * Unit tests for {@link PubSubSinkErrorStrategy}.
 * Plain JUnit tests — no Quarkus context or PubSub access needed.
 */
public class PubSubSinkErrorStrategyTest {

    private PubSubSinkErrorStrategy strategy;

    @BeforeEach
    public void setup() {
        // Clear system property to ensure default behavior for standard tests
        System.clearProperty("debezium.sink.pubsub.retryable.status.codes");
        strategy = new PubSubSinkErrorStrategy();
        strategy.init();
    }

    @AfterEach
    public void teardown() {
        System.clearProperty("debezium.sink.pubsub.retryable.status.codes");
    }

    @Test
    public void testShouldRestartForUnavailableError() {
        UnavailableException unavailable = new UnavailableException(
                new RuntimeException("Connection refused"),
                GrpcStatusCode.of(Status.Code.UNAVAILABLE),
                true);

        assertThat(strategy.shouldRestart(unavailable)).isTrue();
    }

    @Test
    public void testShouldRestartForDeadlineExceededError() {
        DeadlineExceededException deadlineExceeded = new DeadlineExceededException(
                new RuntimeException("Deadline exceeded"),
                GrpcStatusCode.of(Status.Code.DEADLINE_EXCEEDED),
                true);

        assertThat(strategy.shouldRestart(deadlineExceeded)).isTrue();
    }

    @Test
    public void testShouldRestartForWrappedUnavailableError() {
        // PubSub errors are often wrapped in DebeziumException → ExecutionException
        // chain
        UnavailableException unavailable = new UnavailableException(
                new RuntimeException("Connection reset"),
                GrpcStatusCode.of(Status.Code.UNAVAILABLE),
                true);
        DebeziumException wrapped = new DebeziumException(
                new java.util.concurrent.ExecutionException(unavailable));

        assertThat(strategy.shouldRestart(wrapped)).isTrue();
    }

    @Test
    public void testShouldNotRestartForNonGrpcError() {
        NullPointerException npe = new NullPointerException("something is null");

        assertThat(strategy.shouldRestart(npe)).isFalse();
    }

    @Test
    public void testShouldNotRestartForPermissionDenied() {
        PermissionDeniedException permissionDenied = new PermissionDeniedException(
                new RuntimeException("Permission denied"),
                GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                true);

        assertThat(strategy.shouldRestart(permissionDenied)).isFalse();
    }

    @Test
    public void testShouldRestartForWrappedNonRestartableError() {
        PermissionDeniedException permissionDenied = new PermissionDeniedException(
                new RuntimeException("Forbidden"),
                GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                true);
        DebeziumException wrapped = new DebeziumException(permissionDenied);

        assertThat(strategy.shouldRestart(wrapped)).isFalse();
    }

    @Test
    public void testCustomConfiguredRestartableErrors() {
        System.setProperty("debezium.sink.pubsub.retryable.status.codes", "RESOURCE_EXHAUSTED, INTERNAL");
        PubSubSinkErrorStrategy customStrategy = new PubSubSinkErrorStrategy();
        customStrategy.init();

        // Should now restart for RESOURCE_EXHAUSTED
        com.google.api.gax.rpc.ResourceExhaustedException exhausted = new com.google.api.gax.rpc.ResourceExhaustedException(
                new RuntimeException("Quota exceeded"),
                GrpcStatusCode.of(Status.Code.RESOURCE_EXHAUSTED),
                true);
        assertThat(customStrategy.shouldRestart(exhausted)).isTrue();

        // But NOT for UNAVAILABLE since it was overridden
        UnavailableException unavailable = new UnavailableException(
                new RuntimeException("Connection refused"),
                GrpcStatusCode.of(Status.Code.UNAVAILABLE),
                true);
        assertThat(customStrategy.shouldRestart(unavailable)).isFalse();
    }

    @Test
    public void testCustomConfiguredErrorsFiltersUnsafeCodes() {
        System.setProperty("debezium.sink.pubsub.retryable.status.codes",
                "UNAVAILABLE, PERMISSION_DENIED, INVALID_ARGUMENT");
        PubSubSinkErrorStrategy customStrategy = new PubSubSinkErrorStrategy();
        customStrategy.init();

        // UNAVAILABLE is safe, should be included
        UnavailableException unavailable = new UnavailableException(
                new RuntimeException("Connection refused"),
                GrpcStatusCode.of(Status.Code.UNAVAILABLE),
                true);
        assertThat(customStrategy.shouldRestart(unavailable)).isTrue();

        // PERMISSION_DENIED is unsafe, should be filtered out and NOT restart
        PermissionDeniedException permissionDenied = new PermissionDeniedException(
                new RuntimeException("Forbidden"),
                GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),
                true);
        assertThat(customStrategy.shouldRestart(permissionDenied)).isFalse();

        // INVALID_ARGUMENT is unsafe, should be filtered out
        com.google.api.gax.rpc.InvalidArgumentException invalidArg = new com.google.api.gax.rpc.InvalidArgumentException(
                new RuntimeException("Bad request"),
                GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
                true);
        assertThat(customStrategy.shouldRestart(invalidArg)).isFalse();
    }
}
