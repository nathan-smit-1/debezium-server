/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;

import io.debezium.server.SinkErrorStrategy;

/**
 * PubSub-specific implementation of {@link SinkErrorStrategy} that identifies
 * transient gRPC/PubSub errors which can be resolved by restarting the
 * connector.
 * <p>
 * The following gRPC status codes are considered restartable:
 * <ul>
 * <li>{@code UNAVAILABLE} (HTTP 503) — transient network connectivity
 * issues</li>
 * <li>{@code DEADLINE_EXCEEDED} (HTTP 504) — timeout due to network
 * problems</li>
 * </ul>
 * <p>
 * The strategy walks the exception cause chain to find {@link ApiException}
 * instances, which are the standard error type thrown by the Google Cloud
 * client
 * libraries.
 *
 * @author Nathan Smit
 */
@Named("pubsub")
@Dependent
public class PubSubSinkErrorStrategy implements SinkErrorStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubSinkErrorStrategy.class);

    private static final String PROP_RETRYABLE_STATUS_CODES = "debezium.sink.pubsub.retryable.status.codes";
    private static final String DEFAULT_RETRYABLE_STATUS_CODES = "UNAVAILABLE,DEADLINE_EXCEEDED";

    // The set of status codes that are fundamentally "safe" to retry.
    // Codes like PERMISSION_DENIED or UNAUTHENTICATED represent misconfigurations
    // that a restart will not fix.
    private static final Set<StatusCode.Code> SAFE_RETRYABLE_CODES = Set.of(
            StatusCode.Code.UNAVAILABLE,
            StatusCode.Code.DEADLINE_EXCEEDED,
            StatusCode.Code.RESOURCE_EXHAUSTED,
            StatusCode.Code.INTERNAL,
            StatusCode.Code.CANCELLED,
            StatusCode.Code.ABORTED);

    private Set<StatusCode.Code> configuredRetryableCodes = Collections.emptySet();

    @PostConstruct
    public void init() {
        Config config = ConfigProvider.getConfig();
        String codesString = config.getOptionalValue(PROP_RETRYABLE_STATUS_CODES, String.class)
                .orElse(DEFAULT_RETRYABLE_STATUS_CODES);

        Set<StatusCode.Code> parsedCodes = new HashSet<>();
        for (String codeStr : codesString.split(",")) {
            codeStr = codeStr.trim();
            if (codeStr.isEmpty()) {
                continue;
            }
            try {
                StatusCode.Code code = StatusCode.Code.valueOf(codeStr.toUpperCase());
                if (SAFE_RETRYABLE_CODES.contains(code)) {
                    parsedCodes.add(code);
                }
                else {
                    LOGGER.warn(
                            "Status code '{}' is not considered safe for automatic restart and will be ignored. Safe codes are: {}",
                            codeStr, SAFE_RETRYABLE_CODES);
                }
            }
            catch (IllegalArgumentException e) {
                LOGGER.warn("Unknown status code '{}' provided in '{}'", codeStr, PROP_RETRYABLE_STATUS_CODES);
            }
        }

        if (parsedCodes.isEmpty()) {
            LOGGER.warn(
                    "No valid safe restartable status codes configured. Connector restarts for PubSub are effectively disabled.");
        }
        else {
            LOGGER.info("Configured restartable PubSub status codes: {}", parsedCodes);
        }

        this.configuredRetryableCodes = Collections.unmodifiableSet(parsedCodes);
    }

    @Override
    public boolean shouldRestart(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof ApiException apiException) {
                StatusCode.Code code = apiException.getStatusCode().getCode();
                if (configuredRetryableCodes.contains(code)) {
                    LOGGER.info("Identified restartable PubSub error with status code: {}", code);
                    return true;
                }
            }
            current = current.getCause();
        }

        LOGGER.debug("Error is not a restartable PubSub error: {}", error.getMessage());
        return false;
    }
}
