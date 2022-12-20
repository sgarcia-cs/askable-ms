package com.civicscience.markI.util;

import com.google.common.base.Joiner;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.MaxRetriesExceededException;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utils to work with waits. Basically it retries if condition is not met
 */
@Slf4j
public final class ResilientRetryer {


    public static final Integer DEFAULT_RETRIES = 5;

    public static final Integer DEFAULT_INTERVAL_IN_MS = 500;

    private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss:SSSS z");

    /**
     * @param maxAttemptsIncludingFirstCall . The maximum number of attempts (including the initial call as the first attempt)
     * @param mostRandomIntervalInMs        . Interval in ms that is randomly chosen up to from [1..your specified value]
     * @param conditionToRetry              . Condition on which we should retry
     * @param whatToExecute
     * @param loggingTags                   . Tags we will be using for logging in case of retry
     */
    public static <R> R waitUntilConditionWithRandomInterval(int maxAttemptsIncludingFirstCall,
                                                             long mostRandomIntervalInMs,
                                                             Predicate<R> conditionToRetry,
                                                             Callable<R> whatToExecute,
                                                             Map<RetryAction, String> loggingTags) {
        return executeRetryConfig(maxAttemptsIncludingFirstCall,
                conditionToRetry,
                whatToExecute,
                IntervalFunction.ofRandomized(mostRandomIntervalInMs),
                null,
                null,
                loggingTags);
    }

    /**
     * @param maxAttemptsIncludingFirstCall . The maximum number of attempts (including the initial call as the first attempt)
     * @param mostRandomIntervalInMs        . Interval in ms that is randomly chosen up to from [1..your specified value]
     * @param conditionToRetry              . Condition on which we should retry
     * @param whatToExecute
     */
    public static <R> R waitUntilConditionWithRandomInterval(int maxAttemptsIncludingFirstCall,
                                                             long mostRandomIntervalInMs,
                                                             Predicate<R> conditionToRetry,
                                                             Callable<R> whatToExecute) {
        return waitUntilConditionWithRandomInterval(maxAttemptsIncludingFirstCall,
                mostRandomIntervalInMs,
                conditionToRetry,
                whatToExecute,
                new HashMap<>());
    }

    private static <R> R executeRetryConfig(int maxAttemptsIncludingFirstCall,
                                            Predicate<R> conditionToRetry,
                                            Callable<R> whatToExecute,
                                            IntervalFunction intervalFunction,
                                            List<Class<? extends Throwable>> retryOnlyOnExceptions,
                                            List<Class<? extends Throwable>> ignoreExceptions,
                                            Map<RetryAction, String> loggingTags) {
        try {

            RetryConfig.Builder<R> builder = getCustomRetryConfigBuilder(maxAttemptsIncludingFirstCall, intervalFunction);
            if (conditionToRetry != null) {
                builder.retryOnResult(conditionToRetry);
            }
            if (retryOnlyOnExceptions != null) {
                //this is a trick to make sure we retry only on exact exception thrown and not inherited ones
                builder.retryOnException(e -> retryOnlyOnExceptions.stream().anyMatch(clazz -> ExceptionUtils.indexOfThrowable(e, clazz) != -1));
            }
            if (ignoreExceptions != null) {
                builder.ignoreExceptions(ignoreExceptions.toArray(new Class[ignoreExceptions.size()]));
            }
            RetryConfig retryConfig = builder.build();

            // Create a RetryRegistry with a custom global configuration and executes
            return doRetry(maxAttemptsIncludingFirstCall, RetryRegistry.of(retryConfig), whatToExecute, loggingTags);
        } catch (Exception e) {
            String errorMessage;
            if (e instanceof MaxRetriesExceededException) {
                errorMessage = String.format("Max retries exceeded: maxAttempts=[%d]", maxAttemptsIncludingFirstCall);
            } else {
                errorMessage = "Error occurs during retry";
            }
            throw new RuntimeException(errorMessage, e);
        }
    }

    private static <R> RetryConfig.Builder<R> getCustomRetryConfigBuilder(int maxAttemptsIncludingFirstCall, IntervalFunction intervalFunction) {
        return RetryConfig.<R>custom()
                .writableStackTraceEnabled(true)
                .maxAttempts(maxAttemptsIncludingFirstCall)
                .intervalFunction(intervalFunction)
                .failAfterMaxAttempts(true);
    }

    private static <R> R doRetry(int maxAttemptsIncludingFirstCall,
                                 RetryRegistry registry,
                                 Callable<R> whatToExecute,
                                 Map<RetryAction, String> loggingTags) throws Exception {


        Map<String, String> modifiedMap = loggingTags.entrySet().stream().collect(Collectors.toMap(key -> key.getKey().name(), Map.Entry::getValue));

        Retry retryWithCustomConfig = registry.retry("Waiting for specific condition");

        retryWithCustomConfig.getEventPublisher().onRetry(onRetry -> {
            log.info(String.format("Retry attempt doing action [%s]: [%s]," +
                            " maxAttemptsIncludingFirstCall: [%d], creation " +
                            "time: [%s], internal retry event type:[%s]",
                    Joiner.on(",").withKeyValueSeparator(": ").join(modifiedMap),
                    onRetry.getNumberOfRetryAttempts(), maxAttemptsIncludingFirstCall,
                    onRetry.getCreationTime().format(dateTimeFormatter),
                    onRetry.getEventType()));
        }).onError(onError -> {
            log.debug(String.format("Exception during retry. numberOfRetryAttempts=[%d]",
                            onError.getNumberOfRetryAttempts()),
                    onError.getLastThrowable());
        });
        return retryWithCustomConfig.executeCallable(whatToExecute);
    }


    /**
     * @param maxAttemptsIncludingFirstCall . The maximum number of attempts (including the initial call as the first attempt)
     * @param fixedIntervalInMs             . Fixed interval in ms
     * @param conditionToRetry              . Condition on which we should retry
     * @param whatToExecute
     * @param loggingTags                   . Tags we will be using for logging in case of retry
     */
    public static <R> R waitUntilConditionWithFixedInterval(int maxAttemptsIncludingFirstCall,
                                                            long fixedIntervalInMs,
                                                            Predicate<R> conditionToRetry,
                                                            Callable<R> whatToExecute,
                                                            Map<RetryAction, String> loggingTags) {
        return executeRetryConfig(maxAttemptsIncludingFirstCall,
                conditionToRetry,
                whatToExecute,
                IntervalFunction.of(fixedIntervalInMs),
                null,
                null,
                loggingTags);
    }

    /**
     * @param maxAttemptsIncludingFirstCall . The maximum number of attempts (including the initial call as the first attempt)
     * @param fixedIntervalInMs             . Fixed interval in ms
     * @param conditionToRetry              . Condition on which we should retry
     * @param whatToExecute
     */
    public static <R> R waitUntilConditionWithFixedInterval(int maxAttemptsIncludingFirstCall,
                                                            long fixedIntervalInMs,
                                                            Predicate<R> conditionToRetry,
                                                            Callable<R> whatToExecute) {
        return waitUntilConditionWithFixedInterval(maxAttemptsIncludingFirstCall,
                fixedIntervalInMs,
                conditionToRetry,
                whatToExecute,
                new HashMap<>());
    }

    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param conditionToRetry               . Condition on which we should retry
     * @param whatToExecute
     */
    public static <R> R waitUntilConditionWithBackoffRetry(int maxAttemptsIncludingFirstCall,
                                                           long exponentialBackOffIntervalInMs,
                                                           Predicate<R> conditionToRetry,
                                                           Callable<R> whatToExecute) {
        return waitUntilConditionWithBackoffRetry(maxAttemptsIncludingFirstCall,
                exponentialBackOffIntervalInMs,
                conditionToRetry,
                whatToExecute,
                new HashMap<>());
    }

    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param conditionToRetry               . Condition on which we should retry
     * @param whatToExecute
     * @param loggingTags                    . Tags we will be using for logging in case of retry
     */
    public static <R> R waitUntilConditionWithBackoffRetry(int maxAttemptsIncludingFirstCall,
                                                           long exponentialBackOffIntervalInMs,
                                                           Predicate<R> conditionToRetry,
                                                           Callable<R> whatToExecute,
                                                           Map<RetryAction, String> loggingTags) {
        return executeRetryConfig(maxAttemptsIncludingFirstCall,
                conditionToRetry,
                whatToExecute,
                IntervalFunction.ofExponentialBackoff(exponentialBackOffIntervalInMs),
                null,
                null,
                loggingTags);
    }

    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param conditionToRetry               . Condition on which we should retry
     * @param whatToExecute
     * @param ignoreExceptions               .List of exceptions you want to ignore during the retry
     */
    public static <R> R waitUntilConditionWithBackoffRetry(int maxAttemptsIncludingFirstCall,
                                                           long exponentialBackOffIntervalInMs,
                                                           Predicate<R> conditionToRetry,
                                                           Callable<R> whatToExecute,
                                                           List<Class<? extends Throwable>> ignoreExceptions) {
        return waitUntilConditionWithBackoffRetry(maxAttemptsIncludingFirstCall,
                exponentialBackOffIntervalInMs,
                conditionToRetry,
                whatToExecute,
                ignoreExceptions,
                new HashMap<>());
    }

    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param conditionToRetry               . Condition on which we should retry
     * @param whatToExecute
     * @param ignoreExceptions               .List of exceptions you want to ignore during the retry
     * @param loggingTags                    . Tags we will be using for logging in case of retry
     */
    public static <R> R waitUntilConditionWithBackoffRetry(int maxAttemptsIncludingFirstCall,
                                                           long exponentialBackOffIntervalInMs,
                                                           Predicate<R> conditionToRetry,
                                                           Callable<R> whatToExecute,
                                                           List<Class<? extends Throwable>> ignoreExceptions,
                                                           Map<RetryAction, String> loggingTags) {
        return executeRetryConfig(maxAttemptsIncludingFirstCall,
                conditionToRetry,
                whatToExecute,
                IntervalFunction.ofExponentialBackoff(exponentialBackOffIntervalInMs),
                null,
                ignoreExceptions,
                loggingTags);
    }


    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param retryOnlyOnExceptions          . list of exception classes to retry included inherited ones
     * @param whatToExecute
     */
    public static <R> R retryOnExceptionsWithBackoff(int maxAttemptsIncludingFirstCall,
                                                     long exponentialBackOffIntervalInMs,
                                                     Callable<R> whatToExecute,
                                                     List<Class<? extends Throwable>> retryOnlyOnExceptions) {
        return retryOnExceptionsWithBackoff(maxAttemptsIncludingFirstCall,
                exponentialBackOffIntervalInMs,
                whatToExecute,
                retryOnlyOnExceptions,
                new HashMap<>());
    }

    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param retryOnlyOnExceptions          . list of exception classes to retry included inherited ones
     * @param whatToExecute
     * @param loggingTags                    . Tags we will be using for logging in case of retry
     */
    public static <R> R retryOnExceptionsWithBackoff(int maxAttemptsIncludingFirstCall,
                                                     long exponentialBackOffIntervalInMs,
                                                     Callable<R> whatToExecute,
                                                     List<Class<? extends Throwable>> retryOnlyOnExceptions,
                                                     Map<RetryAction, String> loggingTags) {
        return executeRetryConfig(maxAttemptsIncludingFirstCall,
                null,
                whatToExecute,
                IntervalFunction.ofExponentialBackoff(exponentialBackOffIntervalInMs),
                retryOnlyOnExceptions,
                null,
                loggingTags);
    }


    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param retryOnlyOnExceptions          . list of exception classes to retry included inherited ones
     * @param whatToExecute
     * @param conditionToRetry               .Condition on which we should retry
     */
    public static <R> R waitAndRetryOnExceptionsWithBackoff(int maxAttemptsIncludingFirstCall,
                                                            long exponentialBackOffIntervalInMs,
                                                            Predicate<R> conditionToRetry,
                                                            Callable<R> whatToExecute,
                                                            List<Class<? extends Throwable>> retryOnlyOnExceptions) {
        return waitAndRetryOnExceptionsWithBackoff(maxAttemptsIncludingFirstCall,
                exponentialBackOffIntervalInMs,
                conditionToRetry,
                whatToExecute,
                retryOnlyOnExceptions,
                new HashMap<>());
    }

    /**
     * @param maxAttemptsIncludingFirstCall  . The maximum number of attempts (including the initial call as the first attempt)
     * @param exponentialBackOffIntervalInMs . Fixed interval in ms
     * @param retryOnlyOnExceptions          . list of exception classes to retry included inherited ones
     * @param whatToExecute
     * @param conditionToRetry               .Condition on which we should retry
     * @param loggingTags                    . Tags we will be using for logging in case of retry
     */
    public static <R> R waitAndRetryOnExceptionsWithBackoff(int maxAttemptsIncludingFirstCall,
                                                            long exponentialBackOffIntervalInMs,
                                                            Predicate<R> conditionToRetry,
                                                            Callable<R> whatToExecute,
                                                            List<Class<? extends Throwable>> retryOnlyOnExceptions,
                                                            Map<RetryAction, String> loggingTags) {
        return executeRetryConfig(maxAttemptsIncludingFirstCall,
                conditionToRetry,
                whatToExecute,
                IntervalFunction.ofExponentialBackoff(exponentialBackOffIntervalInMs),
                retryOnlyOnExceptions,
                null,
                loggingTags);
    }

    public enum RetryAction {
        RETRY_ACTION;
    }
}
