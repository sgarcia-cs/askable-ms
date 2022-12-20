package com.civicscience.markI.client;

import com.civicscience.markI.util.ResilientRetryer;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.bulk.*;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.time.temporal.ValueRange;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utils to handle general OpenSearch operations
 */
@Builder(toBuilder = true)
@Slf4j
public class OpenSearchClient {

    public static final int RETRY_ON_CONFLICT_DEFAULT = 10;
    private static RestHighLevelClient client;

    private final static BackoffPolicy EXP_BACKOFF = BackoffPolicy.exponentialBackoff();

    private static final int MIN_BULK_ACTIONS_COUNT = 1;
    private static final int MAX_BULK_ACTIONS_COUNT = 10000;
    private static final TimeValue MIN_BULK_FLUSH_INTERVAL_IN_SEC = TimeValue.timeValueSeconds(1);
    private static final TimeValue MAX_BULK_FLUSH_INTERVAL_IN_SEC = TimeValue.timeValueSeconds(10);
    private static final int MIN_BULK_CONCURRENT_REQUESTS = 0;
    private static final int MAX_BULK_CONCURRENT_REQUESTS = 5;

    //TODO implement factory to instantiate bulk processor
    public static BulkProcessor createCustomBulkProcessor() {
        //TODO: change env vars
        return createBulkProcessor(
                Integer.valueOf(Optional.ofNullable(10)
                        .orElse(MIN_BULK_CONCURRENT_REQUESTS)),
                Integer.valueOf(Optional.ofNullable(10)
                        .orElse(MIN_BULK_ACTIONS_COUNT)),
                Integer.valueOf(Optional.ofNullable(10)
                        .orElse((int) MIN_BULK_FLUSH_INTERVAL_IN_SEC.getSeconds())));
    }

    //TODO implement factory to instantiate bulk processor
    public static BulkProcessor createBulkProcessor(int concurrentRequests,
                                                    int bulkActionsCount,
                                                    int flushIntervalInSec) {

        ValueRange concurrentRequestsRange = ValueRange.of(MIN_BULK_CONCURRENT_REQUESTS, MAX_BULK_CONCURRENT_REQUESTS);
        ValueRange bulkActionsCountRange = ValueRange.of(MIN_BULK_ACTIONS_COUNT, MAX_BULK_ACTIONS_COUNT);
        ValueRange flushIntervalInSecRange = ValueRange.of(MIN_BULK_FLUSH_INTERVAL_IN_SEC.getSeconds(), MAX_BULK_FLUSH_INTERVAL_IN_SEC.getSeconds());

        TimeValue flushIntervalTime;
        if (!flushIntervalInSecRange.isValidIntValue(flushIntervalInSec)) {
            log.warn("[BULK_PROCESSOR] Bulk flush interval is out of this range=[{}]. Setting to min value=[{}]", flushIntervalInSecRange, MIN_BULK_FLUSH_INTERVAL_IN_SEC);
            flushIntervalTime = MIN_BULK_FLUSH_INTERVAL_IN_SEC;
        } else {
            flushIntervalTime = TimeValue.timeValueSeconds(flushIntervalInSec);
        }

        if (!concurrentRequestsRange.isValidIntValue(concurrentRequests)) {
            log.warn("[BULK_PROCESSOR] Bulk concurrent requests are out of this range=[{}]. Setting to min value=[{}]", concurrentRequestsRange, MIN_BULK_CONCURRENT_REQUESTS);
            concurrentRequests = MIN_BULK_CONCURRENT_REQUESTS;
        }
        if (!bulkActionsCountRange.isValidIntValue(bulkActionsCount)) {
            log.warn("[BULK_PROCESSOR] Bulk actions count is out of this range=[{}]. Setting to min value=[{}]", bulkActionsCountRange, MIN_BULK_ACTIONS_COUNT);
            bulkActionsCount = MIN_BULK_ACTIONS_COUNT;
        }


        BulkProcessor processor = BulkProcessor.builder((request, bulkResponseActionListener) -> {
                    client.bulkAsync(request, RequestOptions.DEFAULT, bulkResponseActionListener);
                }, new BulkProcessor.Listener() {

                    AtomicInteger count = new AtomicInteger(0);

                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        log.info("Started bulk operation with number of operations='{}', executionId='{}'", request.numberOfActions(), executionId);
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        if (response.hasFailures()) {
                            log.error("Bulk executed with failures. Number of failures='[{}]', executionId='[{}]', hasFailures='[{}]', failureMessage='[{}]'", countFailuresInBulk(response), executionId, response.hasFailures(), response.buildFailureMessage());

                        } else {
                            log.info("Finished bulk operation  with number of operations='[{}]', executionId='[{}]', totalOperationsSoFar='[{}]'", request.numberOfActions(), executionId, count.addAndGet(request.numberOfActions()));
                        }
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        log.error(String.format("Unexpected error during bulk operations. ExecutionId='[%d]'", executionId), failure);
                    }
                })
                .setBulkActions(bulkActionsCount)
                .setFlushInterval(flushIntervalTime)
                .setConcurrentRequests(concurrentRequests)
                .setBackoffPolicy(EXP_BACKOFF)
                .build();

        log.info("Bulk processor has been successfully created with: bulkActionsCount=[{}], flushInterval=[{}], concurrentRequests=[{}]",
                bulkActionsCount, flushIntervalTime.getSeconds(), concurrentRequests);
        return processor;
    }

    private static int countFailuresInBulk(BulkResponse response) {
        if (!response.hasFailures()) {
            return 0;
        }

        int count = 0;
        for (BulkItemResponse item : response.getItems()) {
            if (item.isFailed()) {
                count++;
            }
        }

        return count;
    }

    public static OpenSearchClientBuilder builder(String hostURL) {

        return new OpenSearchClientBuilder() {
            @Override
            public OpenSearchClient build() {
                if (StringUtils.isBlank(hostURL)) {
                    throw new IllegalArgumentException("Please specify valid hostURL in format: <https|http>://hostName:<port>");
                }
                RestClientBuilder builder = RestClient.builder(HttpHost.create(hostURL)).setCompressionEnabled(true);
                client = new RestHighLevelClient(builder);

                return super.build();
            }
        };
    }

    public void indexData(IndexRequest request) {
        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            RestStatus status = ExceptionsHelper.status(e);
            if (status.getStatus() != 400 && status.getStatus() != 404) {
                log.warn("Trying to recover from exception. Calling one more time and doing retry");
                ResilientRetryer.retryOnExceptionsWithBackoff(ResilientRetryer.DEFAULT_RETRIES,
                        ResilientRetryer.DEFAULT_INTERVAL_IN_MS,
                        () -> client.index(request, RequestOptions.DEFAULT),
                        Arrays.asList(OpenSearchRejectedExecutionException.class, OpenSearchStatusException.class, OpenSearchException.class),
                        ImmutableMap.of(ResilientRetryer.RetryAction.RETRY_ACTION, "indexDataRetry"));
                return;
            }

            if (status.getStatus() == 404) {
                log.warn("Document is missing with id='{}' in index='{}'", request.id(), request.index());
                return;
            }
            String errorMessage = String.format("Can not index data in index=%s, documentId=%s", request.index(), request.id());
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public void bulkIndexData(BulkProcessor bulkProcessor, int timeRemainingInMillis) {
        log.info("Remaining time in sec={}", timeRemainingInMillis / 1000);
        try {
            bulkProcessor.awaitClose(timeRemainingInMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Bulk was interrupted", e);
        }

    }


    public void updateData(UpdateRequest request) {
        try {
            request.retryOnConflict(RETRY_ON_CONFLICT_DEFAULT);
            request.docAsUpsert(true);
            client.update(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            RestStatus status = ExceptionsHelper.status(e);
            if (status.getStatus() != 400 && status.getStatus() != 404) {
                log.warn("Trying to recover from exception. Calling one more time and doing retry");
                ResilientRetryer.retryOnExceptionsWithBackoff(ResilientRetryer.DEFAULT_RETRIES,
                        ResilientRetryer.DEFAULT_INTERVAL_IN_MS,
                        () -> client.update(request, RequestOptions.DEFAULT),
                        Arrays.asList(OpenSearchRejectedExecutionException.class, OpenSearchStatusException.class, OpenSearchException.class),
                        ImmutableMap.of(ResilientRetryer.RetryAction.RETRY_ACTION, "updateDataRetry"));
                return;
            }
            if (status.getStatus() == 404) {
                log.warn("Document is missing with id='{}' in index='{}'", request.id(), request.index());
                return;
            }
            String errorMessage = String.format("Can not update document in index=%s, documentId=%s", request.index(), request.id());
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }


    public void deleteData(DeleteRequest request) {
        try {
            client.delete(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            RestStatus status = ExceptionsHelper.status(e);
            if (status.getStatus() != 400 && status.getStatus() != 404) {
                log.warn("Trying to recover from exception. Calling one more time and doing retry");
                ResilientRetryer.retryOnExceptionsWithBackoff(ResilientRetryer.DEFAULT_RETRIES,
                        ResilientRetryer.DEFAULT_INTERVAL_IN_MS,
                        () -> client.delete(request, RequestOptions.DEFAULT),
                        Arrays.asList(OpenSearchRejectedExecutionException.class, OpenSearchStatusException.class, OpenSearchException.class),
                        ImmutableMap.of(ResilientRetryer.RetryAction.RETRY_ACTION, "deleteDataRetry"));
                return;

            }

            if (status.getStatus() == 404) {
                log.warn("Document is missing with id='{}' in index='{}'", request.id(), request.index());
                return;
            }

            String errorMessage = String.format("Can not delete document in index=%s, documentId=%s", request.index(), request.id());
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }


    public SearchResponse search(SearchSourceBuilder searchBuilder, String indexName) {
        SearchResponse response = null;
        SearchRequest searchRequest = null;
        try {
            searchRequest = new SearchRequest();
            searchRequest.indices(indexName);
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            searchRequest.source(searchBuilder);
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            RestStatus status = ExceptionsHelper.status(e);
            if (status.getStatus() == 404) {
                log.warn("Document is missing '{}' in index='{}'", searchRequest.toString(), indexName);
                return null;
            }
            log.error(String.format("Can not search document=[%s]", searchRequest.toString()), e);
        }
        return response;
    }

    public GetResponse getDocumentById(GetRequest getRequest) {
        GetResponse response = null;
        try {
            response = client.get(getRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error(String.format("Can not search document=[%s]", getRequest.toString()), e);
        }
        return response;
    }

    public boolean doesDocumentExist(GetRequest getRequest) {
        try {
            return client.exists(getRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error(String.format("Can not check if document=[%s] exists", getRequest.toString()), e);
        }
        return false;
    }

    public void shutdown() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.error("Can not close opensearch client");
        }
    }


}
