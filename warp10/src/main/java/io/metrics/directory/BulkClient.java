package io.metrics.directory;


import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Explorer native client
 */
public class BulkClient {

    /**
     * Elastic bulk processor which act as a requests buffer
     */
    BulkProcessor bulkProcessor;

    /**
     * Elastic bulk flush time
     */
    Long bulkTime;

    /**
     * Current bulk processing requests state
     */
    Map<String, DocWriteRequest> processing = new HashMap<String, DocWriteRequest>();    

    private static final Logger LOG = LoggerFactory.getLogger(BulkClient.class);

    /**
     * Build a bulk processor for current client
     * @param bulkActions max number of requests in buffer
     * @param bulkConcurrentRequests max concurrent requests for a buffer
     * @param bulkTime buffer flush time
     * @param bulkInitialTime for retry strategy
     * @param bulkRetries Number of possible retries for one request
     * @return Build builk processor
     */
    public BulkClient(ElasticClient client,int bulkActions, int bulkConcurrentRequests, long bulkTime, long bulkInitialTime, int bulkRetries, BulkProcessor.Listener bulkListener) {
        // Construct a Bulk builder based on the user based parameter
        BulkProcessor.Builder builder = BulkProcessor.builder(client.getClient()::bulkAsync, bulkListener);
        builder.setBulkActions(bulkActions);  
        builder.setConcurrentRequests(bulkConcurrentRequests); 
        builder.setFlushInterval(TimeValue.timeValueSeconds(bulkTime)); 

        TimeValue initialDelay = new TimeValue(bulkInitialTime);
        builder.setBackoffPolicy(BackoffPolicy.exponentialBackoff(initialDelay , bulkRetries));
        // Initialize the requests Elastic Bulk processor

        this.bulkProcessor = builder.build(); 
    }

    /**
     * Write an Elastic request in a bulk processor
     * @param request DocumentWriteRequest
     */ 
    public void addBulk(DocWriteRequest request) {
        processing.put(request.id(), request);
        bulkProcessor.add(request);
    }

    /**
     * Remove current string from processing
     * @param id
     */
    public void removeProcessing(String id) {
        processing.remove(id);
    }

    /**
     * Processing map getter
     * @return
     */
    public Map<String, DocWriteRequest> getProcessing() {
        return processing;
    }

}
