package io.metrics.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.main.MainResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

import io.warp10.continuum.store.Constants;
import io.warp10.sensision.Sensision;

/**
 * Class that build an explorer client pool and manage all bulk requests
 */
public class ExplorerClientPool {

    /**
     * Is current backend client available
     */
    protected boolean connected = false;

    private final ElasticClient client = ElasticClient.getInstance();

    /** 
     * Maps of requests currently processed
     */
    private final ConcurrentMap<String, DocWriteRequest> processing = new MapMaker().concurrencyLevel(64).makeMap();

    /**
     * Maps of waiting bulk requests
     */
    private final ConcurrentMap<String, ArrayList<DocWriteRequest>> waiting = new MapMaker().concurrencyLevel(64).makeMap();


    BulkClient bulkClient;

    /**
     * Index of the active pool
     */
    int activeIndex;

    /**
     * Current client activity time
     */
    long lastTicks;

    /**
     * Parameter used in Directory ES
     * Elastic user
     */
    private String ES_USER = "explorer.user";

    /**
     * Parameter used in Directory ES
     * Elastic pwd
     */
    private String ES_PASSWORD = "explorer.password";

    /**
     * Parameter used in Directory ES
     * Hosts of the main ES node
     * Load string configuration. Several hosts can be added using a "," separator
     */
    String ELASTIC_HOSTS_NAMES = "http://localhost:9200";

    /**
     * Bulk number of items before flushing, default 1 000
     */
    private int ELASTIC_BULK_COUNT = 1000;

    /**
     * Bulk amount of time to wait before flushing (seconds), default 10s
     */
    private long ELASTIC_BULK_TIME = 10L;

    /**
     * Bulk number of concurrent requests allowed, default 1
     */
    private int ELASTIC_BULK_CONCURRENT_REQUESTS = 1;

    /**
     * Bulk amount of time to wait before first retry then managed using exponential back-off policy, default 5 ms
     */
    private long ELASTIC_BULK_INITIAL_TIME = 50L;

    /**
     * Bulk number of retry before failing a bulk request, default 5
     */
    private int ELASTIC_BULK_RETRY = 5;

    private static final Logger LOG = LoggerFactory.getLogger(ExplorerClientPool.class);

    /**
     * Initialize pool properties based on the user configuration
     * @param properties Property File (warp-conf)
     */
    public void init(Properties properties) {
        if (null != properties.getProperty("directory.es.user")) {
            this.ES_USER = properties.getProperty("directory.es.user");
        }

        if (null != properties.getProperty("directory.es.password")) {
            this.ES_PASSWORD = properties.getProperty("directory.es.password");
        }

        if (null != properties.getProperty("directory.es.hosts")) {
            this.ELASTIC_HOSTS_NAMES = properties.getProperty("directory.es.hosts");
        }

        if (null != properties.getProperty("directory.es.bulk.count")) {
            this.ELASTIC_BULK_COUNT = Integer.parseInt(properties.getProperty("directory.es.bulk.count"));
        }

        if (null != properties.getProperty("directory.es.bulk.concurrent.requests")) {
            this.ELASTIC_BULK_CONCURRENT_REQUESTS = Integer.parseInt(properties.getProperty("directory.es.bulk.concurrent.requests"));
        }

        if (null != properties.getProperty("directory.es.bulk.time")) {
            this.ELASTIC_BULK_TIME = Long.parseLong(properties.getProperty("directory.es.bulk.time"));
        }

        if (null != properties.getProperty("directory.es.bulk.retry.initial.time")) {
            this.ELASTIC_BULK_INITIAL_TIME = Long.parseLong(properties.getProperty("directory.es.bulk.retry.initial.time"));
        }

        if (null != properties.getProperty("directory.es.bulk.retry.max")) {
            this.ELASTIC_BULK_RETRY = Integer.parseInt(properties.getProperty("directory.es.bulk.retry.max"));
        }
        client.init(ELASTIC_HOSTS_NAMES, ES_USER, ES_PASSWORD);

        this.tryToConnect();
        
        this.bulkClient = new BulkClient(client, this.ELASTIC_BULK_COUNT,this.ELASTIC_BULK_CONCURRENT_REQUESTS, this.ELASTIC_BULK_TIME, this.ELASTIC_BULK_INITIAL_TIME, this.ELASTIC_BULK_RETRY, bulkListener);     
    }

    /**
     * Add an Elastic request into explorer client
     * @param request DocumentWriteRequest
     */
    public void add(DocWriteRequest request) {

        // Manage current bulk state
        if (this.processing.containsKey(request.id())) {
            this.setRequestAsWaiting(request);
        } else {
            this.bulkClient.addBulk(request);
        }
    }

    /**
     * Bulk listener on requests
     */
    BulkProcessor.Listener bulkListener = new BulkProcessor.Listener() { 

        /**
         * Prepare a send request, let empty as now
         */
        public void beforeBulk(long executionId, BulkRequest request) {
            Sensision.update("warp.directory.explorer.insert.requests.total", Sensision.EMPTY_LABELS, 1);
        }

        /**
         * Method called when back-end respond
         */
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

            if (response.hasFailures()) { 
                LOG.warn("Bulk [{}] executed with failures: " + response.buildFailureMessage(), executionId);
                onFailBulkRequest(request);
            } else {
                onSucessBulkResponse(response, request.numberOfActions());
                LOG.debug("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
            }
        }

        /**
         * Method called when no back-end has responded
         */
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            LOG.warn("Bulk listener fail to send or connect to Elastic backend: " + failure.getMessage());
            onFailBulkRequest(request);
        }
    };

    /**
     * Manage correct bulk response, remove each successful Id of the request type list
     * Correct type could be: CREATE (update or HBase entry point), UPDATE (/meta) and DELETE
     * @param bulkResponse
     */
    public void onSucessBulkResponse(BulkResponse bulkResponse, int numberOfActions) {

        // Send success: document Count, requests count and request elapsed time (in platform TU) on sensition
        Sensision.update("warp.directory.explorer.insert.documents.count", Sensision.EMPTY_LABELS, numberOfActions);
        Sensision.update("warp.directory.explorer.insert.requests.success", Sensision.EMPTY_LABELS, 1);
        Sensision.update("warp.directory.explorer.insert.elapsed.time", Sensision.EMPTY_LABELS, bulkResponse.getTook().getMillis()*Constants.TIME_UNITS_PER_MS);

        for (BulkItemResponse bulkItemResponse : bulkResponse) { 
            String id = bulkItemResponse.getResponse().getId(); 

            this.processing.remove(id);
            this.bulkClient.removeProcessing(id);
            if (waiting.containsKey(id)) {
                ArrayList<DocWriteRequest> waitingList = waiting.get(id);
                this.add(waitingList.remove(0));
                if(waitingList.isEmpty()) {
                    this.waiting.remove(id);
                }
            }
        }
    }

    /**
     * Internal function called by addBulk to add the request in waiting list
     * @param request DocumentWriteRequest
     */
    private void setRequestAsWaiting(DocWriteRequest request) {
        ArrayList<DocWriteRequest> waitingList;
        if (waiting.containsKey(request.id())) {
            waitingList = waiting.get(request.id());
        } else {
            waitingList = new ArrayList<DocWriteRequest>();
        }
        waitingList.add(request);

        waiting.put(request.id(), waitingList);
    }

    /**
     * Manage fail bulk response in case of requests
     * @param bulkResponse failed request
     */
    public void onFailBulkRequest(BulkRequest bulkRequest) {
        
        this.tryToConnect();
        
        // Send error requests count to sensision
        Sensision.update("warp.directory.explorer.insert.requests.failed", Sensision.EMPTY_LABELS, 1);

        // In case of requests failure
        if (this.connected) {
            BulkClient errorBulk = new BulkClient(client, this.ELASTIC_BULK_COUNT,1, this.ELASTIC_BULK_TIME, this.ELASTIC_BULK_INITIAL_TIME, this.ELASTIC_BULK_RETRY, bulkListener);
            for (DocWriteRequest bulkItem : bulkRequest.requests()) {
                errorBulk.addBulk(bulkItem);
            }
            
            errorBulk.closeClient();
        }
    }

    /** 
     * Check connection to backend
     * @param currentProcessing
     */
    public void tryToConnect() {
        try {
            Boolean oldConnect = this.connected;
            MainResponse response = client.getClient().info();
            this.connected = response.isAvailable();
            
            // When backend is back start restore current client process
            if (this.connected && !oldConnect) {
                restore(this.processing);
            }
        } catch (IOException e) {
            LOG.warn(e.getMessage());
        }
    }

    /** 
     * Restore current processing state in case of error failure
     * @param currentProcessing
     */
    private void restore(ConcurrentMap<String, DocWriteRequest> currentProcessing) {
        BulkClient errorBulk = new BulkClient(client, this.ELASTIC_BULK_COUNT,1, this.ELASTIC_BULK_TIME, this.ELASTIC_BULK_INITIAL_TIME, this.ELASTIC_BULK_RETRY, bulkListener);
        for (DocWriteRequest bulkItem : currentProcessing.values()) {
            errorBulk.addBulk(bulkItem);
        }  
        errorBulk.closeClient();      
    }
}
