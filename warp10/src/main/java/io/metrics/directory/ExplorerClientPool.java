package io.metrics.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
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
    private AtomicBoolean connected = new AtomicBoolean(false);


    /**
     * Is current testing connection on backend client
     */
    private AtomicBoolean connecting = new AtomicBoolean(false);

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

    BulkClient errorBulk;

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
     * Max elastic client timeout, default 60s in ms
     */
    private int ELASTIC_TIMEOUT = 60000;

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
        
        if (null != properties.getProperty("directory.es.maxtimeout")) {
            this.ELASTIC_TIMEOUT = Integer.parseInt(properties.getProperty("directory.es.maxtimeout"));
        }
        client.init(ELASTIC_HOSTS_NAMES, ES_USER, ES_PASSWORD, ELASTIC_TIMEOUT);

        // Open a bulk, only for "error request" that can store 2 times more data in buffer but send data in single request 
        this.errorBulk = new BulkClient(client, this.ELASTIC_BULK_COUNT * 2,0, this.ELASTIC_BULK_TIME, this.ELASTIC_BULK_INITIAL_TIME, this.ELASTIC_BULK_RETRY, bulkListener);

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

        LockSupport.parkNanos(6000000000L);
        this.tryToConnect();

        // Send error requests count to sensision
        Sensision.update("warp.directory.explorer.insert.requests.failed", Sensision.EMPTY_LABELS, 1);

        // In case of requests failure
        if (this.connected.get()) {
            for (DocWriteRequest bulkItem : bulkRequest.requests()) {
                errorBulk.addBulk(bulkItem);
            }
        }
    }

    /** 
     * Check if current client is availble
     * true connected
     * false not connected
     * @return
     */
    public boolean getConnectedStatus() {
        return this.connected.get();
    }

    /** 
     * Check connection to backend
     * @param currentProcessing
     */
    public void tryToConnect() {

        // Thread safe operation, set boolean connecting to true
        if (this.connecting.compareAndSet(false, true)) {
            try {

                // Get current client answer
                MainResponse response = client.getClient().info();

                // Check if server is available
                if (response.isAvailable()) {

                    // If server is now available compare to old value
                    if (this.connected.compareAndSet(false, response.isAvailable())) {

                        // Restart processing at stop state
                        restore(this.processing);
                    } 
                } else {

                    // If server isn't available, set connected to false
                    this.connected.compareAndSet(true, response.isAvailable());
                }

            } catch (IOException e) {
                LOG.warn(e.getMessage());
            } finally {

                // Release boolean connecting to true
                this.connecting.compareAndSet(true, false);
            }
        }
    }

    /** 
     * Restore current processing state in case of error failure
     * @param currentProcessing
     */
    private void restore(ConcurrentMap<String, DocWriteRequest> currentProcessing) {
        for (DocWriteRequest bulkItem : currentProcessing.values()) {
            errorBulk.addBulk(bulkItem);
        }    
    }

    public boolean exist(String id, String indexName) throws IOException {
        GetRequest getRequest = new GetRequest(indexName);

        getRequest.id(id);

        // Do not load current document source
        FetchSourceContext fetchSourceContext = new FetchSourceContext(false);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = this.client.getClient().get(getRequest);
        if (getResponse.isExists()) {
            return true;
        } else {
            return false;
        }

    }


    public boolean loadIds(ConcurrentMap<String, Boolean> ids, String indexName) {

        try {

            // Set scroll request size and keep alive scroll time
            int requestSize = this.ELASTIC_BULK_COUNT;
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

            // Prepare search all
            SearchRequest searchRequest = new SearchRequest(indexName); 
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery()); 

            // Remove source from result and set request option
            searchSourceBuilder.fetchSource(false);
            searchSourceBuilder.size(requestSize); 
            searchRequest.scroll(scroll);
            searchRequest.source(searchSourceBuilder);

            // Execute request
            SearchResponse searchResponse = client.getClient().search(searchRequest);

            // Get Scroller id
            String scrollId = searchResponse.getScrollId(); 

            // Loop over search result         
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) { 

                for (SearchHit searchHit : searchHits) {
                    ids.putIfAbsent(searchHit.getId(), true);
                }

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
                scrollRequest.scroll(scroll);
                searchResponse = client.getClient().searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); 
            clearScrollRequest.addScrollId(scrollId);

            client.getClient().clearScrollAsync(clearScrollRequest,Clearlistener);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.warn("Exeption during restore" + e.getMessage());
        }
        return false;
    }

    ActionListener<ClearScrollResponse> Clearlistener =new ActionListener<ClearScrollResponse>() {
        @Override
        public void onResponse(ClearScrollResponse clearScrollResponse) {
            LOG.debug("Clear success");
        }

        @Override
        public void onFailure(Exception e) {
            LOG.debug("Fail to clear");
        }
    };
}
