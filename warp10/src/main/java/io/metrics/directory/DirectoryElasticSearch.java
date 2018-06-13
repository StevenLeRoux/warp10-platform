package io.metrics.directory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.DirectoryPlugin;

/**
 * Plugin which defines functions directory function to write Warp meta-data in Elastic-Search
 */
public class DirectoryElasticSearch extends DirectoryPlugin {

    /**
     * Parameter used in Directory ES
     * Name of the application label to use to type in ES
     * By default use Warp10 Application label constant
     */
    private String ES_APP = Constants.APPLICATION_LABEL;

    /**
     * Parameter used in Directory ES
     * Name of the elastic key for the class name
     * By default use name
     */
    private String ES_CLASS = "name";

    /**
     * Parameter used in Directory ES
     * Name of the elastic key for the Warp 10 application name    
     * By default use app
     */
    private String ES_STORE_APP = "app";

    /**
     * Parameter used in Directory ES
     * Name of the elastic key for the labels
     * By default use labels
     */
    private String ES_LABELS = "labels";
    
    /**
     * Parameter used in Directory ES
     * Name of the elastic key for the attributes
     * By default use attributes
     */
    private String ES_ATTRIBUTES = "attributes";
    
    /**
     * Parameter used in Directory ES
     * Name of the elastic index name
     * By default use explorer
     */
    private String ES_INDEX_NAME = "explorer";
    
    
    /**
     * Parameter used in Directory ES
     * Do we restore based on HBASE value
     */
    private boolean ES_RESTORE = false;
    
    private int missing;
    
    private AtomicBoolean tmp = new AtomicBoolean(false);

    /**
     * Parameter used to indicate whether Elastic directory have to process HBase reload data,
     * default false
     */
    private boolean init;

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryElasticSearch.class);

    /**
     * Keep active client reference
     */
    private ExplorerClientPool client;

    /**
     * Maps of ES key necessary
     */
    private final ConcurrentMap<String, Boolean> ids = new MapMaker().concurrencyLevel(64).makeMap();

    /**
     * Initialize the plugin. This method is called immediately after a plugin has been instantiated.
     *
     * @param properties Properties from the Warp configuration file
     */
    public void init(Properties properties) {

        // Load locally all user properties

        if (null != properties.getProperty("directory.es.app")) {
            this.ES_APP = properties.getProperty("directory.es.app");
        }

        if (null != properties.getProperty("directory.es.classname")) {
            this.ES_CLASS = properties.getProperty("directory.es.classname");
        }

        if (null != properties.getProperty("directory.es.labels")) {
            this.ES_LABELS = properties.getProperty("directory.es.labels");
        }

        if (null != properties.getProperty("directory.es.attributes")) {
            this.ES_ATTRIBUTES = properties.getProperty("directory.es.attributes");
        }

        if (null != properties.getProperty("directory.es.store.app")) {
            this.ES_STORE_APP = properties.getProperty("directory.es.store.app");
        }

        if (null != properties.getProperty("directory.es.index.name")) {
            this.ES_INDEX_NAME = properties.getProperty("directory.es.index.name");
        }

        if (null != properties.getProperty("directory.es.restore")) {
            this.ES_RESTORE = Boolean.parseBoolean(properties.getProperty("directory.es.restore"));
        }

        this.init = "true".equals(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_INIT));

        LOG.debug("Start DirectoryES - init");

        this.client = new ExplorerClientPool();
        this.client.init(properties);
        
        if (this.init && this.ES_RESTORE) {
            loadAllIds();
        }
    }

    private void loadAllIds() {
        long now = System.currentTimeMillis();
        this.client.loadIds(this.ids, this.ES_INDEX_NAME);
        Long duration = System.currentTimeMillis() - now;
        LOG.info("Load: " + this.ids.size() + " gts in " + duration + "ms");
    }

    /**
     * Stores a GTS.
     *
     * GTS to store might already exist in the storage layer. It may be pushed because the attributes have changed.
     *
     * @param source Indicates the source of the data to be stored. Will be null when initializing Directory.
     * @param gts The GTS to store.
     * @return true if the storing succeeded, false otherwise
     */
    public boolean store(String source, GTS gts) {
        
        // source
        // null -> hbase load
        // INGRESS_METADATA_SOURCE -> /update
        // INGRESS_METADATA_UPDATE_ENDPOINT -> /meta
        
        // Check if backend is alive
        if (!this.client.getConnectedStatus()) {
            this.client.tryToConnect();
            return false;
        }
        
        
        // When loading directory and restore activated
        if (null == source && this.ES_RESTORE) {
            if (this.ids.containsKey(gts.getId())) {
                return true;
            } else {
                missing++;
            }
            //return true;
        } 
        
        if (source != null && this.ES_RESTORE) {
            if (tmp.compareAndSet(false, true)) {
                LOG.info("Restore added " + missing + " gts");
            }
        }

        boolean notKnown = (null == this.ids.putIfAbsent(gts.getId(), true));
        
        if (notKnown){
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, 1);
        }
        
        // Based on the current GTS create an Elastic Item
        ElasticSearchItem storeItem = new ElasticSearchItem(gts, ES_APP, ES_INDEX_NAME);

        // If request corresponds to a meta update, then apply an update only on the series attributes
        if(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT == source && notKnown) {
            
			// Create an Elastic Update request
			UpdateRequest updateRequest = new UpdateRequest(storeItem.getIndex(), storeItem.getType(), storeItem.getId())
					.doc(storeItem.getAttributesSource(ES_ATTRIBUTES));

			// Add it to current bulk processor
			this.client.add(updateRequest);

			
        } else {
            // Create Elastic index request
            IndexRequest indexRequest;

            indexRequest = new IndexRequest(storeItem.getIndex(), storeItem.getType(), storeItem.getId())
                    .source(storeItem.getSource(ES_CLASS, ES_STORE_APP, ES_LABELS, ES_ATTRIBUTES));
            // Add it to current bulk processor
            this.client.add(indexRequest);
        }
        return true;
    }


    /**
     * Deletes a GTS from storage.
     * Note that the key for the GTS is the combination name + labels, the attributes are not part of the key.
     *
     * @param gts GTS to delete
     * @return
     */
    public boolean delete(GTS gts) {

        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);

        // Based on the current GTS create an Elastic Item
        ElasticSearchItem storeItem = new ElasticSearchItem(gts, ES_APP, ES_INDEX_NAME);
        DeleteRequest deleteRequest = new DeleteRequest(storeItem.getIndex(), storeItem.getType(), storeItem.getId());

        // Add it to current bulk processor
        this.client.add(deleteRequest);

        this.ids.remove(gts.getId());
        return true;
    }

    /**
     * Identify matching GTS.
     *
     * @param shard Shard ID for which the request is done
     * @param classSelector Regular expression for selecting the class name.
     * @param labelsSelectors Regular expressions for selecting the labels names.
     * @return An iterator on the matching GTS.
     */
    public GTSIterator find(int shard, String classSelector, Map<String, String> labelsSelectors) {
        // Let empty as it's not used in ES directory
        return null;
    }

    /**
     * Check if a given GTS is known.
     * This is used to avoid storing unknown GTS in HBase simply because they were
     * part of a /meta request.
     *
     * @param gts The GTS to check.
     * @return true if the GTS is known.
     */
    public boolean known (GTS gts) {
        return this.ids.containsKey(gts.getId());
    }

}
