package io.metrics.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import io.warp10.continuum.store.Constants;
import io.warp10.warp.sdk.DirectoryPlugin.GTS;

/**
 * Transfom a GTS into an ElasticRequest
 */
public class ElasticSearchItem {

    private String index;
    private String id;

    private String seriesName;
    private List<Map<String, String>> labels;
    private List<Map<String, String>> attributes;
    private String applicationName;

    private Boolean hasLastActivity = false;
    private Long lastActivity;


    public String generateMetaJson(String type) throws JSONException {
        // Create a JSON index Object
        JSONObject indexJSON = new JSONObject();

        indexJSON.put("_type", type);
        indexJSON.put("_id",id);
        indexJSON.put("_index", index);

        // Generate ES meta data JSON
        JSONObject metaJSON = new JSONObject();
        metaJSON.put("index", indexJSON);
        return metaJSON.toString();
    }

    /**
     * Construct a new ES item based on a GTS
     * @return
     */ 
    public ElasticSearchItem(GTS gts, String application, String indexName) {
        this.index = indexName;
        this.id = gts.getId();
        this.seriesName = gts.getName();
        this.labels = removeUnapropriateData(gts.getLabels());
        this.attributes = removeUnapropriateData(gts.getAttributes());
        this.applicationName = gts.getLabels().get(application);
        if (gts.isSetLastActivity()) {
            this.lastActivity = gts.getLastActivity();
            this.hasLastActivity = true;
        }
        
    }

    /**
     * Remove generated Warp labels and tags that can't be overwritten in Elastic Search
     * @param tagMap
     * @return
     */
    private List<Map<String, String>> removeUnapropriateData(final Map<String,String> tagMap) {

        List<Map<String, String>> result = new ArrayList<Map<String,String>>(tagMap.size()); 
        Map<String, String> tags = new HashMap<String,String>(tagMap.size());

        // Add all tag
        tags.putAll(tagMap);

        // Remove all tags warp10 internal tags starting with a "."
        tags.keySet().removeIf(key -> key.equals(Constants.APPLICATION_LABEL));
        tags.keySet().removeIf(key -> key.equals(Constants.PRODUCER_LABEL));
        tags.keySet().removeIf(key -> key.equals(Constants.OWNER_LABEL));

        // Remove all tags equals to _type, _index and _id
        tags.keySet().removeIf(key -> key.equals("_type"));
        tags.keySet().removeIf(key -> key.equals("_index"));
        tags.keySet().removeIf(key -> key.equals("_id"));

        // Create map list
        for ( String key : tags.keySet()) {
            HashMap<String, String> item = new HashMap<>();
            item.put("key", key);
            item.put("value", tags.get(key));
            result.add(item);
        }
        return result;
    }

    /**
     * Generated Index getter
     * @return
     */
    public String getIndex() {
        return index;
    }

    /**
     * Generated Id getter
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * Generated Type getter
     * @return
     */
    public String getType() {
        return "doc";
    }

    /**
     * Generated Attributes getter
     * @return
     */
    public List<Map<String, String>> getAttributes() {
        return attributes;
    }

    /** 
     * Get Attributes Sources map for update requests
     * @param attributesKey
     * @return
     */
    public Map<String, Object> getUpdateSource(String attributesKey, String lastActivityKey) {
        Map<String, Object> source =  new HashMap<String,Object>();
        source.put(attributesKey, attributes);  
        if (this.hasLastActivity) {
            source.put(lastActivityKey, this.lastActivity);
        }
        return source;
    }

    /**
     * Generate Data Source map
     * @return
     * @throws IOException 
     */
    public Map<String, Object> getSource(String classNameKey, String applicationKey, String labelsKey, String attributesKey, String lastActivityKey) {

        Map<String, Object> source =  new HashMap<String,Object>(4);
        source.put(labelsKey, this.labels);
        source.put(attributesKey, this.attributes);
        source.put(classNameKey, this.seriesName);
        source.put(applicationKey, this.applicationName);
        
        if (this.hasLastActivity) {
            source.put(lastActivityKey, this.lastActivity);
        }

        
        return source;
    }
}
