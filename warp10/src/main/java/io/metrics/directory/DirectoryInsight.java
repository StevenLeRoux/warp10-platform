package io.metrics.directory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.warp.sdk.DirectoryPlugin;
import io.warp10.sensision.Sensision;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.Configuration;

/**
 * Extension which defines functions around concurrent execution
 * of WarpScript code.
 */
public class DirectoryInsight extends DirectoryPlugin {
  /**
   * label/attribute used as insight key
   */
  private String INSIGHT_KEY = ".csg";

  private static final Logger LOG = LoggerFactory.getLogger(DirectoryInsight.class);

  /**
   * Maps of insigh key to class name to gts
   */
  private final ConcurrentMap<String, Boolean> ids = new MapMaker().concurrencyLevel(64).makeMap();
  private final ConcurrentMap<String,Map<String,Map<String,AtomicReference<GTS>>>> metadatas = new MapMaker().concurrencyLevel(64).makeMap();
  private final ConcurrentMap<String,AtomicReference<GTS>> series = new MapMaker().concurrencyLevel(64).makeMap();
  private final ConcurrentSkipListSet<String> apps = new ConcurrentSkipListSet<>();

  /**
   * Initialize the plugin. This method is called immediately after a plugin has been instantiated.
   *
   * @param properties Properties from the Warp configuration file
   */
  public void init(Properties props){
    if (null != props.getProperty("directory.insight.key")) {
      this.INSIGHT_KEY = props.getProperty("directory.insight.key");
    }
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
  public boolean store(String source, GTS gts){
    // source
    // null -> hbase load
    // INGRESS_METADATA_SOURCE -> /update
    // INGRESS_METADATA_UPDATE_ENDPOINT -> /meta

    if (null == this.ids.putIfAbsent(gts.getId(), true)){
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, 1);
    }

    // Retrive insigh keys
    Collection<String> keysLabels = this.getKeys(gts.getLabels()).values();
    Collection<String> keysAttributes = this.getKeys(gts.getAttributes()).values();
    Collection<String> keys = new HashSet<>(keysLabels.size() + keysAttributes.size());
    keys.addAll(keysLabels);
    keys.addAll(keysAttributes);

    // clean previous entries if needed
    if (series.containsKey(gts.getId())) {
      GTS gtsD = null;
      boolean delete = false;
      if (keys.isEmpty()) {
        // remove serie from know series as is only know by attributes
        gtsD = series.remove(gts.getId()).get();
        delete = true;
      } else {
        gtsD = series.get(gts.getId()).get();
      }

      Collection<String> keysAttributesD = this.getKeys(gtsD.getAttributes()).values();

      if (delete) {
        Collection<String> keysLabelsD = this.getKeys(gtsD.getAttributes()).values();
        Collection<String> keysD = new HashSet<String>(keysLabelsD.size() + keysAttributesD.size());
        keysD.addAll(keysLabelsD);
        keysD.addAll(keysAttributesD);

        for (String key : keysD) {
          metadatas.get(key).get(gtsD.getName()).remove(gtsD.getId());
        }
        return true;
      } else {
        // clean old keys from attributes
        Collection<String> keysD = new HashSet<String>(keysAttributesD.size());
        keysD.addAll(keysAttributesD);
        keysD.removeAll(keysAttributes);
        for (String key : keysD) {
          metadatas.get(key).get(gtsD.getName()).remove(gtsD.getId());
        }

        // add only new keys
        keys.clear();
        keys.addAll(keysAttributes);
        keys.removeAll(keysAttributesD);
      }
    }

    // Ignore gts without insigh key
    if(keys.isEmpty()) {
      return true;
    }

    AtomicReference<GTS> gtsRef = new AtomicReference<GTS>(gts);

    // Add to series map
    AtomicReference<GTS> gtsRefP = series.putIfAbsent(gts.getId(), gtsRef);
    // Already know serie
    if (null != gtsRefP) {
      // Update gts
      gtsRefP.set(gts);
      gtsRef = gtsRefP;
    } else {
      apps.add(gts.getLabels().get(Constants.APPLICATION_LABEL));
    }

    for (String key : keys) {
      // Ensure csg is defined
      Map<String, Map<String, AtomicReference<GTS>>> classes = new ConcurrentSkipListMap<String, Map<String, AtomicReference<GTS>>>();
      Map<String, Map<String, AtomicReference<GTS>>> classesP = metadatas.putIfAbsent(key, classes);
      if (null != classesP) {
        classes = classesP;
      }

      // Ensure class map is defined
      Map<String, AtomicReference<GTS>> gtss = new ConcurrentSkipListMap<String, AtomicReference<GTS>>();
      Map<String, AtomicReference<GTS>> gtssP = classes.putIfAbsent(gts.getName(), gtss);
      if (null != gtssP) {
        gtss = gtssP;
      }

      // Add serie to csg classes
      gtss.put(gts.getId(), gtsRef);
    }

    Sensision.set("warp.directory.csg", Sensision.EMPTY_LABELS, metadatas.size());
    Sensision.set("warp.directory.series", Sensision.EMPTY_LABELS, series.size());
    Sensision.set("warp.directory.apps", Sensision.EMPTY_LABELS, apps.size());

    return true;
  }

  /**
   * Deletes a GTS from storage.
   * Note that the key for the GTS is the combination name + labels, the attributes are not part of the key.
   *
   * @param gts GTS to delete
   * @return
   */
  public boolean delete(GTS gts){
    this.ids.remove(gts.getId());

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);

    if (!series.containsKey(gts.getId())) {
      return true;
    }

    series.remove(gts.getId());

    // Retrive insigh keys
    Collection<String> keysLabels = this.getKeys(gts.getLabels()).values();
    Collection<String> keysAttributes = this.getKeys(gts.getAttributes()).values();
    Collection<String> keys = new HashSet<>(keysLabels.size() + keysAttributes.size());
    keys.addAll(keysLabels);
    keys.addAll(keysAttributes);

    for(String key: keys) {
      metadatas.get(key).get(gts.getName()).remove(gts.getId());
    }

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
  public GTSIterator find(int shard, String classSelector, Map<String,String> labelsSelectors) {
    // Process only insight app
    if(!"=insight".equals(labelsSelectors.get(Constants.APPLICATION_LABEL))) {
      LOG.warn("Bad " + Constants.APPLICATION_LABEL + " '" + labelsSelectors.get(Constants.APPLICATION_LABEL) + "' should be '=insight'. Returning empty answer");
      return new DirectoryInsightFindIterator();
    }
    // Process only insight owner
    if(!"=50b7bb74-0ddc-4e30-b010-8ef9fbce6366".equals(labelsSelectors.get(Constants.OWNER_LABEL))) {
      LOG.warn("Bad " + Constants.OWNER_LABEL + " '" + labelsSelectors.get(Constants.OWNER_LABEL) + "' should be '=50b7bb74-0ddc-4e30-b010-8ef9fbce6366'. Returning empty answer");
      return new DirectoryInsightFindIterator();
    }

    // Remove app and owner
    labelsSelectors.remove(Constants.APPLICATION_LABEL);
    labelsSelectors.remove(Constants.OWNER_LABEL);

    String key = labelsSelectors.get(this.INSIGHT_KEY);
    // Do not process query without INSIGHT_KEY
    if(null == key) {
      LOG.warn("Missing " + INSIGHT_KEY + ". Returning empty answer");
      return new DirectoryInsightFindIterator();
    }

    // only exact csg
    if (!key.startsWith("=")) {
      LOG.warn("Bad " + INSIGHT_KEY + " should not be a regex. Returning empty answer");
      return new DirectoryInsightFindIterator();
    }
    key = key.substring(1);

    labelsSelectors.remove(this.INSIGHT_KEY); // Series are already filter by key

    Map<String, Map<String, AtomicReference<GTS>>> classes = metadatas.get(key);
    if (null == classes) {
      return new DirectoryInsightFindIterator();
    }

    return new DirectoryInsightFindIterator(classSelector, labelsSelectors, classes);
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

  private Map<String, String> getKeys(Map<String, String> labels) {
    Map<String,String> keys = new HashMap<String,String>();

    for (String k : labels.keySet()) {
      if (k.startsWith(this.INSIGHT_KEY)) {
        keys.put(k, labels.get(k));
      }
    }

    return keys;
  }
}
