package io.metrics.directory;

import java.util.NoSuchElementException;
import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.Map.Entry;
import java.util.ArrayList;

import io.warp10.warp.sdk.DirectoryPlugin.GTS;
import io.warp10.warp.sdk.DirectoryPlugin.GTSIterator;
import io.warp10.SmartPattern;

public class DirectoryInsightFindIterator extends GTSIterator {
  Map<String, Map<String, AtomicReference<GTS>>> classes;

  Iterator<String> classesNames;
  Iterator<AtomicReference<GTS>> gtss;
  GTS gts;

  SmartPattern classSmartPattern;
  List<String> labelNames;
  List<SmartPattern> labelPatterns;
  List<String> labelValues;

  public DirectoryInsightFindIterator() {
    this.classesNames = Collections.emptyIterator();
    this.gtss = Collections.emptyIterator();
  }

  public DirectoryInsightFindIterator(String classSelector, Map<String,String> labelsSelectors, Map<String, Map<String, AtomicReference<GTS>>> classes) {
    this.classes = classes;

    if (!classSelector.startsWith("~")) {
      // Exact classname
      String className = classSelector.startsWith("=") ? classSelector.substring(1) : classSelector;

      // No need to iterate through class names
      this.classesNames = Collections.emptyIterator();
      Map<String, AtomicReference<GTS>> gtss = classes.get(className);
      if (null == gtss) {
        this.gtss = Collections.emptyIterator();
      } else {
        this.gtss = gtss.values().iterator();
      }
    } else {
      this.classSmartPattern = new SmartPattern(Pattern.compile(classSelector.substring(1)));
      this.classesNames = this.classes.keySet().iterator();
      this.gtss = Collections.emptyIterator();
    }

    // Build label patterns
    Map<String,SmartPattern> wildPatterns = new HashMap<String,SmartPattern>();
    Map<String,SmartPattern> exactPatterns = new HashMap<String,SmartPattern>();
    Map<String,SmartPattern> regexPatterns = new HashMap<String,SmartPattern>();
    for (Entry<String,String> entry: labelsSelectors.entrySet()) {
      String label = entry.getKey();
      String expr = entry.getValue();

      if (!expr.startsWith("~")) {
        SmartPattern pattern = new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr);
        exactPatterns.put(label, pattern);
      } else {
        Pattern p = Pattern.compile(expr.substring(1));
        SmartPattern pattern = new SmartPattern(p);
        if (".*".equals(p.pattern()) || "^.*".equals(p.pattern()) || "^.*$".equals(p.pattern())) {
          wildPatterns.put(label, pattern);
        } else {
          regexPatterns.put(label, pattern);
        }
      }
    }

    // Sort label patterns
    this.labelNames = new ArrayList<String>(wildPatterns.size() + exactPatterns.size() + regexPatterns.size());
    this.labelPatterns = new ArrayList<SmartPattern>(this.labelNames.size());
    for (Entry<String,SmartPattern> entry: wildPatterns.entrySet()) {
      this.labelNames.add(entry.getKey());
      this.labelPatterns.add(entry.getValue());
    }
    for (Entry<String,SmartPattern> entry: exactPatterns.entrySet()) {
      this.labelNames.add(entry.getKey());
      this.labelPatterns.add(entry.getValue());
    }
    for (Entry<String,SmartPattern> entry: regexPatterns.entrySet()) {
      this.labelNames.add(entry.getKey());
      this.labelPatterns.add(entry.getValue());
    }

    this.labelValues = new ArrayList<String>(this.labelNames.size());

    this.gts = this.fNext();
  }

  public boolean hasNext() {
    return null != this.gts;
  }

  public GTS next() {
    if (null == this.gts) {
      throw new NoSuchElementException();
    }

    GTS gts = this.gts;

    this.gts = this.fNext();

    return gts;
  }

  public void close() {

  }

  private GTS fNext() {
    while(this.classesNames.hasNext() || this.gtss.hasNext()) {
      while(this.gtss.hasNext()) {
        GTS gts = this.gtss.next().get();

        // Extract gts labels values to match
        this.labelValues.clear();
        for (String labelName: this.labelNames) {
          String labelValue = gts.getLabels().get(labelName);
          if (null == labelValue) {
            labelValue = gts.getAttributes().get(labelName);
            if (null == labelValue) {
              break;
            }
          }

          this.labelValues.add(labelValue);
        }

        // If we did not collect enough label/attribute values, exclude the GTS
        if (this.labelValues.size() < this.labelNames.size()) {
          continue;
        }

        // Check if the label value matches, if not, exclude the GTS
        boolean match = true;
        for (int j = 0; j < this.labelNames.size(); j++) {
          if (!this.labelPatterns.get(j).matches(this.labelValues.get(j))) {
            match = false;
            break;
          }
        }
        if (match) {
          return gts;
        }
      }

      // look for new gtss to process
      while(this.classesNames.hasNext() && !this.gtss.hasNext()) {
        String className = this.classesNames.next();

        if (this.classSmartPattern.matches(className)) {
          this.gtss = this.classes.get(className).values().iterator();
        }
      }
    }

    return null;
  }
}
