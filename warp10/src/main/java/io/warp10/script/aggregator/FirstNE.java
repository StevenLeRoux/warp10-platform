//
//   Copyright 2016  Cityzen Data
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.script.aggregator;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptException;

/**
 * Return the first value not equal to a threshold
 *
 */
public class FirstNE extends NamedWarpScriptFunction implements WarpScriptAggregatorFunction, WarpScriptMapperFunction, WarpScriptReducerFunction, WarpScriptBucketizerFunction {
  
  private long lthreshold;
  private double dthreshold;
  private String sthreshold;
  private Boolean bthreshold;
  
  private TYPE type;
  
  public FirstNE(String name, long threshold) {
    super(name);
    this.type = TYPE.LONG;
    this.lthreshold = threshold;
  }
  
  public FirstNE(String name, double threshold) {
    super(name);
    this.type = TYPE.DOUBLE;
    this.dthreshold = threshold;
  }

  public FirstNE(String name, String threshold) {
    super(name);
    this.type = TYPE.STRING;
    this.sthreshold = threshold;
  }
  
  public FirstNE(String name, boolean threshold) {
    super(name);
    this.type = TYPE.BOOLEAN;
    this.bthreshold = threshold ? Boolean.TRUE : Boolean.FALSE;
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    
    long tick = Long.MAX_VALUE;
    
    long[] ticks = (long[]) args[3];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];
    
    int idx = -1;
    
    for (int i = 0; i < values.length; i++) {
      // skip ticks older than the one we already identified
      if (ticks[i] >= tick) {
        continue;
      }

      if (values[i] instanceof Number) {
        switch (type) {
          case LONG:
            if (((Number) values[i]).longValue() != lthreshold) {
              idx = i;
              tick = ticks[i];
            }
            break;
          case DOUBLE:
            if (((Number) values[i]).doubleValue() != dthreshold) {
              idx = i;
              tick = ticks[i];
            }
            break;
          case STRING:
            if (!sthreshold.equals(values[i].toString())) {
              idx = i;
              tick = ticks[i];
            }
            break;
          case BOOLEAN:
            if (!bthreshold.equals(values[i])) {
              idx = i;
              tick = ticks[i];
            }
            break;            
        }        
      } else if (values[i] instanceof String && TYPE.STRING == type) {
        if (!sthreshold.equals(values[i])) {
          idx = i;
          tick = ticks[i];
        }
      } else if (values[i] instanceof Boolean && TYPE.BOOLEAN == type) {
        if (!bthreshold.equals(values[i])) {
          idx = i;
          tick = ticks[i];
        }
      }
    }
    
    if (-1 != idx) {
      return new Object[] { ticks[idx], locations[idx], elevations[idx], values[idx] };
    } else {
      return new Object[] { args[0], GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, null };      
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    switch (type) {
      case LONG:
        sb.append(StackUtils.toString(lthreshold));
        break;
      case DOUBLE:
        sb.append(StackUtils.toString(dthreshold));
        break;
      case STRING:
        sb.append(StackUtils.toString(sthreshold));
        break;
      case BOOLEAN:
        sb.append(StackUtils.toString(bthreshold));
        break;
    }
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();      
  }
}
