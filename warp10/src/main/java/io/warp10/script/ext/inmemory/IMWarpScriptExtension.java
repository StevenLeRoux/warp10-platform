package io.warp10.script.ext.inmemory;

import java.util.HashMap;
import java.util.Map;
import io.warp10.WarpConfig;
import io.warp10.warp.sdk.WarpScriptExtension;

/**
 * Extension which defines in memory functions
 */
public class IMWarpScriptExtension extends WarpScriptExtension {

  private static final Map<String,Object> functions;

  static {
    functions = new HashMap<String, Object>();

    functions.put("IMINFO", new IMINFO("IMINFO", WarpConfig.getProperties()));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}