package io.warp10.script.ext.distupdate;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;
import io.warp10.script.functions.UPDATE;

/**
 * Extension which defines functions to perform remote updates
 */
public class DistUpdateWarpScriptExtension extends WarpScriptExtension {

  private static final Map<String,Object> functions;

  static {
    functions = new HashMap<String, Object>();

    functions.put("DISTUPDATE", new UPDATE("DISTUPDATE", true));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }

}
