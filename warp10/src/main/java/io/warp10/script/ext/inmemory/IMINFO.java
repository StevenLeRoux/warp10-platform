package io.warp10.script.ext.inmemory;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

public class IMINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    //private static final Logger LOG = LoggerFactory.getLogger(IMINFO.class);
    private final Map<String,Object> infos;
    private final long startTime;

    public IMINFO(String name, Properties warpConfigProperties) {
        super(name);
        //System.out.println("NEW IMINFO");

        this.infos = new HashMap<String, Object>();
        this.startTime = System.currentTimeMillis();

        if (warpConfigProperties.containsKey(Configuration.IN_MEMORY_CHUNKED)) {
            String chunkedStr = warpConfigProperties.getProperty(Configuration.IN_MEMORY_CHUNKED);
            infos.put("isChunked", "true".equals(chunkedStr));
        }
        if (warpConfigProperties.containsKey(Configuration.IN_MEMORY_CHUNK_COUNT)) {
            String chunkCountStr = warpConfigProperties.getProperty(Configuration.IN_MEMORY_CHUNK_COUNT);
            infos.put("chunkCount", Integer.parseInt(chunkCountStr));
        }
        if (warpConfigProperties.containsKey(Configuration.IN_MEMORY_CHUNK_LENGTH)) {
            String chunkDurationStr = warpConfigProperties.getProperty(Configuration.IN_MEMORY_CHUNK_LENGTH);
            infos.put("chunkDuration", Long.parseLong(chunkDurationStr));
        }
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {

        long uptime = System.currentTimeMillis() - this.startTime;
        this.infos.put("uptime", uptime * Constants.TIME_UNITS_PER_MS);

        stack.push(this.infos);
        return stack;
    }
}
