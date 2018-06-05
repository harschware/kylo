package com.thinkbiganalytics.kylo.utils;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.Map;

public class ScriptGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ScriptGenerator.class);

    @Resource
    private Map<String, String> scriptRegistry;

    public String script(String name, Object... args) {
        Validate.isTrue(scriptRegistry.containsKey(name),
                String.format("Unable to find a script with name = '%s'", name));

        // TODO: store in a dedent'ing map, rather than trim here
        String script = String.format(scriptRegistry.get(name).trim(), args);
        logger.debug("Constructed script with name='{}' as '{}'", name, script);

        return script;
    }


    public String wrappedScript(String name, String pre, String post, Object... args) {
        Validate.isTrue(scriptRegistry.containsKey(name),
                String.format("Unable to find a script with name = '%s'", name));

        // nulls ok
        pre = pre == null ? "" : pre;
        post = post == null ? "" : post;

        // TODO: store in a dedent'ing map, rather than trim here
        StringBuilder sb = new StringBuilder(pre);
        sb.append(String.format(scriptRegistry.get(name).trim(), args));
        sb.append(post).toString();

        String script = sb.toString();
        logger.debug("Constructed script with name='{}' as '{}'", name, script);

        return script;
    }
}
