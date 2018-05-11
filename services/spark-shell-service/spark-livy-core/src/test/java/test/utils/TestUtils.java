package test.utils;

import com.thinkbiganalytics.kylo.model.TestSerializing;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public class TestUtils {
    public static String getTestResourcesFileAsString(String filename) throws IOException {
        return IOUtils.toString(TestUtils.class.getClassLoader().getResourceAsStream(filename), Charset.defaultCharset());
    }
}
