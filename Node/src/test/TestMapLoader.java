package test;

import com.hazelcast.core.MapLoader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by lba on 2015-05-18.
 */
public class TestMapLoader implements MapLoader<String, String> {
    private static String data = "fpjfpåjfås ohgåosr hgorsfwdtyjty0wcty0w8jyt0dntr0ncyt87nyr08cmy0rqxjyr09jdkyr09ncyvrt09m8wdxyrh0cnqyrn08qxyr04ncqtr4o8qcntr087qnxtr087qcjtr08qxnt084q37ntrcx084m70tr8nxqtr8o47qtxjr7o84xnqtr40783nqttn qpm9c+cqmåmåpqhxf479qxyt9qxpmyt94q3yxm9tr4pqty4qxmyt94pqxymshphfmx7qx";

    static Map<String, String> dataMap = new HashMap<>();
    static {
        for(int i=0;i<500;i++) {
            dataMap.put("" + i, i + " " + data);
        }
    }

    @Override
    public String load(String key) {
        return dataMap.get(key);
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        Map<String, String> result = new HashMap<>();
        for(String key : keys) {
            result.put(key, dataMap.get(key));
        }
        return result;
    }

    @Override
    public Set<String> loadAllKeys() {
        return dataMap.keySet();
    }
}
