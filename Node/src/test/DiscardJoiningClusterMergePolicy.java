package test;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DiscardJoiningClusterMergePolicy implements MapMergePolicy {
    private static final transient Logger LOGGER = Logger.getLogger(DiscardJoiningClusterMergePolicy.class);

    public DiscardJoiningClusterMergePolicy() {
        LOGGER.info("Initialising "+this.getClass().getSimpleName());
    }

    @Override
    public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
        LOGGER.info("Merge returns " + (null == existingEntry ? "null" : existingEntry.getValue()) + ", merging entry was " + (null == mergingEntry ? "null" : mergingEntry.getValue()));
        return null != existingEntry ? existingEntry.getValue() : null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
