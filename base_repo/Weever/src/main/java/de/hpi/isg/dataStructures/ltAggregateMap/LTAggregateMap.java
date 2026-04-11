package de.hpi.isg.dataStructures.ltAggregateMap;

import de.hpi.isg.tidSets.Operand;
import de.hpi.isg.tidSets.TIdSet;

public interface LTAggregateMap {
    Operand findSmallerTIds(final Number k, boolean inclusive);
    void addAndCreateIfNecessary(final Number k, final int tId);
    void removeTIdAndSetIfNecessary(final Number k, final int tId);
}
