package de.hpi.isg;

import de.hpi.isg.tidSets.*;

import java.util.HashMap;

public class Utils {
    public static boolean combinePrefix;
    static TIdSetType type = TIdSetType.RoaringBitSet;
    public static TIdSet createNewTIdSet() {
        switch (type) {
            case HashSet:
                return new HashTIdSet();
            case JavaBitSet:
                return new BitTidSet();
            case RoaringBitSet:
                return new RoaringTidSet();
        }
        return new RoaringTidSet();
    }

    static PredicateScheduler.SortOrder sortOrder = PredicateScheduler.SortOrder.selectivityOnly;
    static boolean inverseSorting = false;
    static boolean storeViolations = true;
    static int[] predicateIdx = new int[0];
    static int batchSize = 1;
}
