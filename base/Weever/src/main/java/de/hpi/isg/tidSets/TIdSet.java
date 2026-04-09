package de.hpi.isg.tidSets;

import org.roaringbitmap.RoaringBitmap;

public interface TIdSet extends Iterable<Integer>, Cloneable {
    TIdSet clone();
    TIdSet intersect(TIdSet other);
    TIdSet union(TIdSet other);
    TIdSet minus(TIdSet other);
    TIdSet add(int tId);
    TIdSet remove(int tId);
    boolean isEmpty();
    int cardinality();

    boolean equals(Object other);

    int hashCode();

    RoaringBitmap getRoaring();
}

