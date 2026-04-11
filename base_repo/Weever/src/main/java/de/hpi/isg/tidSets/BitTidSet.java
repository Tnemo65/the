package de.hpi.isg.tidSets;

import org.roaringbitmap.RoaringBitmap;

import java.util.BitSet;
import java.util.Iterator;

public class BitTidSet implements TIdSet {
    BitSet set;

    public BitTidSet() {
        set = new BitSet();
    }

    @Override
    public TIdSet clone() {
        BitTidSet b;
        try {
            b = (BitTidSet) super.clone();
        } catch (CloneNotSupportedException var3) {
            throw new InternalError();
        }
        b.set = (BitSet) set.clone();
        return b;
    }

    @Override
    public TIdSet intersect(TIdSet other) {
        set.and(((BitTidSet) other).set);
        return this;
    }

    @Override
    public TIdSet union(TIdSet other) {
        set.or(((BitTidSet) other).set);
        return this;
    }

    @Override
    public TIdSet minus(TIdSet other) {
        set.andNot(((BitTidSet) other).set);
        return this;
    }

    @Override
    public TIdSet add(int tId) {
        set.set(tId);
        return this;
    }

    @Override
    public TIdSet remove(int tId) {
        set.clear(tId);
        return this;
    }

    @Override
    public boolean isEmpty() {
        return set.length() == 0;
    }

    @Override
    public int cardinality() {
        return set.cardinality();
    }

    @Override
    public RoaringBitmap getRoaring() {
        return null;
    }

    @Override
    public Iterator<Integer> iterator() {
        return set.stream().iterator();
    }
}
