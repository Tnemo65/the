package de.hpi.isg.tidSets;

import de.hpi.isg.Utils;
import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;

public class RoaringTidSet implements TIdSet {
    RoaringBitmap set;

    public RoaringTidSet() {
        set = new RoaringBitmap();
    }

    public RoaringTidSet(RoaringBitmap bitmap) {
        set = bitmap;
    }

    @Override
    public TIdSet clone() {
        RoaringTidSet r;
        try {
            r = (RoaringTidSet) super.clone();
        } catch (CloneNotSupportedException var3) {
            throw new InternalError();
        }
        r.set = set.clone();
        return r;
    }

    @Override
    public TIdSet intersect(TIdSet other) {
        set.and(((RoaringTidSet) other).set);
        // Utils.numSets += 2;
        // Utils.setSize += set.getCardinality();
        // Utils.setSize += other.cardinality();
        return this;
    }

    @Override
    public TIdSet union(TIdSet other) {
        set.or(((RoaringTidSet) other).set);
        // Utils.numSets += 2;
        // Utils.setSize += set.getCardinality();
        // Utils.setSize += other.cardinality();
        return this;
    }

    @Override
    public TIdSet minus(TIdSet other) {
        set.andNot(((RoaringTidSet) other).set);
        // Utils.numSets += 2;
        // Utils.setSize += set.getCardinality();
        // Utils.setSize += other.cardinality();
        return this;
    }

    @Override
    public TIdSet add(int tId) {
        set.add(tId);
        return this;
    }

    @Override
    public TIdSet remove(int tId) {
        set.remove(tId);
        return this;
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public int cardinality() {
        return set.getCardinality();
    }

    @Override
    public Iterator<Integer> iterator() {
        return set.iterator();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || getClass() != other.getClass()) return false;

        RoaringTidSet o = (RoaringTidSet) other;
        return set.equals(o.set);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public RoaringBitmap getRoaring() {
        return set;
    }

    @Override
    public String toString() {
        return set.toString();
    }
}
