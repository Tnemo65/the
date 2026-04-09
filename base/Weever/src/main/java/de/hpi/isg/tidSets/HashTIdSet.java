package de.hpi.isg.tidSets;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;

public class HashTIdSet implements TIdSet {
    IntOpenHashSet set;

    public HashTIdSet() {
        set = new IntOpenHashSet();
    }

    @Override
    public TIdSet clone() {
        HashTIdSet h;
        try {
            h = (HashTIdSet) super.clone();
        } catch (CloneNotSupportedException var3) {
            throw new InternalError();
        }
        h.set = set.clone();
        return h;
    }

    @Override
    public TIdSet intersect(TIdSet other) {
        set.retainAll(((HashTIdSet) other).set);
        return this;
    }

    @Override
    public TIdSet union(TIdSet other) {
        set.addAll(((HashTIdSet) other).set);
        return this;
    }

    @Override
    public TIdSet minus(TIdSet other) {
        set.removeAll(((HashTIdSet) other).set);
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
        return set.size();
    }

    @Override
    public RoaringBitmap getRoaring() {
        return null;
    }

    @Override
    public Iterator<Integer> iterator() {
        return set.iterator();
    }
}
