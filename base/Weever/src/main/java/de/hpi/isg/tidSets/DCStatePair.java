package de.hpi.isg.tidSets;

import java.util.ArrayList;

public class DCStatePair {
    public ArrayList<TIdSet> leftSets;
    public ArrayList<TIdSet> rightSets;

    public DCStatePair(ArrayList<TIdSet> left, ArrayList<TIdSet> right) {
        this.leftSets = left;
        this.rightSets = right;
    }
}
