package de.hpi.isg.tidSets;

public class Operand {
    public TIdSet smallerTIds = null;
    public TIdSet equalTIds = null;
    public boolean smallerAndEqual = false;

    public Operand(TIdSet smallerTIds, TIdSet equalTIds, boolean smallerAndEqual) {
        this.smallerTIds = smallerTIds;
        this.equalTIds = equalTIds;
        this.smallerAndEqual = smallerAndEqual;
    }
    public Operand(TIdSet smallerTIds, TIdSet equalTIds) {
        this.smallerTIds = smallerTIds;
        this.equalTIds = equalTIds;
    }

    public Operand(TIdSet equalTIds) {
        this.equalTIds = equalTIds;
    }

    public Operand(){}
}
