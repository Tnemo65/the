package de.hpi.isg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class DenialConstraint implements Serializable {
    ArrayList<Predicate> predicates;

    public DenialConstraint(ArrayList<Predicate> predicates) throws Exception {
        if (predicates.isEmpty()) {
            throw new Exception("DC must contain at least one predicate");
        }
        this.predicates = predicates;
    }

    public DenialConstraint(Predicate... predicates) throws Exception {
        this(new ArrayList<>(Arrays.asList(predicates)));
    }
}