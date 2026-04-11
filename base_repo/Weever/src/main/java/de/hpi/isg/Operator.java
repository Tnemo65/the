package de.hpi.isg;

import java.io.Serializable;

public enum Operator implements Serializable {
    EQUAL, UNEQUAL, GREATER, LESS, GREATER_EQUAL, LESS_EQUAL;

    public String shortString;
    public Operator inverted;
    public Operator tPrimeVersion;

    public <T> boolean eval(Comparable<T> value1, T value2) {
        if (this == EQUAL) {
            return value1.equals(value2);
        } else if (this == UNEQUAL) {
            return !value1.equals(value2);
        } else {
            int c = value1.compareTo(value2);
            switch (this) {
                case GREATER_EQUAL:
                    return c >= 0;
                case LESS:
                    return c < 0;
                case LESS_EQUAL:
                    return c <= 0;
                case GREATER:
                    return c > 0;
                default:
                    break;
            }
        }

        return false;
    }

    static {
        EQUAL.inverted = UNEQUAL;
        UNEQUAL.inverted = EQUAL;
        GREATER.inverted = LESS;
        GREATER_EQUAL.inverted = LESS_EQUAL;
        LESS.inverted = GREATER;
        LESS_EQUAL.inverted = GREATER_EQUAL;
    }

    static {
        EQUAL.tPrimeVersion = EQUAL;
        UNEQUAL.tPrimeVersion = UNEQUAL;
        GREATER.tPrimeVersion = LESS;
        GREATER_EQUAL.tPrimeVersion = LESS_EQUAL;
        LESS.tPrimeVersion = GREATER;
        LESS_EQUAL.tPrimeVersion = GREATER_EQUAL;
    }

    static {
        EQUAL.shortString = "==";
        UNEQUAL.shortString = "<>";
        GREATER.shortString = ">";
        LESS.shortString = "<";
        GREATER_EQUAL.shortString = ">=";
        LESS_EQUAL.shortString = "<=";
    }
}
