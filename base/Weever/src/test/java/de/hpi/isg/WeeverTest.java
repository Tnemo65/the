package de.hpi.isg;

import de.hpi.isg.tidSets.TIdSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class WeeverTest {
    boolean isParallel = false;

    static class ViolationBuilder {
        final Int2ObjectOpenHashMap<TIdSet> expectedViolations = new Int2ObjectOpenHashMap<>();

        public ViolationBuilder addViolation(int leftSide, int rightSide, boolean symmetrical) {
            TIdSet result = expectedViolations.computeIfAbsent(leftSide, a -> Utils.createNewTIdSet());
            if (rightSide != -1) {
                result.add(rightSide);
            }
            if (symmetrical) {
                addViolation(rightSide, leftSide, false);
            }
            return this;
        }

        public ViolationBuilder removeViolation(int leftSide, int rightSide, boolean symmetrical) {
            TIdSet set = expectedViolations.get(leftSide);
            set.remove(rightSide);
            if (set.isEmpty()) expectedViolations.remove(leftSide);

            if (symmetrical) {
                removeViolation(rightSide, leftSide, false);
            }
            return this;
        }

        public Int2ObjectOpenHashMap<TIdSet> getStaticExpectedViolations() {
            Int2ObjectOpenHashMap<TIdSet> clone = new Int2ObjectOpenHashMap<>();
            for (Int2ObjectMap.Entry<TIdSet> entry : expectedViolations.int2ObjectEntrySet()) {
                clone.put(entry.getIntKey(), entry.getValue().clone());
            }
            return clone;
        }
    }

    private Weever getWeeverInstance(ArrayList<DenialConstraint> denialConstraints, int[] keyColumns, ArrayList<String> columnTypes) {
        return new WeeverSequential(denialConstraints, keyColumns, columnTypes);
    }

    @Test
    public void testEqualOpSameCol() throws Exception {
        final Object[][] insertTuples = {{0, 0}, {1, 1}, {2, 0}, {3, 2}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 1, Operator.EQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < insertTuples.length; i++) {
            Object[] tuple = insertTuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }
        weever.delete(insertTuples[1]);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(insertTuples[0]);
        violationBuilder = new ViolationBuilder();
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testEqualOpDiffCol() throws Exception {
        final Object[][] insertTuples = {{0, 0, 0}, {1, 1, 1}, {2, 0, 0}, {3, 2, 2}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 2, Operator.EQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < insertTuples.length; i++) {
            Object[] tuple = insertTuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }
        weever.delete(insertTuples[1]);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(insertTuples[0]);
        violationBuilder = new ViolationBuilder();
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testUnequalOpSameCol() throws Exception {
        final Object[][] insertTuples = {{0, 0}, {1, 0}, {2, 1}, {3, 2}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 1, Operator.UNEQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, true).addViolation(1, 2, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 3, true).addViolation(1, 3, true).addViolation(2, 3, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < insertTuples.length; i++) {
            Object[] tuple = insertTuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(insertTuples[0]);
        violationBuilder.removeViolation(0, 2, true).removeViolation(0, 3, true);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(insertTuples[3]);
        violationBuilder.removeViolation(1, 3, true).removeViolation(2, 3, true);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(insertTuples[1]);
        violationBuilder.removeViolation(1, 2, true);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }


    @Test
    public void testUnequalOpDiffCol() throws Exception {
        final Object[][] insertTuples = {{0, 0, 0}, {1, 0, 0}, {2, 1, 1}, {3, 2, 2}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 2, Operator.UNEQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, true).addViolation(1, 2, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 3, true).addViolation(1, 3, true).addViolation(2, 3, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < insertTuples.length; i++) {
            Object[] tuple = insertTuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(insertTuples[0]);
        violationBuilder.removeViolation(0, 2, true).removeViolation(0, 3, true);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(insertTuples[3]);
        violationBuilder.removeViolation(1, 3, true).removeViolation(2, 3, true);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(insertTuples[1]);
        violationBuilder.removeViolation(1, 2, true);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testLessOpSameCol() throws Exception {
        final Object[][] tuples = {{0, 1}, {1, 1}, {2, 2}, {3, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 1, Operator.LESS));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, false).addViolation(1, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(3, 0, false).addViolation(3, 1, false).addViolation(3, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(0, 2, false).removeViolation(3, 0, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(3, 1, false).removeViolation(3, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(1, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testLessOpDiffCol() throws Exception {
        final Object[][] tuples = {{0, 1, 1}, {1, 1, 1}, {2, 2, 2}, {3, 0, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 2, Operator.LESS));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, false).addViolation(1, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(3, 0, false).addViolation(3, 1, false).addViolation(3, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(0, 2, false).removeViolation(3, 0, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(3, 1, false).removeViolation(3, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(1, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testLessEqualOpSameCol() throws Exception {
        final Object[][] tuples = {{0, 1}, {1, 1}, {2, 2}, {3, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 1, Operator.LESS_EQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 1, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, false).addViolation(1, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(3, 0, false).addViolation(3, 1, false).addViolation(3, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(0, 1, true).removeViolation(0, 2, false).removeViolation(3, 0, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(3, 1, false).removeViolation(3, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(1, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testLessEqualOpDiffCol() throws Exception {
        final Object[][] tuples = {{0, 1, 1}, {1, 1, 1}, {2, 2, 2}, {3, 0, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 2, Operator.LESS_EQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 1, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 2, false).addViolation(1, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(3, 0, false).addViolation(3, 1, false).addViolation(3, 2, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(0, 1, true).removeViolation(0, 2, false).removeViolation(3, 0, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(3, 1, false).removeViolation(3, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(1, 2, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testGreaterOpSameCol() throws Exception {
        final Object[][] tuples = {{0, 1}, {1, 1}, {2, 2}, {3, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 1, Operator.GREATER));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(2, 0, false).addViolation(2, 1, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 3, false).addViolation(1, 3, false).addViolation(2, 3, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(2, 0, false).removeViolation(0, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(1, 3, false).removeViolation(2, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(2, 1, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testGreaterOpDiffCol() throws Exception {
        final Object[][] tuples = {{0, 1, 1}, {1, 1, 1}, {2, 2, 2}, {3, 0, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 2, Operator.GREATER));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(2, 0, false).addViolation(2, 1, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 3, false).addViolation(1, 3, false).addViolation(2, 3, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(2, 0, false).removeViolation(0, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(1, 3, false).removeViolation(2, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(2, 1, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testGreaterEqualOpSameCol() throws Exception {
        final Object[][] tuples = {{0, 1}, {1, 1}, {2, 2}, {3, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 1, Operator.GREATER_EQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 1, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(2, 0, false).addViolation(2, 1, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 3, false).addViolation(1, 3, false).addViolation(2, 3, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(0, 1, true).removeViolation(2, 0, false).removeViolation(0, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(1, 3, false).removeViolation(2, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(2, 1, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }

    @Test
    public void testGreaterEqualOpDiffCol() throws Exception {
        final Object[][] tuples = {{0, 1, 1}, {1, 1, 1}, {2, 2, 2}, {3, 0, 0}};
        DenialConstraint constraint = new DenialConstraint(new Predicate(1, 2, Operator.GREATER_EQUAL));
        Weever weever = getWeeverInstance(new ArrayList<>(Collections.singletonList(constraint)), new int[]{0}, new ArrayList<>(Arrays.asList("INTEGER", "INTEGER", "INTEGER")));
        ArrayList<Int2ObjectOpenHashMap<TIdSet>> expectedViolations = new ArrayList<>();
        ViolationBuilder violationBuilder = new ViolationBuilder();
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 1, true);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(2, 0, false).addViolation(2, 1, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());
        violationBuilder.addViolation(0, 3, false).addViolation(1, 3, false).addViolation(2, 3, false);
        expectedViolations.add(violationBuilder.getStaticExpectedViolations());

        for (int i = 0; i < tuples.length; i++) {
            Object[] tuple = tuples[i];
            weever.insert(tuple);
            Assert.assertEquals(expectedViolations.get(i), weever.getViolations().get(0));
        }

        weever.delete(tuples[0]);
        violationBuilder.removeViolation(0, 1, true).removeViolation(2, 0, false).removeViolation(0, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[3]);
        violationBuilder.removeViolation(1, 3, false).removeViolation(2, 3, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
        weever.delete(tuples[1]);
        violationBuilder.removeViolation(2, 1, false);
        Assert.assertEquals(violationBuilder.getStaticExpectedViolations(), weever.getViolations().get(0));
    }


}
