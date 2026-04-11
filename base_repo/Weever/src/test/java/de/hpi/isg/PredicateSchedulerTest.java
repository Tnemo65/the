package de.hpi.isg;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static de.hpi.isg.Operator.EQUAL;

public class PredicateSchedulerTest {
    @Test
    public void testTwoEqualPredicates() throws Exception {
        var a = new Predicate(0, 0, EQUAL);
        var b = new Predicate(1, 1, EQUAL);
        DenialConstraint dc = new DenialConstraint(b, a);

        PredicateScheduler predicateScheduler = new PredicateScheduler();
        predicateScheduler.initDataStructures(new ArrayList<>(List.of(dc)));
        Schedule[] schedule = predicateScheduler.scheduleFirstTime(new int[]{5, 3});
        Assert.assertArrayEquals(new Object[]{a, b}, schedule[0].predicates.toArray());
    }

    @Test
    public void testSameColEqualDiffColEqualPredicates() throws Exception {
        DenialConstraint dc = new DenialConstraint(new Predicate(0, 0, EQUAL), new Predicate(1, 2, EQUAL));

        PredicateScheduler predicateScheduler = new PredicateScheduler();
        predicateScheduler.initDataStructures(new ArrayList<>(List.of(dc)));
        Schedule[] schedule = predicateScheduler.scheduleFirstTime(new int[]{1, 10});

        Assert.assertArrayEquals(new Object[]{new Predicate(0, 0, EQUAL), new Predicate(2, 1, EQUAL)}, schedule[0].predicates.toArray());
    }

    @Test
    public void testFindingPrefixes() throws Exception {
        var a = new Predicate(0, 0, EQUAL);
        var b = new Predicate(1, 1, EQUAL);
        var c = new Predicate(2, 2, EQUAL);
        var d = new Predicate(3, 3, EQUAL);
        var e = new Predicate(4, 4, EQUAL);
        var f = new Predicate(5, 5, EQUAL);
        var g = new Predicate(6, 6, EQUAL);
        var h = new Predicate(7, 7, EQUAL);
        var i = new Predicate(8, 8, EQUAL);
        var j = new Predicate(9, 9, EQUAL);
        var k = new Predicate(10, 10, EQUAL);

//        var dc1 = new DenialConstraint(b, c, d, e, h);
//        var dc2 = new DenialConstraint(a, b, c, d, e, f);
//        var dc3 = new DenialConstraint(e, f, g);
//        var dc4 = new DenialConstraint(c, d, e, f, i);
//        var dc5 = new DenialConstraint(a, b, e, f, j);

//        var dc1 = new DenialConstraint(b, c, d, e, f, i);
//        var dc2 = new DenialConstraint(b, c, d, e, g);
//        var dc3 = new DenialConstraint(d, e, h);
//        var dc4 = new DenialConstraint(a, b, c, d, e, f);

//        var dc1 = new DenialConstraint(c, d, e, g);
//        var dc2 = new DenialConstraint(c, d, f, h);
//        var dc3 = new DenialConstraint(a, c, d, e);
//        var dc4 = new DenialConstraint(b, c, d, f);

        var dc1 = new DenialConstraint(b, c, d, e, f, h);
        var dc2 = new DenialConstraint(c, d, e, f, h, i);
        var dc3 = new DenialConstraint(e, f);
        var dc4 = new DenialConstraint(c, d, e, f, j, k);
        var dc5 = new DenialConstraint(a, c, d, e, f, j);
        var dc6 = new DenialConstraint(e, f, k);

        PredicateScheduler predicateScheduler = new PredicateScheduler();
        predicateScheduler.initDataStructures(new ArrayList<>(List.of(dc1, dc2, dc3, dc4, dc5, dc6)));

        /*var dc1 = new DenialConstraint(a, b, c, d);
        var dc2 = new DenialConstraint(   b, c, d, e);
        var dc3 = new DenialConstraint(      c, d,    f);
        var dc4 = new DenialConstraint(a, b,             g);*/


        Schedule[] schedule = predicateScheduler.scheduleFirstTime(new int[]{11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1});
//        var abc = PredicateScheduler.findPrefixes(schedule);
//        System.out.println(abc.dcPrefixes);

    }

}
