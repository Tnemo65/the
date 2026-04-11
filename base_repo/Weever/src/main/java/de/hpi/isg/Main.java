package de.hpi.isg;

import de.hpi.isg.tidSets.TIdSetType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ListIterator;

public class Main {
    private static ConfigParameter parseConfigFile(String jsonString) throws Exception {
        JSONObject root = new JSONObject(jsonString);
        String csvPath = root.getString("csvPath");

        JSONArray constraints = root.getJSONArray("constraints");
        ArrayList<DenialConstraint> denialConstraints = new ArrayList<>(constraints.length());
        for (int i = 0; i < constraints.length(); i++) {
            JSONArray dc = constraints.getJSONArray(i);
            ArrayList<Predicate> predicates = new ArrayList<>(dc.length());
            for (int j = 0; j < dc.length(); j++) {
                JSONObject predicate = dc.getJSONObject(j);
                predicates.add(new Predicate(predicate.getInt("colIdx1"), predicate.getInt("colIdx2"), Operator.valueOf(predicate.getString("op")), j));
            }
            denialConstraints.add(new DenialConstraint(predicates));
        }

        JSONArray types = root.getJSONArray("types");
        ArrayList<String> typesList = new ArrayList<>(types.length());
        for (int i = 0; i < types.length(); i++) {
            typesList.add(types.getString(i));
        }
        JSONArray keyCols = root.getJSONArray("keyColumns");
        int[] keyColumns = new int[keyCols.length()];
        for (int i = 0; i < keyCols.length(); i++) {
            keyColumns[i] = keyCols.getInt(i);
        }
        String tIdSetType = root.getString("tIdSetType");
        boolean measureMemory = root.getBoolean("measureMemory");
        int maxRows = root.getInt("maxRows");
        boolean parallel = root.getBoolean("parallel");
        String dbName = root.getString("dbName");
        int measureGranularity = root.getInt("measureGranularity");
        String experimentName = root.getString("experimentName");
        int runId = root.getInt("runId");
        boolean combinePrefix = root.getBoolean("combinePrefix");
        String sortOrder = root.getString("sortOrder");
        boolean inverseSorting = root.getBoolean("inverseSorting");
        boolean storeViolations = root.getBoolean("storeViolations");
        JSONArray predicateOrder = root.getJSONArray("predicateOrder");
        int[] predicateIdx = new int[predicateOrder.length()];
        for (int i = 0; i < predicateOrder.length(); i++) {
            predicateIdx[i] = predicateOrder.getInt(i);
        }
        int batchSize = root.getInt("batchSize");
        return new ConfigParameter(csvPath, denialConstraints, typesList, keyColumns, tIdSetType, measureMemory, maxRows, parallel, dbName, measureGranularity, experimentName, runId, combinePrefix, sortOrder, inverseSorting, storeViolations, predicateIdx, batchSize);
    }

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "config.json";
        ConfigParameter configParameter = parseConfigFile(Files.readString(Paths.get(configPath)));
        ArrayList<Object[]> allTuples = new ArrayList<>();
        CSVParser parser = CSVFormat.DEFAULT.withDelimiter(',').withQuote('"').parse(Files.newBufferedReader(Paths.get(configParameter.csvPath)));
        int rows = 0;
        for (CSVRecord r : parser) {
            if (rows == 0) {
                rows++;
                continue;
            }
            int size = r.size();
            Object[] data = new Object[size];
            for (int i = 0; i < data.length; ++i) {
                switch (configParameter.types.get(i)) {
                    case "INTEGER":
                        data[i] = Integer.valueOf(r.get(i));
                        break;
                    case "FLOAT":
                        data[i] = Float.valueOf(r.get(i));
                        break;
                    default:
                        data[i] = r.get(i);
                }
            }
            allTuples.add(data);
            if (configParameter.maxRows > 0 && rows >= configParameter.maxRows) {
                break;
            }
            rows++;
        }

        allTuples.trimToSize();
        System.gc();

        execute(allTuples, configParameter);
    }

    static void execute(ArrayList<Object[]> allTuples, ConfigParameter configParameter) {
        Utils.type = TIdSetType.valueOf(configParameter.tIdSetType);
        Utils.combinePrefix = configParameter.combinePrefix;
        Utils.sortOrder = PredicateScheduler.SortOrder.valueOf(configParameter.sortOrder);
        Utils.inverseSorting = configParameter.inverseSorting;
        Utils.storeViolations = configParameter.storeViolations;
        Utils.predicateIdx = configParameter.predicateIdx;
        Utils.batchSize = configParameter.batchSize;

        if (configParameter.measureMemory) {
            Runtime runtime = Runtime.getRuntime();
            System.out.println("Used memory before: " + (runtime.totalMemory() - runtime.freeMemory()));
        }

        System.out.println("Start processing tuples");
        long start = System.nanoTime();
        Weever weever;
        if (configParameter.parallel) {
            weever = new WeeverSingleIndex(configParameter.constraints, configParameter.keyColumns, configParameter.types);
        } else {
            weever = new WeeverSequential(configParameter.constraints, configParameter.keyColumns, configParameter.types);
        }
        long initTime = System.nanoTime() - start;

        long insertTimeSum = 0;
        long longestInsertTime = 0;
        int longestTId = 0;
        long[] timePerHundredth = new long[allTuples.size() / configParameter.measureGranularity + 1];
        int[] errorsPerHundredth = new int[allTuples.size() / configParameter.measureGranularity + 1];
        for (int tId = 0; tId < allTuples.size(); tId++) {
            Object[] tuple = allTuples.get(tId);
            long tStart = System.nanoTime();
            weever.insert(tuple);
            long tmp = System.nanoTime() - tStart;
            insertTimeSum += tmp;
            if (tmp > longestInsertTime) {
                longestInsertTime = tmp;
                longestTId = tId;
            }
            if ((tId + 1) % configParameter.measureGranularity == 0) {
                timePerHundredth[(tId + 1) / configParameter.measureGranularity] = insertTimeSum / 1000000;
                errorsPerHundredth[(tId + 1) / configParameter.measureGranularity] = weever.getNumberOfViolations()[0];
                if (!configParameter.dbName.isEmpty()) {
                    try {
                        Class.forName("org.sqlite.JDBC");
                        Connection c = DriverManager.getConnection("jdbc:sqlite:" + configParameter.dbName);
                        c.setAutoCommit(false);
                        var stmt = c.createStatement();
                        stmt.executeUpdate(String.format("INSERT INTO %s (run_id, tuples, time, violations) VALUES ('%s', %s, %s, %s);", configParameter.experimentName, configParameter.runId, tId + 1, insertTimeSum / 1000000, Arrays.stream(weever.getNumberOfViolations()).sum()));
                        stmt.close();
                        c.commit();
                        c.close();
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
        }

        if (!configParameter.dbName.isEmpty()) {
            try {
                Class.forName("org.sqlite.JDBC");
                Connection c = DriverManager.getConnection("jdbc:sqlite:" + configParameter.dbName);
                c.setAutoCommit(false);
                var stmt = c.createStatement();
                stmt.executeUpdate(String.format("INSERT INTO %s (run_id, tuples, time, violations) VALUES ('%s', %s, %s, %s);", configParameter.experimentName, configParameter.runId, allTuples.size(), insertTimeSum / 1000000, Arrays.stream(weever.getNumberOfViolations()).sum()));
                stmt.close();
                c.commit();
                c.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

//        if (!configParameter.dbName.isEmpty()) {
//            try {
//                Class.forName("org.sqlite.JDBC");
//                Connection c = DriverManager.getConnection("jdbc:sqlite:" + configParameter.dbName);
//                c.setAutoCommit(false);
//                var stmt = c.createStatement();
//                stmt.executeUpdate(String.format("INSERT INTO %s (run_id, tuples, time, violations) VALUES ('%s', %s, %s, %s);", configParameter.experimentName, configParameter.runId, allTuples.size(), 0, Arrays.stream(weever.getNumberOfViolations()).sum()));
//                stmt.close();
//                c.commit();
//                c.close();
//            } catch (Exception e) {
//                System.out.println(e.getMessage());
//            }
//        }

        System.out.println("Violations on total dataset: " + Arrays.stream(weever.getNumberOfViolations()).sum());

        if (configParameter.measureMemory) {
            System.gc();
            Runtime runtime = Runtime.getRuntime();
            System.out.println("Used memory peak: " + (runtime.totalMemory() - runtime.freeMemory()));
        }

        long deleteTimeSum = 0;
        if (configParameter.storeViolations) {
//            Collections.shuffle(allTuples);
//
//            for (int tId = 0; tId < allTuples.size(); tId++) {
//                Object[] tuple = allTuples.get(tId);
//                long tStart = System.nanoTime();
//                weever.delete(tuple);
//                deleteTimeSum += System.nanoTime() - tStart;
//
//                if ((tId + 1) % configParameter.measureGranularity == 0) {
//                    timePerHundredth[(tId + 1) / configParameter.measureGranularity] = deleteTimeSum / 1000000;
//                    errorsPerHundredth[(tId + 1) / configParameter.measureGranularity] = weever.getNumberOfViolations()[0];
//                    if (!configParameter.dbName.isEmpty()) {
//                        try {
//                            Class.forName("org.sqlite.JDBC");
//                            Connection c = DriverManager.getConnection("jdbc:sqlite:" + configParameter.dbName);
//                            c.setAutoCommit(false);
//                            var stmt = c.createStatement();
//                            stmt.executeUpdate(String.format("INSERT INTO %s (run_id, tuples, time, violations) VALUES ('%s', %s, %s, %s);", configParameter.experimentName, configParameter.runId, allTuples.size() - tId + 1, deleteTimeSum / 1000000, Arrays.stream(weever.getNumberOfViolations()).sum()));
//                            stmt.close();
//                            c.commit();
//                            c.close();
//                        } catch (Exception e) {
//                            System.out.println(e.getMessage());
//                        }
//                    }
//                }
//            }
            for (ListIterator<Object[]> it = allTuples.listIterator(allTuples.size()); it.hasPrevious(); ) {
                Object[] tuple = it.previous();
                long tStart = System.nanoTime();
                weever.delete(tuple);
                deleteTimeSum += System.nanoTime() - tStart;
            }
        }

        System.out.println("Violations on no tuples: " + Arrays.stream(weever.getNumberOfViolations()).sum());

        System.out.println("Processing complete: " + (System.nanoTime() - start) / 1000000d);
        System.out.println("Init took: " + initTime / 1000000d);
        System.out.println("Inserting tuples took: " + (insertTimeSum / 1000000d));
        System.out.println("Deleting tuples took: " + (deleteTimeSum / 1000000d));
        System.out.println("Longest Insert took: " + (longestInsertTime / 1000000d) + " for tId: " + longestTId);
        if (configParameter.dbName.isEmpty()) {
            System.out.println("Times:" + Arrays.toString(timePerHundredth));
            System.out.println("Errors: " + Arrays.toString(errorsPerHundredth));
        }
    }
}
