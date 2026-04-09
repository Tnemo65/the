package de.hpi.isg;

import java.util.ArrayList;

public class ConfigParameter {
    public int maxRows;
    public String csvPath;
    public ArrayList<DenialConstraint> constraints;
    public ArrayList<String> types;

    public int[] keyColumns;
    public String tIdSetType;
    public boolean measureMemory;
    public boolean parallel;
    public String dbName;
    public int measureGranularity;
    public String experimentName;
    public int runId;
    public boolean combinePrefix;
    public String sortOrder;
    public boolean inverseSorting;
    public boolean storeViolations;
    public int[] predicateIdx;
    public int batchSize;

    public ConfigParameter(String csvPath, ArrayList<DenialConstraint> constraints, ArrayList<String> types, int[] keyColumns, String tIdSetType, boolean measureMemory, int maxRows, boolean parallel, String dbName, int measureGranularity, String experimentName, int runId, boolean combinePrefix, String sortOrder, boolean inverseSorting, boolean storeViolations, int[] predicateIdx, int batchSize) {
        this.csvPath = csvPath;
        this.constraints = constraints;
        this.types = types;
        this.keyColumns = keyColumns;
        this.tIdSetType = tIdSetType;
        this.measureMemory = measureMemory;
        this.maxRows = maxRows;
        this.parallel = parallel;
        this.dbName = dbName;
        this.measureGranularity = measureGranularity;
        this.experimentName = experimentName;
        this.runId = runId;
        this.combinePrefix = combinePrefix;
        this.sortOrder = sortOrder;
        this.inverseSorting = inverseSorting;
        this.storeViolations = storeViolations;
        this.predicateIdx = predicateIdx;
        this.batchSize = batchSize;
    }
}
