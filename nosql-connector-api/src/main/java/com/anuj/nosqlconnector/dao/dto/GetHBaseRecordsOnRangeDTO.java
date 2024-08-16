package com.anuj.nosqlconnector.dao.dto;

import java.io.Serializable;

public class GetHBaseRecordsOnRangeDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    // Mandatory Parameters
    private String tableName;
    private Serializable startKey;
    private Serializable endKey;
    private boolean startKeyInclusive;
    private boolean endKeyInclusive;

    // Optional Parameters
    private String columnFamily;
    private String[] columnsToBeFetched;

    public String getTableName() {
        return tableName;
    }

    public Serializable getStartKey() {
        return startKey;
    }

    public Serializable getEndKey() {
        return endKey;
    }

    public boolean isStartKeyInclusive() {
        return startKeyInclusive;
    }

    public boolean isEndKeyInclusive() {
        return endKeyInclusive;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String[] getColumnsToBeFetched() {
        return columnsToBeFetched;
    }

    private GetHBaseRecordsOnRangeDTO(GetHBaseRecordsOnRangeBuilder builder){
        this.tableName = builder.tableName;
        this.startKey = builder.startKey;
        this.endKey = builder.endKey;
        this.startKeyInclusive = builder.startKeyInclusive;
        this.endKeyInclusive = builder.endKeyInclusive;
        this.columnFamily = builder.columnFamily;
        this.columnsToBeFetched = builder.columnsToBeFetched;
    }

    public static class GetHBaseRecordsOnRangeBuilder implements Serializable {
        private static final long serialVersionUID = 1L;

        // Mandatory Parameters
        private String tableName;
        private Serializable startKey;
        private Serializable endKey;
        private boolean startKeyInclusive;
        private boolean endKeyInclusive;

        // Optional Parameters
        private String columnFamily;
        private String[] columnsToBeFetched;

        public GetHBaseRecordsOnRangeBuilder(String tableName, Serializable startKey, Serializable endKey,
                                             boolean startKeyInclusive, boolean endKeyInclusive){
            this.tableName = tableName;
            this.startKey = startKey;
            this.endKey = endKey;
            this.startKeyInclusive = startKeyInclusive;
            this.endKeyInclusive = endKeyInclusive;
        }

        public GetHBaseRecordsOnRangeBuilder columnFamily(String columnFamily){
            this.columnFamily = columnFamily;
            return this;
        }

        public GetHBaseRecordsOnRangeBuilder columnsToBeFetched(String[] columnsToBeFetched){
            this.columnsToBeFetched = columnsToBeFetched;
            return this;
        }

        public GetHBaseRecordsOnRangeDTO build(){
            return new GetHBaseRecordsOnRangeDTO(this);
        }
    }
}
