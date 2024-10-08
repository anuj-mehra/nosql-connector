package com.anuj.nosqlconnector.dao.dto;

import java.io.Serializable;

public class GetHBaseRecordDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    // Mandatory Parameters
    private String tableName;
    private Serializable rowKey;

    // Optional Parameters
    private String columnFamily;
    private String[] columnsToBeFetched;

    private GetHBaseRecordDTO(GetHBaseRecordBuilder builder){
        this.tableName = builder.tableName;
        this.rowKey = builder.rowKey;
        this.columnFamily = builder.columnFamily;
        this.columnsToBeFetched = builder.columnsToBeFetched;
    }

    public String getTableName() {
        return tableName;
    }

    public Serializable getRowKey() {
        return rowKey;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String[] getColumnsToBeFetched() {
        return columnsToBeFetched;
    }

    public static class GetHBaseRecordBuilder implements Serializable {

        private static final long serialVersionUID = 1L;

        // Mandatory Parameters
        private String tableName;
        private Serializable rowKey;

        // Optional Parameters
        private String columnFamily;
        private String[] columnsToBeFetched;

        public GetHBaseRecordBuilder(String tableName, Serializable rowKey){
            this.tableName = tableName;
            this.rowKey = rowKey;
        }

        public GetHBaseRecordBuilder columnFamily(final String columnFamily){
            this.columnFamily = columnFamily;
            return this;
        }

        public GetHBaseRecordBuilder columnFamily(final String[] columnsToBeFetched){
            this.columnsToBeFetched = columnsToBeFetched;
            return this;
        }

        public GetHBaseRecordDTO build(){
            return new GetHBaseRecordDTO(this);
        }
    }
}
