package com.anuj.nosqlconnector.dao.dto;

import org.apache.hadoop.hbase.filter.FilterList;

import java.io.Serializable;

public class ScanHBaseTableDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    //Mandatory Parameters
    private String tableName;

    // Optional Parameters
    private String columnFamily;
    private FilterList filterList;
    private String[] columnsToBeFetched;

    private ScanHBaseTableDTO(ScanHBaseTableBuilder builder){
        this.tableName = builder.tableName;
        this.columnFamily = builder.columnFamily;
        this.filterList = builder.filterList;
        this.columnsToBeFetched = builder.columnsToBeFetched;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public FilterList getFilterList() {
        return filterList;
    }

    public String[] getColumnsToBeFetched() {
        return columnsToBeFetched;
    }

    public static class ScanHBaseTableBuilder implements Serializable {
        private static final long serialVersionUID = 1L;

        //Mandatory Parameters
        private String tableName;

        // Optional Parameters
        private String columnFamily;
        private FilterList filterList;
        private String[] columnsToBeFetched;

        public ScanHBaseTableBuilder(String tableName){
            this.tableName = tableName;
        }

        public ScanHBaseTableBuilder columnFamily(final String columnFamily){
            this.columnFamily = columnFamily;
            return this;
        }

        public ScanHBaseTableBuilder filterList(final FilterList filterList){
            this.filterList = filterList;
            return this;
        }

        public ScanHBaseTableBuilder columnsToBeFetched(final String[] columnsToBeFetched){
            this.columnsToBeFetched = columnsToBeFetched;
            return this;
        }

        public ScanHBaseTableDTO build(){
            return new ScanHBaseTableDTO(this);
        }
    }
}
