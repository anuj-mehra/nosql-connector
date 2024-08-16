package com.anuj.nosqlconnector.dao.dto;

import java.io.Serializable;
import java.util.List;

public class GetHBaseRecordsDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    // Mandatory Parameters
    private String tableName;
    private List<? extends Serializable> rowKeys;

    // Optional Parameters
    private String columnFamily;
    private String[] columnsToBeFetched;

    private GetHBaseRecordsDTO(GetHBaseRecordsBuilder builder){
        this.tableName = builder.tableName;
        this.rowKeys = builder.rowKeys;
        this.columnFamily = builder.columnFamily;
        this.columnsToBeFetched = builder.columnsToBeFetched;
    }


    public static class GetHBaseRecordsBuilder implements Serializable {

        private static final long serialVersionUID = 1L;

        // Mandatory Parameters
        private String tableName;
        private List<? extends Serializable> rowKeys;

        // Optional Parameters
        private String columnFamily;
        private String[] columnsToBeFetched;

        public GetHBaseRecordsBuilder(String tableName, List<? extends Serializable> rowKeys){
            this.tableName = tableName;
            this.rowKeys = rowKeys;
        }

        public GetHBaseRecordsBuilder columnFamily(final String columnFamily){
            this.columnFamily = columnFamily;
            return this;
        }

        public GetHBaseRecordsBuilder columnFamily(final String[] columnsToBeFetched){
            this.columnsToBeFetched = columnsToBeFetched;
            return this;
        }

        public GetHBaseRecordsDTO build(){
            return new GetHBaseRecordsDTO(this);
        }
    }

}
