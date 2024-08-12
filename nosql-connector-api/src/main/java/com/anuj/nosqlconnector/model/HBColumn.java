package com.anuj.nosqlconnector.model;

import com.anuj.nosqlconnector.utils.Pair;

import java.io.Serializable;

public class HBColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    private String columnFamily;

    /**
     * Pair for ColumnName-ColumnValue
     */
    private Pair<String , ? extends Serializable> columnNameValue;

    public HBColumn(final String columnFamily, final Pair<String, ? extends Serializable> columnNameValue){
        this.columnFamily = columnFamily;
        this.columnNameValue = columnNameValue;
    }

    public HBColumn(final String columnFamily, final String columnName, final byte[] columnValue){
        this.columnFamily = columnFamily;
        this.columnNameValue = new Pair<String, byte[]>(columnName, columnValue);
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public Pair<String, ? extends Serializable> getColumnNameValue() {
        return columnNameValue;
    }

    public void setColumnNameValue(Pair<String, ? extends Serializable> columnNameValue) {
        this.columnNameValue = columnNameValue;
    }
}
