package com.anuj.nosqlconnector.model;

import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * <p>
 *     Model class to store the values/records that needs to be stored in the HBase table mapped by the <code>tableName</code>.
 * </p>
 * @param <T>
 */
public class HBTableRowMapping<T extends Serializable> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Parameter to hold the name of the hbase table.
     */
    private String tableName;

    /**
     * Parameter to hold the rowkey.
     */
    private T rowKey;

    /**
     * Collection of column-famiy, column-name and column-value.
     */
    private Collection<HBColumn> columns;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public T getRowKey() {
        return rowKey;
    }

    public void setRowKey(T rowKey) {
        this.rowKey = rowKey;
    }

    public Collection<HBColumn> getColumns() {
        return columns;
    }

    public void setColumns(Collection<HBColumn> columns) {
        this.columns = columns;
    }

    public void addColumnToCollection(final HBColumn column){
        columns.add(column);
    }

    public void addColumn(final HBColumn column){
        if(CollectionUtils.isEmpty(this.columns)){
            this.columns = new ArrayList<>();
        }
        columns.add(column);
    }

    public void createCollection(){
        this.columns = new ArrayList<>();
    }
}
