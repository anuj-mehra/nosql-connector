package com.anuj.nosqlconnector.models;

// HBase table name is read from the properties file.

import com.anuj.nosqlconnector.annotations.HBaseColumn;
import com.anuj.nosqlconnector.annotations.HBaseEntity;
import com.anuj.nosqlconnector.annotations.RowKey;
import com.anuj.nosqlconnector.annotations.TableName;

@HBaseEntity
@TableName(name = "My_HBase_Table_1")
public class Table_1_Model {


    private static final long serialVersionUID = 1L;

    @RowKey
    private String rowkey;

    @HBaseColumn(columnFamily = "cf", columnName = "fn")
    private String firstName;

    @HBaseColumn(columnFamily = "cf", columnName = "ln")
    private String lastName;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
}
