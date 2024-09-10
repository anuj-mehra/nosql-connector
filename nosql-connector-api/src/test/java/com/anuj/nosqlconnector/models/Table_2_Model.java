package com.anuj.nosqlconnector.models;

// HBase table name is read from the properties file.

import com.anuj.nosqlconnector.annotations.HBaseColumn;
import com.anuj.nosqlconnector.annotations.HBaseEntity;
import com.anuj.nosqlconnector.annotations.RowKey;
import com.anuj.nosqlconnector.annotations.TableName;

import java.io.Serializable;

@HBaseEntity
@TableName(name = "My_HBase_Table_2")
public class Table_2_Model implements Serializable {

    private static final long serialVersionUID = 1L;

    public Table_2_Model(){
    }

    public Table_2_Model(Double rowkey){
        this.rowkey = rowkey;
    }

    @RowKey
    private Double rowkey;

    @HBaseColumn(columnFamily = "cf", columnName = "fn")
    private String firstName;

    @HBaseColumn(columnFamily = "cf", columnName = "ln")
    private String lastName;

    public Double getRowkey() {
        return rowkey;
    }

    public void setRowkey(Double rowkey) {
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
