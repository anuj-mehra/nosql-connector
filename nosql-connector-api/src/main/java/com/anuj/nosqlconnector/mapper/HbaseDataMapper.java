package com.anuj.nosqlconnector.mapper;

import com.anuj.nosqlconnector.annotations.HBaseColumn;
import com.anuj.nosqlconnector.annotations.RowKey;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.model.HBColumn;
import com.anuj.nosqlconnector.model.HBTableRowMapping;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;

public interface HbaseDataMapper<T extends Serializable, I> extends DataMapper<I, HBTableRowMapping<T>> {

    default HBTableRowMapping<T> mapToTableRowMapping(final I input) throws HBaseDataIntegrationException{

        final HBTableRowMapping<T> table = new HBTableRowMapping<>();

        final Field[] fields = input.getClass().getDeclaredFields();

        final Collection<HBColumn> columns = new ArrayList<>();

        try{
            for(final Field field: fields){
                field.setAccessible(true);
                //Populate HBase row columns
                if(field.isAnnotationPresent(HBaseColumn.class)){
                    final HBColumn col = this.populateColumnData(input, field);
                    columns.add(col);
                }else if(field.isAnnotationPresent(RowKey.class)){
                    table.setRowKey((T)field.get(input));
                }
            }
            table.setColumns(columns);
            table.setTableName(this.fetchTableName(input));
        }catch(Exception e){
            throw new HBaseDataIntegrationException.HBaseDataIntegrationExceptionBuilder().errorMessage("Exception Occured while map To TableRowMapping object")
                    .throwable(e).build();
        }

        return table;
    }

}
