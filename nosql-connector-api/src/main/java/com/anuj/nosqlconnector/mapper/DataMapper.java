package com.anuj.nosqlconnector.mapper;

import com.anuj.nosqlconnector.annotations.HBaseColumn;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.logger.Loggable;
import com.anuj.nosqlconnector.model.HBColumn;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.utils.ObjectUtils;
import com.anuj.nosqlconnector.utils.Pair;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface DataMapper <I,U extends HBTableRowMapping<? extends Serializable>> extends Loggable {

    /**
     * <p>
     *     This method converts the Entity bean class object to {@link HBTableRowMapping} object
     * </p>
     * @param input
     * @return HBTableRowMapping converted response
     * @throws HBaseDataIntegrationException
     */
    HBTableRowMapping<? extends Serializable> mapToTableRowMapping(I input) throws HBaseDataIntegrationException;

    /**
     *
     * @param inputs
     * @return
     * @throws HBaseDataIntegrationException
     */
    default List<HBTableRowMapping<? extends Serializable>> mapToTableRowMapping(final List<I> inputs) throws HBaseDataIntegrationException{
        final List<HBTableRowMapping<? extends Serializable>> response = new ArrayList<>();
        for(I input: inputs){
            response.add(this.mapToTableRowMapping(input));
        }

        return response;
    }

    /**
     *
     * @param tableRowMappingRespList
     * @param className
     * @return
     * @throws HBaseDataIntegrationException
     */
    default List<I> mapListFromTableRowMappings(final List<HBTableRowMapping<? extends Serializable>> tableRowMappingRespList,
                                                final String className) throws HBaseDataIntegrationException{

        final List<I> response = new ArrayList<>();

        for(final HBTableRowMapping<? extends Serializable> tableRowMapping: tableRowMappingRespList){
            response.add(this.mapFromTableRowMapping(tableRowMapping, className));
        }

        return response;
    }


    default <I> I mapFromTableRowMapping(final HBTableRowMapping<? extends Serializable> tableRowMappingResp,
                                         final String className) throws HBaseDataIntegrationException{

        final I object = (I) ObjectUtils.createObject(className);
        final Field[] fields = object.getClass().getDeclaredFields();

        // Populate HBase row columns
        final Collection<HBColumn> columns = tableRowMappingResp.getColumns();

        try{
            for(final Field field: fields){
                field.setAccessible(true);

                if(field.isAnnotationPresent(HBaseColumn.class)){
                    // TODO: need to improve the in-line logic to better the performance

                    try{
                        for(final HBColumn columnData: columns){

                        }
                    }
                }
            }
        }

    }


    /**
     *
     * @param input
     * @param field
     * @return
     * @throws HBaseDataIntegrationException
     */
    default HBColumn populateColumnData(final I input, final Field field) throws HBaseDataIntegrationException{
        try{
            final HBaseColumn column = field.getAnnotation(HBaseColumn.class);
            HBColumn col = null;
            final Serializable value = (Serializable)field.get(input);
            if(null != value){
                col = new HBColumn(column.columnFamily(), new Pair<String, Serializable>(column.columnName(), value));
            }
            return col;
        }catch(IllegalAccessException | IllegalArgumentException e){
            throw new HBaseDataIntegrationException.HBaseDataIntegrationExceptionBuilder().errorMessage("Exception occured while populating HBase Column Data")
                    .throwable(e).build();
        }
    }

    default String populateColumnData(final I input) {

    }
}
