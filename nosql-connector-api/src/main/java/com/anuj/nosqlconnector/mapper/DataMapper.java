package com.anuj.nosqlconnector.mapper;

import com.anuj.nosqlconnector.annotations.HBaseColumn;
import com.anuj.nosqlconnector.annotations.RowKey;
import com.anuj.nosqlconnector.annotations.TableName;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.logger.Loggable;
import com.anuj.nosqlconnector.model.HBColumn;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.utils.CustomBytes;
import com.anuj.nosqlconnector.utils.ObjectUtils;
import com.anuj.nosqlconnector.utils.Pair;
import com.anuj.nosqlconnector.utils.PropertiesCache;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.beans.PropertyDescriptor;

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
                            final HBaseColumn column = field.getAnnotation(HBaseColumn.class);

                            final Serializable fieldValue = (Serializable)field.get(object);
                            if(null == fieldValue
                            && column.columnName().equals(columnData.getColumnFamily())
                            && column.columnName().equals(columnData.getColumnNameValue().getKey())){

                                final Pair<String, ? extends Serializable> pair = columnData.getColumnNameValue();
                                final PropertyDescriptor pd = new PropertyDescriptor(field.getName(), object.getClass());

                                if(field.getType().equals(String.class)){
                                    pd.getWriteMethod().invoke(object, Bytes.toString((byte[])pair.getValue()));
                                }else if(field.getType().equals(Long.class)){
                                    pd.getWriteMethod().invoke(object, Bytes.toLong((byte[])pair.getValue()));
                                }else if(field.getType().equals(Integer.class)){
                                    pd.getWriteMethod().invoke(object, Bytes.toInt((byte[])pair.getValue()));
                                }else if(field.getType().equals(Double.class)){
                                    pd.getWriteMethod().invoke(object, Bytes.toDouble((byte[])pair.getValue()));
                                }else{
                                    pd.getWriteMethod().invoke(object, CustomBytes.toObject((byte[])pair.getValue()));
                                }
                                break;
                            }
                        }
                    }catch(final Exception e){
                        throw new HBaseDataIntegrationException.HBaseDataIntegrationExceptionBuilder().errorMessage("Exception Occured while populating HBase Entity bean Object")
                                .throwable(e).build();
                    }
                }else if(field.isAnnotationPresent(RowKey.class)){
                    field.set(object, tableRowMappingResp.getRowKey());
                }
            }
        }catch(IllegalArgumentException | IllegalAccessException e){
            throw new HBaseDataIntegrationException.HBaseDataIntegrationExceptionBuilder().errorMessage("Exception Occured while mapping TableRowMapping bean object to HBase Entity bean Object")
                    .throwable(e).build();
        }
        return object;
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

    /**
     * <p>
     *     Method to fetch hbase table name from the HBase Entity bean object.
     * </p>
     * @param input HBase Entity Bean Object
     * @return name HBase table name
     */
    default String fetchTableName(final I input) {
        String response = null;
        final Annotation[] annotations = input.getClass().getAnnotations();
        for(final Annotation annotation: annotations){
            if(annotation instanceof TableName){
                final TableName tableName = (TableName)annotation;
                response = PropertiesCache.getInstance().getProperty(tableName.name());
                break;
            }
        }
        return response;
    }

}
