package com.anuj.nosqlconnector.service.hbase;

import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.model.HBColumn;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.service.HBaseDataIntegrationService;
import com.anuj.nosqlconnector.utils.CustomBytes;
import com.anuj.nosqlconnector.utils.HBaseRowKeyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  Service layer to provide methods to convert the <code>DataMapper</code> object to the {@link Put} object,
 *  that will be persisted into the HBase table.
 *
 *  This class is concrete implementation of interface {@link HBaseDataIntegrationService}
 * </p>
 */
public class HBaseDataIntegrationServiceImpl implements HBaseDataIntegrationService {

    /**
     * Private constructor
     */
    private HBaseDataIntegrationServiceImpl(){

    }

    /**
     * Singleton instance. Eager initialization.
     */
    private static volatile HBaseDataIntegrationServiceImpl instance = new HBaseDataIntegrationServiceImpl();

    /**
     * Static factory method to return the instance of the class.
     * @return
     */
    public static HBaseDataIntegrationServiceImpl getInstance(){
        return instance;
    }

    @Override
    public Put convertToPutObject(HBTableRowMapping<? extends Serializable> tableRowMapping) throws HBaseDataIntegrationException {

        Put put  = null;

        try{
            put = new Put(CustomBytes.getBytes(tableRowMapping.getRowKey()));

            for(final HBColumn hbColumn: tableRowMapping.getColumns()){
                if(null != hbColumn){
                    put.addColumn(hbColumn.getColumnFamily().getBytes(),
                            hbColumn.getColumnNameValue().getKey().getBytes(),
                            CustomBytes.getBytes(hbColumn.getColumnNameValue().getValue()));
                }
            }
        }catch(final IOException e){
            throw new HBaseDataIntegrationException.HBaseDataIntegrationExceptionBuilder().errorMessage("IOException occured while converting HBTableRowMapping to Put object")
                    .throwable(e).build();
        }

        return put;
    }

    @Override
    public List<Put> convertToPutObjects(List<? extends HBTableRowMapping<? extends Serializable>> tableRowMappings) throws HBaseDataIntegrationException {
        final List<Put> response = new ArrayList<>(tableRowMappings.size());
        for(final HBTableRowMapping<? extends Serializable> mapping: tableRowMappings){
            response.add(this.convertToPutObject(mapping));
        }
        return response;
    }

    @Override
    public HBTableRowMapping<? extends Serializable> convertFromResultObject(Result result, HBaseRowKeyType hbaseRowKeyType) throws HBaseDataIntegrationException {
        
        final byte[] rowkey = result.getRow();
        Object rowKeyObj = null;
        
        try{
            rowKeyObj = CustomBytes.toObject(rowkey, hbaseRowKeyType);
        }catch(ClassNotFoundException | IOException e){
            throw new HBaseDataIntegrationException.HBaseDataIntegrationExceptionBuilder().errorMessage("IOException occured while converting Result to HBTableRowMapping")
                    .throwable(e).build();
        }

        if(rowKeyObj instanceof String){
            final HBTableRowMapping<String> tableRowMapping = new HBTableRowMapping<>();
            tableRowMapping.setRowKey((String)rowKeyObj);
            HBaseDataIntegrationHelper.populateColumnsData(tableRowMapping, result);
            return tableRowMapping;
        }else if(rowKeyObj instanceof Long){
            final HBTableRowMapping<Long> tableRowMapping = new HBTableRowMapping<>();
            tableRowMapping.setRowKey((Long)rowKeyObj);
            HBaseDataIntegrationHelper.populateColumnsData(tableRowMapping, result);
            return tableRowMapping;
        }else if(rowKeyObj instanceof Integer){
            final HBTableRowMapping<Integer> tableRowMapping = new HBTableRowMapping<>();
            tableRowMapping.setRowKey((Integer)rowKeyObj);
            HBaseDataIntegrationHelper.populateColumnsData(tableRowMapping, result);
            return tableRowMapping;
        }else if(rowKeyObj instanceof Double){
            final HBTableRowMapping<Double> tableRowMapping = new HBTableRowMapping<>();
            tableRowMapping.setRowKey((Double)rowKeyObj);
            HBaseDataIntegrationHelper.populateColumnsData(tableRowMapping, result);
            return tableRowMapping;
        }else if(rowKeyObj instanceof Float){
            final HBTableRowMapping<Float> tableRowMapping = new HBTableRowMapping<>();
            tableRowMapping.setRowKey((Float)rowKeyObj);
            HBaseDataIntegrationHelper.populateColumnsData(tableRowMapping, result);
            return tableRowMapping;
        }else if(rowKeyObj instanceof Serializable){
            // TODO:: need to see what should be done here.
        }
        return null;
    }

    @Override
    public List<HBTableRowMapping<? extends Serializable>> convertFromResultObjects(List<Result> results, HBaseRowKeyType hbaseRowKeyType) throws HBaseDataIntegrationException {
        final List<HBTableRowMapping<? extends Serializable>> response = new ArrayList<>();
        for(final Result result: results){
            response.add(this.convertFromResultObject(result, hbaseRowKeyType));
        }
        return response;
    }
}
