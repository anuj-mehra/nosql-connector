package com.anuj.nosqlconnector.service.hbase;

import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.service.HBaseDataIntegrationService;
import com.anuj.nosqlconnector.utils.HBaseRowKeyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.Serializable;
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
        return null;
    }

    @Override
    public List<Put> convertToPutObject(List<? extends HBTableRowMapping<? extends Serializable>> tableRowMappings) throws HBaseDataIntegrationException {
        return null;
    }

    @Override
    public HBTableRowMapping<? extends Serializable> convertFromResultObject(Result result, HBaseRowKeyType hbaseRowKeyType) throws HBaseDataIntegrationException {
        return null;
    }

    @Override
    public List<HBTableRowMapping<? extends Serializable>> convertFromResultObjects(List<Result> results, HBaseRowKeyType hbaseRowKeyType) throws HBaseDataIntegrationException {
        return null;
    }
}
