package com.anuj.nosqlconnector.facade;

import com.anuj.nosqlconnector.dao.HBaseCRUDDao;
import com.anuj.nosqlconnector.dao.HBaseDDLDao;
import com.anuj.nosqlconnector.dao.dto.*;
import com.anuj.nosqlconnector.dao.impl.HBaseCRUDDaoImpl;
import com.anuj.nosqlconnector.dao.impl.HBaseDDLDaoImpl;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.logger.Loggable;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.service.HBaseDataIntegrationService;
import com.anuj.nosqlconnector.service.hbase.HBaseDataIntegrationServiceImpl;
import com.anuj.nosqlconnector.utils.PropertiesCache;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.Serializable;
import java.util.List;

public final class HBaseFacade implements Loggable, Serializable {

    private static final HBaseCRUDDao<Result, Put> hbaseCRUDDao = HBaseCRUDDaoImpl.getInstance();
    private static final HBaseDDLDao hbaseDDLDao = HBaseDDLDaoImpl.getInstance();
    private static final HBaseDataIntegrationService hbaseDataIntegrationService = HBaseDataIntegrationServiceImpl.getInstance();

    private HBaseFacade(){}

    private static volatile HBaseFacade instance = new HBaseFacade();

    public static HBaseFacade getInstance(String propertyFileFullyQualifiedPath){
        //Load Property File
        PropertiesCache.loadProperties(propertyFileFullyQualifiedPath);
        return instance;
    }

    public boolean isTableExist(String tableName) throws HBaseDaoException{
        final boolean isExist = hbaseCRUDDao.isTableExist(tableName);
        return isExist;
    }

    public void createTable(final CreateHBaseTableDTO createHBaseTableDTO) throws HBaseDaoException{
        hbaseDDLDao.createTable(createHBaseTableDTO);
    }

    public void deleteTable(final String tableName) throws HBaseDaoException{
        hbaseDDLDao.deleteTable(tableName);
    }

    public Result getDataRecord(final GetHBaseRecordDTO getHBaseRecordDTO) throws HBaseDaoException{
        final String tableName = getHBaseRecordDTO.getTableName();
        final String columnFamily = getHBaseRecordDTO.getColumnFamily();
        final Serializable hbaseRowKey = getHBaseRecordDTO.getRowKey();
        final String[] columnsToBeFetched = getHBaseRecordDTO.getColumnsToBeFetched();

        return hbaseCRUDDao.getDataRecord(tableName,columnFamily,hbaseRowKey,columnsToBeFetched);
    }

    public List<Result> getBulkDataRecords(final GetHBaseRecordsDTO getHBaseRecordsDTO) throws HBaseDaoException{
        final String tableName = getHBaseRecordsDTO.getTableName();
        final String columnFamily = getHBaseRecordsDTO.getColumnFamily();
        final List<? extends Serializable> hbaseRowKeys = getHBaseRecordsDTO.getRowKeys();
        final String[] columnsToBeFetched = getHBaseRecordsDTO.getColumnsToBeFetched();

        return hbaseCRUDDao.getBulkDataRecords(tableName,columnFamily,hbaseRowKeys,columnsToBeFetched);
    }

    public List<Result> getDataRecordsOnRange(final GetHBaseRecordsOnRangeDTO getHBaseRecordsOnRangeDTO) throws HBaseDaoException{
        final String tableName = getHBaseRecordsOnRangeDTO.getTableName();
        final String columnFamily = getHBaseRecordsOnRangeDTO.getColumnFamily();
        final String[] columnsToBeFetched = getHBaseRecordsOnRangeDTO.getColumnsToBeFetched();

        final Serializable startKey = getHBaseRecordsOnRangeDTO.getStartKey();
        final Serializable endKey = getHBaseRecordsOnRangeDTO.getEndKey();
        final boolean startKeyInclusive = getHBaseRecordsOnRangeDTO.isStartKeyInclusive();
        final boolean endKeyInclusive = getHBaseRecordsOnRangeDTO.isEndKeyInclusive();

        return hbaseCRUDDao.getDataRecordsOnRange(tableName,columnFamily,startKey,endKey,startKeyInclusive,endKeyInclusive,columnsToBeFetched);
    }

    public void save(final String tableName, final HBTableRowMapping<? extends Serializable> tableRowMapping)
            throws HBaseDaoException, HBaseDataIntegrationException {
        final Put putObject = hbaseDataIntegrationService.convertToPutObject(tableRowMapping);
        hbaseCRUDDao.save(tableName, putObject);
    }

    public void save(final String tableName, final List<? extends HBTableRowMapping<? extends Serializable>> tableRowMappings)
            throws HBaseDaoException, HBaseDataIntegrationException{
        final List<Put> putObjects = hbaseDataIntegrationService.convertToPutObjects(tableRowMappings);
        hbaseCRUDDao.save(tableName, putObjects);
    }

    public void delete(final String tableName, final Serializable key) throws HBaseDaoException{
        hbaseCRUDDao.delete(tableName, key);
    }

    public void bulkDelete(final String tableName, final List<? extends Serializable> keys) throws HBaseDaoException{
        hbaseCRUDDao.bulkDelete(tableName, keys);
    }

    public void deleteDataRecordsOnRange(final String tableName, final Serializable startKey, final Serializable endKey,
                                         final boolean inclusive) throws HBaseDaoException{
        hbaseCRUDDao.deleteDataRecordsOnRange(tableName, startKey, endKey, inclusive);
    }

    public List<Result> scan(final ScanHBaseTableDTO scanHBaseTableDTO) throws HBaseDaoException{
        final String tableName = scanHBaseTableDTO.getTableName();
        final String columnFamily = scanHBaseTableDTO.getColumnFamily();
        final String[] columnsToBeFetched = scanHBaseTableDTO.getColumnsToBeFetched();
        final FilterList filterList = scanHBaseTableDTO.getFilterList();

        return hbaseCRUDDao.scan(tableName, columnFamily, filterList, columnsToBeFetched);
    }

}
