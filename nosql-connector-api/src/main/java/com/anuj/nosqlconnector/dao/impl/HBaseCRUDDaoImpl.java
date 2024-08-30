package com.anuj.nosqlconnector.dao.impl;

import com.anuj.nosqlconnector.dao.HBaseCRUDDao;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.utils.CustomBytes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class HBaseCRUDDaoImpl implements HBaseCRUDDao<Result, Put> {

    private HBaseCRUDDaoImpl(){}

    private static volatile HBaseCRUDDaoImpl instance = new HBaseCRUDDaoImpl();

    public static HBaseCRUDDaoImpl getInstance(){
        return instance;
    }

    @Override
    public Result getDataRecord(String tableName, String columnFamily, Serializable key, String... columns) throws HBaseDaoException {
        Result result = null;

        if(!StringUtils.isEmpty(tableName)
                && !StringUtils.isEmpty(String.valueOf(key))){
            final Table hTable = this.getTable(tableName);
            try{
                final Get get = new Get(CustomBytes.getBytes(key));
                if(!StringUtils.isEmpty(columnFamily)
                    && !ArrayUtils.isEmpty(columns)){

                    for(final String column: columns){
                        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                    }
                }else if(!StringUtils.isEmpty(columnFamily)){
                    get.addFamily(Bytes.toBytes(columnFamily));
                }

                result = hTable.get(get);
            }catch(final IOException e){
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while fetching data from HBase Table: " + tableName)
                        .throwable(e).build();
            }
        }

        return result;
    }

    @Override
    public List<Result> getBulkDataRecords(String tableName, String columnFamily, List<? extends Serializable> keys, String... columns) throws HBaseDaoException {
        List<Result> result = null;

        if(!StringUtils.isEmpty(tableName)
                && !CollectionUtils.isEmpty(keys)){

            final Table hTable = this.getTable(tableName);

            final List<Get> getList = new ArrayList<>(keys.size());
            try{

                for(final Serializable key: keys){
                    if(!StringUtils.isEmpty(key.toString())){
                        final Get get = new Get(CustomBytes.getBytes(key));
                        if(!StringUtils.isEmpty(columnFamily)
                                && !ArrayUtils.isEmpty(columns)){

                            for(final String column: columns){
                                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                            }
                        }else if(!StringUtils.isEmpty(columnFamily)){
                            get.addFamily(Bytes.toBytes(columnFamily));
                        }
                        getList.add(get);
                    }
                }

                final Result[] results = hTable.get(getList);
                if(!ArrayUtils.isEmpty(results)){
                    result = Arrays.asList(results);
                }
            }catch(final IOException e){
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while fetching data from HBase Table: " + tableName)
                        .throwable(e).build();
            }

        }
        return result;
    }

    @Override
    public List<Result> getDataRecordsOnRange(String tableName, String columnFamily, Serializable startKey, Serializable endKey, boolean isStartKeyInclusive, boolean isEndKeyInclusive, String... columns) throws HBaseDaoException {
        return null;
    }

    @Override
    public void save(String tableName, Put objectToSave) throws HBaseDaoException {

        if(!StringUtils.isEmpty(tableName)
            && null != objectToSave){

            try{
                final Table table = this.getTable(tableName);
                table.put(objectToSave);
            }catch(final IOException e){
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while fetching data from HBase Table: " + tableName)
                        .throwable(e).build();
            }
        }
    }

    @Override
    public void save(String tableName, List<Put> objectsToSave) throws HBaseDaoException {
        if(!StringUtils.isEmpty(tableName)
                && !CollectionUtils.isEmpty(objectsToSave)){

            try{
                final Table table = this.getTable(tableName);
                table.put(objectsToSave);
            }catch(final IOException e){
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while fetching data from HBase Table: " + tableName)
                        .throwable(e).build();
            }
        }
    }

    @Override
    public void delete(String tableName, Serializable key) throws HBaseDaoException {

    }

    @Override
    public void bulkDelete(String tableName, List<? extends Serializable> keys) throws HBaseDaoException {

    }

    @Override
    public void deleteDataReconbrdsOnRange(String tableName, Serializable startKey, Serializable endKey, boolean inclusive) throws HBaseDaoException {

    }

    @Override
    public List<Result> scan(String tableName, String columnFamily, FilterList filterList, String... columns) throws HBaseDaoException {
        return null;
    }
}
