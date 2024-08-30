package com.anuj.nosqlconnector.dao.impl;

import com.anuj.nosqlconnector.dao.HBaseCRUDDao;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.utils.CustomBytes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.*;
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
                throw new HBaseDaoException.HBaseDaoExceptionBuilder()
                        .errorMessage("IOException occured while fetching data from HBase Table: " + tableName)
                        .throwable(e).build();
            }

        }
        return result;
    }

    @Override
    public List<Result> getDataRecordsOnRange(String tableName, String columnFamily, Serializable startKey, Serializable endKey, boolean isStartKeyInclusive, boolean isEndKeyInclusive, String... columns) throws HBaseDaoException {

        final List<Result> response = new ArrayList<>();
        final Table hTable = this.getTable(tableName);

        final Scan scan = new Scan();
        //scan.setCaching(10);
        if(!StringUtils.isEmpty(columnFamily)
            && !ArrayUtils.isEmpty(columns)){
            for(final String column: columns){
                scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            }
        }else if(!StringUtils.isEmpty(columnFamily)){
            scan.addFamily(Bytes.toBytes(columnFamily));
        }

        try{
            scan.setCacheBlocks(false);
            scan.withStartRow(CustomBytes.getBytes(startKey), isStartKeyInclusive);
            scan.withStopRow(CustomBytes.getBytes(endKey), isEndKeyInclusive);

            final ResultScanner scanner = hTable.getScanner(scan);
            scanner.forEach(a -> response.add(a));
        }catch(IOException e){
            throw new HBaseDaoException.HBaseDaoExceptionBuilder()
                    .errorMessage("IOException occured while fetching records on range from HBase Table: " + tableName)
                    .throwable(e).build();
        }

        return response;
    }

    @Override
    public void save(String tableName, Put objectToSave) throws HBaseDaoException {

        if(!StringUtils.isEmpty(tableName)
            && null != objectToSave){

            try{
                final Table hTable = this.getTable(tableName);
                hTable.put(objectToSave);
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
                final Table hTable = this.getTable(tableName);
                hTable.put(objectsToSave);
            }catch(final IOException e){
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while fetching data from HBase Table: " + tableName)
                        .throwable(e).build();
            }
        }
    }

    @Override
    public void delete(String tableName, Serializable key) throws HBaseDaoException {
        if(!StringUtils.isEmpty(tableName)
                && !StringUtils.isEmpty(key.toString())){

            try{
                final Delete d = new Delete(CustomBytes.getBytes(key));
                final Table hTable = this.getTable(tableName);
                hTable.delete(d);
            }catch(final IOException e){
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while deleting single rowkey from HBase Table: " + tableName)
                        .throwable(e).build();
            }
        }
    }

    @Override
    public void bulkDelete(String tableName, List<? extends Serializable> keys) throws HBaseDaoException {

        if(!StringUtils.isEmpty(tableName)
                && !CollectionUtils.isEmpty(keys)){

            try{
                final List<Delete> deletes = new ArrayList<>(keys.size());
                for(final Serializable key: keys){
                    deletes.add(new Delete(CustomBytes.getBytes(key)));
                }
                final Table hTable = this.getTable(tableName);
                hTable.delete(deletes);
            }catch(final IOException e){
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while deleting bulk rowkeys from HBase Table: " + tableName)
                        .throwable(e).build();
            }
        }

    }

    @Override
    public void deleteDataRecordsOnRange(String tableName, Serializable startKey, Serializable endKey, boolean inclusive) throws HBaseDaoException {

        final List<Delete> deleteList = new ArrayList<>();

        if(startKey instanceof  Long){
            final Long startRange = (Long)startKey;
            final Long endRange = (Long)endKey;

            for(Long rowKey=startRange;rowKey<endRange;rowKey++){
                try{
                    deleteList.add(new Delete(CustomBytes.toBytes(rowKey)));
                }catch(Exception e){
                    throw new HBaseDaoException.HBaseDaoExceptionBuilder()
                            .errorMessage("Exception occured while deleting records on range from HBase Table: " + tableName)
                            .throwable(e).build();
                }
            }
        }else if(startKey instanceof  Double){
            final Double startRange = (Double)startKey;
            final Double endRange = (Double)endKey;

            for(Double rowKey=startRange;rowKey<endRange;rowKey++){
                try{
                    deleteList.add(new Delete(CustomBytes.toBytes(rowKey)));
                }catch(Exception e){
                    throw new HBaseDaoException.HBaseDaoExceptionBuilder()
                            .errorMessage("Exception occured while deleting records on range from HBase Table: " + tableName)
                            .throwable(e).build();
                }
            }
        }else if(startKey instanceof  Integer){
            final Integer startRange = (Integer)startKey;
            final Integer endRange = (Integer)endKey;

            for(Integer rowKey=startRange;rowKey<endRange;rowKey++){
                try{
                    deleteList.add(new Delete(CustomBytes.toBytes(rowKey)));
                }catch(Exception e){
                    throw new HBaseDaoException.HBaseDaoExceptionBuilder()
                            .errorMessage("Exception occured while deleting records on range from HBase Table: " + tableName)
                            .throwable(e).build();
                }
            }
        } else if(startKey instanceof  Float){
            final Float startRange = (Float)startKey;
            final Float endRange = (Float)endKey;

            for(Float rowKey=startRange;rowKey<endRange;rowKey++){
                try{
                    deleteList.add(new Delete(CustomBytes.toBytes(rowKey)));
                }catch(Exception e){
                    throw new HBaseDaoException.HBaseDaoExceptionBuilder()
                            .errorMessage("Exception occured while deleting records on range from HBase Table: " + tableName)
                            .throwable(e).build();
                }
            }
        }

        try{
            if(inclusive){
                deleteList.add(new Delete(CustomBytes.toBytes(endKey)));
            }

            final Table table = this.getTable(tableName);
            table.delete(deleteList);
        }catch(IOException e){
            throw new HBaseDaoException.HBaseDaoExceptionBuilder()
                    .errorMessage("Exception occured while deleting records on range from HBase Table: " + tableName)
                    .throwable(e).build();
        }
    }

    @Override
    public List<Result> scan(String tableName, String columnFamily, FilterList filterList, String... columns) throws HBaseDaoException {
        final List<Result> response = new ArrayList<>();
        final Table table = this.getTable(tableName);

        final Scan scan = this.getFullScan();

        if(!StringUtils.isEmpty(columnFamily)
            && !ArrayUtils.isEmpty(columns)){
            for(final String column: columns){
                scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            }
        }else if(!StringUtils.isEmpty(columnFamily)){
            scan.addFamily(Bytes.toBytes(columnFamily));
        }

        if(null != filterList){
            scan.setFilter(filterList);
        }

        try{
            final ResultScanner scanner = table.getScanner(scan);
            scanner.forEach(a -> response.add(a));
        }catch(final IOException e){
            throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("IOException occured while execution scan on HBase Table: " + tableName)
                    .throwable(e).build();
        }

        return response;
    }

    private Scan getFullScan(){
        final Scan scan = new Scan();
        scan.setCacheBlocks(false);
        return scan;
    }
}
