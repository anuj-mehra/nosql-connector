package com.anuj.nosqlconnector.dao;

import com.anuj.nosqlconnector.exception.HBaseDaoException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *
 * </p>
 */
public interface HBaseCRUDDao<T,Q> extends HBaseDao {

    T getDataRecord(final String tableName, final String columnFamily, final Serializable key, final String... columns) throws HBaseDaoException;

    List<T> getBulkDataRecords(final String tableName, final String columnFamily, final List<? extends Serializable> keys, final String... columns) throws HBaseDaoException;

    List<Result> getDataRecordsOnRange(final String tableName, final String columnFamily, final Serializable startKey,
                                       final Serializable endKey, final boolean isStartKeyInclusive, final boolean isEndKeyInclusive,
                                       final String... columns) throws HBaseDaoException;

    void save(final String tableName, final Q objectToSave) throws HBaseDaoException;

    void save(final String tableName, final List<Q> objectsToSave) throws HBaseDaoException;

    void delete(final String tableName, final Serializable key) throws HBaseDaoException;

    void bulkDelete(final String tableName, final List<? extends Serializable> keys) throws HBaseDaoException;

    void deleteDataReconbrdsOnRange(final String tableName, final Serializable startKey,
                                    final Serializable endKey, final boolean inclusive) throws HBaseDaoException;

    List<Result> scan(final String tableName, final String columnFamily, final FilterList filterList, final String... columns) throws HBaseDaoException;

}
