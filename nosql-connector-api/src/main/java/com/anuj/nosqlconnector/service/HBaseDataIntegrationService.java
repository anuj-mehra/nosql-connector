package com.anuj.nosqlconnector.service;

import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.utils.HBaseRowKeyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *     Service layer to provide methods to convert the <code>DataMapper</code> objects to the {@link Put} object
 *     that will be persisted into the hbase table.
 * </p>
 */
public interface HBaseDataIntegrationService {

    /**
     * <p>
     *  Method converts the {@link HBTableRowMapping} object received as input into the corresponding {@link Put} object.
     *  This {@link Put} object will be saved into the HBase Table.
     * </p>
     *
     * @param tableRowMapping object containing the data that needs to be inserted into the HBase Table.
     * @return put object to be saved into the hbase table.
     * @throws HBaseDataIntegrationException
     */
    Put convertToPutObject(final HBTableRowMapping<? extends Serializable> tableRowMapping) throws HBaseDataIntegrationException;

    /**
     * <p>
     *  Method converts the {@link HBTableRowMapping} objects received as input into the corresponding {@link Put} object.
     *  This {@link Put} object will be saved into the HBase Table.
     * </p>
     *
     * @param tableRowMappings object containing the data that needs to be inserted into the HBase Table.
     * @return put object to be saved into the hbase table.
     * @throws HBaseDataIntegrationException
     */
    List<Put> convertToPutObjects(final List<? extends HBTableRowMapping<? extends Serializable>> tableRowMappings) throws HBaseDataIntegrationException;

    /**
     * <p>
     *  Method converts the {@link Result} object received from HBase into the corresponding {@link HBTableRowMapping} object.
     *  This {@link Put} object will be saved into the HBase Table.
     * </p>
     * @param result
     * @param hbaseRowKeyType
     * @return
     * @throws HBaseDataIntegrationException
     */
    HBTableRowMapping<? extends Serializable> convertFromResultObject(final Result result,
                                                                      final HBaseRowKeyType hbaseRowKeyType) throws HBaseDataIntegrationException;


    /**
     * <p>
     *  Method converts the {@link Result} objects received from HBase into the corresponding list of {@link HBTableRowMapping} objects.
     *  This {@link Put} object will be saved into the HBase Table.
     * </p>
     * @param results
     * @param hbaseRowKeyType
     * @return
     * @throws HBaseDataIntegrationException
     */
    List<HBTableRowMapping<? extends Serializable>> convertFromResultObjects(final List<Result> results,
                                                                            final HBaseRowKeyType hbaseRowKeyType) throws HBaseDataIntegrationException;


}
