package com.anuj.nosqlconnector.dao;

import com.anuj.nosqlconnector.connection.HBaseConnectionFactory;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.logger.Loggable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.TableName;


import java.io.IOException;
import java.io.Serializable;

/**
 * <p>
 *     DAO contract for HBase Database.
 *     In general this layer provdes Cluster Administration API's.
 * </p>
 */
public interface HBaseDao extends Serializable, Loggable {

    /**
     * <p>
     *     Get the HBase table reference.
     * </p>
     * @param tableName
     * @return
     * @throws HBaseDaoException
     */
    default Table getTable(final String tableName) throws HBaseDaoException {
        final TableName table = TableName.valueOf(tableName);
        final Connection connection = HBaseConnectionFactory.getConnection();
        try{
            return connection.getTable(table);
        }catch(IOException e){
            logger().info("Exception occured while fetching HBase table reference: " + tableName);
            throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("Exception occured while fetching HBase table reference: " + tableName)
                    .throwable(e).build();
        }
    }

    default boolean isTableExist(final String tableName) throws HBaseDaoException {
        logger().info("****Method Entry - isTableExist ****  tableName => " + tableName);
        boolean isExist = false;

        if(!StringUtils.isEmpty(tableName)){
            final Admin admin = this.getAdmin();
            try{
                final TableName[] tables = admin.listTableNames();
                for(final TableName tbl: tables){
                    if(tbl.getNameAsString().equalsIgnoreCase(tableName)){
                        isExist = true;
                        break;
                    }
                }
            }catch(IOException e){
                logger().info("Exception occured while finding if table exists in hbase: " + tableName);
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("Exception occured while finding if table exists in hbase: " + tableName)
                        .throwable(e).build();
            }
        }
        logger().info("****Method Exit - isTableExist ****  tableName => " + tableName);
        return isExist;
    }

    default Admin getAdmin(){
        return HBaseConnectionFactory.getAdmin();
    }
}

