package com.anuj.nosqlconnector.dao.impl;

import com.anuj.nosqlconnector.dao.HBaseDDLDao;
import com.anuj.nosqlconnector.dao.dto.CreateHBaseTableDTO;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.RegionSplitter;

import java.io.IOException;

public class HBaseDDLDaoImpl implements HBaseDDLDao {

    private HBaseDDLDaoImpl(){}

    public static volatile HBaseDDLDaoImpl instance = new HBaseDDLDaoImpl();

    public static HBaseDDLDaoImpl getInstance(){
        return instance;
    }

    @Override
    public void createTable(CreateHBaseTableDTO createTableDTO) throws HBaseDaoException {
        logger().info("**** Method Entry - createTable ****" + createTableDTO.getTableName());

        final boolean isTableExist = this.isTableExist(createTableDTO.getTableName());

        if(!isTableExist){
            final Admin admin = this.getAdmin();

            try{
                final HTableDescriptor tableDescriptor = new HTableDescriptor(this.getTableName(createTableDTO.getTableName()));

                // Add Column Families
                if(!ArrayUtils.isEmpty(createTableDTO.getColumnFamilies())){
                    for(final String colFamily: createTableDTO.getColumnFamilies()){
                        final HColumnDescriptor cf = new HColumnDescriptor(colFamily);
                        tableDescriptor.addFamily(cf);
                    }
                }

                // Add number of default splits
                if(null != createTableDTO.getTotalSplits() && createTableDTO.getTotalSplits() > 0){
                    final byte[][] splits = new RegionSplitter.HexStringSplit().split(createTableDTO.getTotalSplits());
                    admin.createTable(tableDescriptor, splits);
                }else{
                    admin.createTable(tableDescriptor);
                }
                logger().info("**** Table created => " + createTableDTO.getTableName());
            }catch(Exception e){
                logger().error("Exception occured while creating hbase table: " + createTableDTO.getTableName());
                throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("Exception occured while creating hbase table : " + createTableDTO.getTableName())
                        .throwable(e).build();
            }
        }

        logger().info("**** Method Exit - createTable ****" + createTableDTO.getTableName());
    }

    @Override
    public void deleteTable(String tableName) throws HBaseDaoException {
        logger().info("**** Method Entry - deleteTable ****" + tableName);

        final Admin admin = this.getAdmin();
        try{
            final TableName table = this.getTableName(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
        }catch(IOException e){
            logger().error("Exception occured while deleting hbase table: " + tableName);
            throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("Exception occured while deleting hbase table : " + tableName)
                    .throwable(e).build();
        }
        logger().info("**** Method Exit - deleteTable ****" + tableName);
    }

    @Override
    public void backupTableAsSnapshot(String tableName, String snapshotName) throws HBaseDaoException {
        logger().info("**** Method Entry - backupTableAsSnapshot **** table Name:" + tableName + ": Snapshot Name:" + snapshotName);

        final Admin admin = this.getAdmin();
        try{
            final TableName table = this.getTableName(tableName);
            admin.disableTable(table);
            admin.snapshot(snapshotName, table);
            admin.cloneSnapshot(snapshotName, table);
            admin.deleteSnapshot(snapshotName);
            admin.enableTable(table);
        }catch(IOException e){
            logger().error("Exception occured while creating shapshot of hbase table: " + tableName);
            throw new HBaseDaoException.HBaseDaoExceptionBuilder().errorMessage("Exception occured while creating shapshot of hbase table: " + tableName)
                    .throwable(e).build();
        }
        logger().info("**** Method Exit - backupTableAsSnapshot **** table Name:" + tableName + ": Snapshot Name:" + snapshotName);
    }

    private TableName getTableName(final String tableName){
        return TableName.valueOf(tableName);
    }
}

