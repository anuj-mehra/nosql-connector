package com.anuj.nosqlconnector.service.hbase;

import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.logger.Loggable;
import com.anuj.nosqlconnector.model.HBColumn;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.Serializable;

public class HBaseDataIntegrationHelper implements Loggable {

    public static Logger LOGGER = Logger.getLogger(HBaseDataIntegrationHelper.class);

    /**
     * <p>
     *     This method populates the data received in {@link Result} object from HBase into the {@link HBTableRowMapping} object.
     * </p>
     * @param tableRowMapping
     * @param result
     * @throws HBaseDataIntegrationException
     */
    public static void populateColumnsData(final HBTableRowMapping<? extends Serializable> tableRowMapping, final Result result) throws HBaseDataIntegrationException{

        tableRowMapping.createCollection();

        for(final Cell cell: result.rawCells()){
            final HBColumn hbColumn = new HBColumn(Bytes.toString(CellUtil.cloneFamily(cell)),
                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                    CellUtil.cloneValue(cell));

            tableRowMapping.addColumnToCollection(hbColumn);
        }
    }
}
