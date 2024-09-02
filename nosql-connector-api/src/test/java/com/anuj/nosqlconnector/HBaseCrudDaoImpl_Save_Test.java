package com.anuj.nosqlconnector;

import static org.junit.Assert.fail;
import com.anuj.nosqlconnector.dao.dto.CreateHBaseTableDTO;
import com.anuj.nosqlconnector.dao.dto.GetHBaseRecordDTO;
import com.anuj.nosqlconnector.dao.dto.GetHBaseRecordsDTO;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.facade.HBaseFacade;
import com.anuj.nosqlconnector.mappers.Table_1_Mapper;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.models.Table_1_Model;
import com.anuj.nosqlconnector.service.HBaseDataIntegrationService;
import com.anuj.nosqlconnector.service.hbase.HBaseDataIntegrationServiceImpl;
import com.anuj.nosqlconnector.utils.HBaseRowKeyType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class HBaseCrudDaoImpl_Save_Test {

    private String projectDirPath = Paths.get("").toAbsolutePath().toString();

    private HBaseFacade hbaseFacade = HBaseFacade.getInstance(projectDirPath.concat("\\src\\test\\resources\\hbase-table-name.properties"));

    private HBaseDataIntegrationService  hbaseDataIntegrationService = HBaseDataIntegrationServiceImpl.getInstance();
    private Table_1_Mapper mapper = new Table_1_Mapper();

    @Before
    public void setUp(){

    }

    @Test
    public void testSave(){

        try{
            String[] columnFamilies = new String[]{"cf"};
            CreateHBaseTableDTO createHBaseTableDTO =
                    new CreateHBaseTableDTO.CreateTableBuilder("cpbnd:My_HBase_Table_1", columnFamilies)
                    .tableCompressionAlgo(Compression.Algorithm.SNAPPY)
                    .totalSplits(10).build();
            //hbaseFacade.createTable(createHBaseTableDTO);


            final Table_1_Model model = new Table_1_Model("rowkey1");
            model.setFirstName("firstname");
            model.setLastName("lastName");

            final HBTableRowMapping<String> tableRowMapping = mapper.mapToTableRowMapping(model);
            hbaseFacade.save(tableRowMapping.getTableName(), tableRowMapping);

            //Fetch Result from hbase
            //final Table_1_Model fetchDataModel = new Table_1_Model("rowkey1");
            //final HBTableRowMapping<String> fetchTableMapping = mapper.mapToTableRowMapping(model);
            final GetHBaseRecordDTO getHBaseRecordDTO =
                    new GetHBaseRecordDTO.GetHBaseRecordBuilder("cpbnd:My_HBase_Table_1", "rowkey1")
                    .columnFamily("cf")
                    .build();
            final Result result = hbaseFacade.getDataRecord(getHBaseRecordDTO);
            final HBTableRowMapping<? extends Serializable> resp =
                    hbaseDataIntegrationService.convertFromResultObject(result, HBaseRowKeyType.STRING);
            final Table_1_Model tableResp = mapper.mapFromTableRowMapping(resp, Table_1_Model.class.getCanonicalName());
            System.out.println(tableResp.getLastName());
            System.out.println(tableResp.getFirstName());
            System.out.println(tableResp.getRowkey());
        }catch(HBaseDaoException | HBaseDataIntegrationException e){
            e.printStackTrace();
            fail();
        }
    }

}
