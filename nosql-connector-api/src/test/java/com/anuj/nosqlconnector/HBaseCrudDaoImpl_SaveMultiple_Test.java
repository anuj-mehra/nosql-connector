package com.anuj.nosqlconnector;

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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.fail;

public class HBaseCrudDaoImpl_SaveMultiple_Test {

    private String projectDirPath = Paths.get("").toAbsolutePath().toString();

    private HBaseFacade hbaseFacade = HBaseFacade.getInstance(projectDirPath.concat("/src/test/resources/hbase-table-name.properties"));

    private HBaseDataIntegrationService  hbaseDataIntegrationService = HBaseDataIntegrationServiceImpl.getInstance();
    private Table_1_Mapper mapper = new Table_1_Mapper();

    @Before
    public void setUp(){

    }

    @Test
    public void testSave(){

        try{
            final String[] columnFamilies = new String[]{"cf"};
            final CreateHBaseTableDTO createHBaseTableDTO =
                    new CreateHBaseTableDTO.CreateTableBuilder("cpbnd:My_HBase_Table_1", columnFamilies)
                    .tableCompressionAlgo(Compression.Algorithm.SNAPPY)
                    .totalSplits(10).build();
            hbaseFacade.createTable(createHBaseTableDTO);


            final Table_1_Model model1 = new Table_1_Model("rowkey1");
            model1.setFirstName("firstname1");
            model1.setLastName("lastName1");

            final Table_1_Model model2 = new Table_1_Model("rowkey2");
            model2.setFirstName("firstname2");
            model2.setLastName("lastName2");

            final HBTableRowMapping<String> tableRowMapping1 = mapper.mapToTableRowMapping(model1);
            final HBTableRowMapping<String> tableRowMapping2 = mapper.mapToTableRowMapping(model2);
            final List<HBTableRowMapping<String>> tableRowMappings = new LinkedList<>();
            tableRowMappings.add(tableRowMapping1);
            tableRowMappings.add(tableRowMapping2);

            hbaseFacade.save(tableRowMapping1.getTableName(), tableRowMappings);

            //Fetch Result from hbase
            //final Table_1_Model fetchDataModel = new Table_1_Model("rowkey1");
            //final HBTableRowMapping<String> fetchTableMapping = mapper.mapToTableRowMapping(model);
            final List<String> searchKeys = new ArrayList<>();
            searchKeys.add("rowkey1");
            searchKeys.add("rowkey2");
            final GetHBaseRecordsDTO getHBaseRecordsDTO =
                    new GetHBaseRecordsDTO.GetHBaseRecordsBuilder("cpbnd:My_HBase_Table_1", searchKeys)
                    .columnFamily("cf")
                    .build();
            final List<Result> results = hbaseFacade.getBulkDataRecords(getHBaseRecordsDTO);
            final List<HBTableRowMapping<? extends Serializable>> resp =
                    hbaseDataIntegrationService.convertFromResultObjects(results, HBaseRowKeyType.STRING);
            final List<Table_1_Model> tableResp = mapper.mapListFromTableRowMappings(resp, Table_1_Model.class.getCanonicalName());
            System.out.println(tableResp.size());
            System.out.println(tableResp.get(0).getLastName());
            System.out.println(tableResp.get(0).getFirstName());
            System.out.println(tableResp.get(0).getRowkey());

            System.out.println(tableResp.get(1).getLastName());
            System.out.println(tableResp.get(1).getFirstName());
            System.out.println(tableResp.get(1).getRowkey());
        }catch(HBaseDaoException | HBaseDataIntegrationException e){
            e.printStackTrace();
            fail();
        }
    }

}
