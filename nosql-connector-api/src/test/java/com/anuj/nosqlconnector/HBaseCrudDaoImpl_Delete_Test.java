package com.anuj.nosqlconnector;

import com.anuj.nosqlconnector.dao.dto.CreateHBaseTableDTO;
import com.anuj.nosqlconnector.dao.dto.ScanHBaseTableDTO;
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
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.fail;

public class HBaseCrudDaoImpl_Delete_Test {

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

            final boolean isTableExist = hbaseFacade.isTableExist("cpbnd:My_HBase_Table_1");

           if(!isTableExist){
               final String[] columnFamilies = new String[]{"cf"};
               final CreateHBaseTableDTO createHBaseTableDTO =
                       new CreateHBaseTableDTO.CreateTableBuilder("cpbnd:My_HBase_Table_1", columnFamilies)
                               .tableCompressionAlgo(Compression.Algorithm.SNAPPY)
                               .totalSplits(10).build();
               hbaseFacade.createTable(createHBaseTableDTO);
           }

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
            final ScanHBaseTableDTO scanHBaseTableDTO = new ScanHBaseTableDTO.ScanHBaseTableBuilder("cpbnd:My_HBase_Table_1").build();
            final List<Result> result1 = hbaseFacade.scan(scanHBaseTableDTO);

            final List<HBTableRowMapping<? extends Serializable>> resp1 = hbaseDataIntegrationService.convertFromResultObjects(result1, HBaseRowKeyType.STRING);
            final List<Table_1_Model> response1 = mapper.mapListFromTableRowMappings(resp1, Table_1_Model.class.getCanonicalName());
            System.out.println(response1.size());

            // Delete the data
            hbaseFacade.delete("cpbnd:My_HBase_Table_1", "rowkey1");

            //Scan the table again
            final List<Result> result2 = hbaseFacade.scan(scanHBaseTableDTO);
            final List<HBTableRowMapping<? extends Serializable>> resp2 = hbaseDataIntegrationService.convertFromResultObjects(result2, HBaseRowKeyType.STRING);
            final List<Table_1_Model> response2 = mapper.mapListFromTableRowMappings(resp2, Table_1_Model.class.getCanonicalName());
            System.out.println(response2.size());

        }catch(HBaseDaoException | HBaseDataIntegrationException e){
            e.printStackTrace();
            fail();
        }
    }

}
