package com.anuj.nosqlconnector;

import com.anuj.nosqlconnector.dao.dto.CreateHBaseTableDTO;
import com.anuj.nosqlconnector.dao.dto.GetHBaseRecordsOnRangeDTO;
import com.anuj.nosqlconnector.dao.dto.ScanHBaseTableDTO;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.facade.HBaseFacade;
import com.anuj.nosqlconnector.mappers.Table_2_Mapper;
import com.anuj.nosqlconnector.model.HBTableRowMapping;
import com.anuj.nosqlconnector.models.Table_2_Model;
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

public class HBaseCrudDaoImpl_FetchRange_DeleteRange_Test {

    private String projectDirPath = Paths.get("").toAbsolutePath().toString();

    private HBaseFacade hbaseFacade = HBaseFacade.getInstance(projectDirPath.concat("/src/test/resources/hbase-table-name.properties"));

    private HBaseDataIntegrationService  hbaseDataIntegrationService = HBaseDataIntegrationServiceImpl.getInstance();
    private Table_2_Mapper mapper = new Table_2_Mapper();

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
                       new CreateHBaseTableDTO.CreateTableBuilder("cpbnd:My_HBase_Table_2", columnFamilies)
                               .tableCompressionAlgo(Compression.Algorithm.SNAPPY)
                               .totalSplits(10).build();
               hbaseFacade.createTable(createHBaseTableDTO);
           }

            final Table_2_Model model1 = new Table_2_Model(Double.valueOf(100100));
            model1.setFirstName("firstname1");
            model1.setLastName("lastName1");

            final Table_2_Model model2 = new Table_2_Model(Double.valueOf(100200));
            model2.setFirstName("firstname2");
            model2.setLastName("lastName2");

            final Table_2_Model model3 = new Table_2_Model(Double.valueOf(100300));
            model2.setFirstName("firstname3");
            model2.setLastName("lastName3");

            final HBTableRowMapping<Double> tableRowMapping1 = mapper.mapToTableRowMapping(model1);
            final HBTableRowMapping<Double> tableRowMapping2 = mapper.mapToTableRowMapping(model2);
            final HBTableRowMapping<Double> tableRowMapping3 = mapper.mapToTableRowMapping(model3);
            final List<HBTableRowMapping<Double>> tableRowMappings = new LinkedList<>();
            tableRowMappings.add(tableRowMapping1);
            tableRowMappings.add(tableRowMapping2);
            tableRowMappings.add(tableRowMapping3);

            hbaseFacade.save(tableRowMapping1.getTableName(), tableRowMappings);

            //Fetch Result from hbase | Using Range
            final GetHBaseRecordsOnRangeDTO getHBaseRecordsOnRangeDTO = new GetHBaseRecordsOnRangeDTO.GetHBaseRecordsOnRangeBuilder("cpbnd:My_HBase_Table_2", Double.valueOf(100100), Double.valueOf(100299), true, true).build();
            final List<Result> results1 = hbaseFacade.getDataRecordsOnRange(getHBaseRecordsOnRangeDTO);
            final List<HBTableRowMapping<? extends Serializable>> resp1 =
                    hbaseDataIntegrationService.convertFromResultObjects(results1, HBaseRowKeyType.DOUBLE);
            final List<Table_2_Model> response1 = mapper.mapListFromTableRowMappings(resp1,Table_2_Model.class.getCanonicalName());
            System.out.println(response1.size());

            // Delete results on Range
            hbaseFacade.deleteDataRecordsOnRange("cpbnd:My_HBase_Table_2", Double.valueOf(100100), Double.valueOf(100299), false);


            //Fetch Result from hbase | Using Range
            final GetHBaseRecordsOnRangeDTO getHBaseRecordsOnRangeDTO2 = new GetHBaseRecordsOnRangeDTO.GetHBaseRecordsOnRangeBuilder("cpbnd:My_HBase_Table_2", Double.valueOf(100100), Double.valueOf(100299), true, true).build();
            final List<Result> results2 = hbaseFacade.getDataRecordsOnRange(getHBaseRecordsOnRangeDTO2);
            final List<HBTableRowMapping<? extends Serializable>> resp2 =
                    hbaseDataIntegrationService.convertFromResultObjects(results2, HBaseRowKeyType.DOUBLE);
            final List<Table_2_Model> response2 = mapper.mapListFromTableRowMappings(resp2,Table_2_Model.class.getCanonicalName());
            System.out.println(response2.size());

        }catch(HBaseDaoException | HBaseDataIntegrationException e){
            e.printStackTrace();
            fail();
        }
    }

}
