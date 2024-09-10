package com.anuj.nosqlconnector;

import com.anuj.nosqlconnector.dao.dto.CreateHBaseTableDTO;
import com.anuj.nosqlconnector.dao.dto.GetHBaseRecordDTO;
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

import static org.junit.Assert.fail;

public class HBaseCrudDaoImpl_Save_DoubleKey_Test {

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
            String[] columnFamilies = new String[]{"cf"};
            CreateHBaseTableDTO createHBaseTableDTO =
                    new CreateHBaseTableDTO.CreateTableBuilder("cpbnd:My_HBase_Table_2", columnFamilies)
                    .tableCompressionAlgo(Compression.Algorithm.SNAPPY)
                    .totalSplits(10).build();
            hbaseFacade.createTable(createHBaseTableDTO);


            final Table_2_Model model = new Table_2_Model(Double.valueOf(100001));
            model.setFirstName("firstname");
            model.setLastName("lastName");

            final HBTableRowMapping<Double> tableRowMapping = mapper.mapToTableRowMapping(model);
            hbaseFacade.save(tableRowMapping.getTableName(), tableRowMapping);

            //Fetch Result from hbase
            final GetHBaseRecordDTO getHBaseRecordDTO =
                    new GetHBaseRecordDTO.GetHBaseRecordBuilder("cpbnd:My_HBase_Table_2", model.getRowkey())
                    .columnFamily("cf")
                    .build();
            final Result result = hbaseFacade.getDataRecord(getHBaseRecordDTO);
            final HBTableRowMapping<? extends Serializable> resp =
                    hbaseDataIntegrationService.convertFromResultObject(result, HBaseRowKeyType.DOUBLE);
            final Table_2_Model tableResp = mapper.mapFromTableRowMapping(resp, Table_2_Model.class.getCanonicalName());
            System.out.println(tableResp.getLastName());
            System.out.println(tableResp.getFirstName());
            System.out.println(tableResp.getRowkey());
        }catch(HBaseDaoException | HBaseDataIntegrationException e){
            e.printStackTrace();
            fail();
        }
    }

}
