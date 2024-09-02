package com.anuj.nosqlconnector;

import static org.junit.Assert.fail;
import com.anuj.nosqlconnector.dao.dto.CreateHBaseTableDTO;
import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;
import com.anuj.nosqlconnector.facade.HBaseFacade;
import com.anuj.nosqlconnector.mappers.Table_1_Mapper;
import com.anuj.nosqlconnector.service.HBaseDataIntegrationService;
import com.anuj.nosqlconnector.service.hbase.HBaseDataIntegrationServiceImpl;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.Before;
import org.junit.Test;

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
            hbaseFacade.createTable(createHBaseTableDTO);

        }catch(HBaseDaoException /*| HBaseDataIntegrationException */e){
            e.printStackTrace();
            fail();
        }
    }

}
