package com.anuj.nosqlconnector.dao;

import com.anuj.nosqlconnector.dao.dto.CreateHBaseTableDTO;
import com.anuj.nosqlconnector.exception.HBaseDaoException;

/**
 * <p>
 *
 * </p>
 */
public interface HBaseDDLDao extends HBaseDao{

    void createTable(CreateHBaseTableDTO createTableDTO) throws HBaseDaoException;

    void deleteTable(final String tableName) throws HBaseDaoException;
}
