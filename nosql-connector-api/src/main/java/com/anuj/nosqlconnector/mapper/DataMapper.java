package com.anuj.nosqlconnector.mapper;

import com.anuj.nosqlconnector.logger.Loggable;
import com.anuj.nosqlconnector.model.HBTableRowMapping;

import java.io.Serializable;

public interface DataMapper <I,U extends HBTableRowMapping<? extends Serializable>> extends Loggable {

}
