package com.anuj.nosqlconnector.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 *     Is used to specify a mapped column for a persistent property or field.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HBaseColumn {

    /**
     * (Mandatory) Name of the column family
     * @return column family name
     */
    String columnFamily();

    /**
     * (Mandatory) Column name inside the column family
     * @return column name
     */
    String columnName();
}
