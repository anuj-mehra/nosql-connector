package com.anuj.nosqlconnector.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 *     Is used to specify the HBase Table Name.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface TableName {

    /**
     * (Mandatory) HBase Table Name
     * @return table name
     */
    String name();
}
