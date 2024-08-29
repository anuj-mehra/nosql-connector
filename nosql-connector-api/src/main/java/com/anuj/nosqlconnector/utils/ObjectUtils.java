package com.anuj.nosqlconnector.utils;

import com.anuj.nosqlconnector.exception.HBaseDaoException;
import com.anuj.nosqlconnector.exception.HBaseDataIntegrationException;

import java.lang.reflect.Constructor;

/**
 * <p>
 *     Utility class to transform <code>Object</code>
 * </p>
 */
public class ObjectUtils {

    /**
     * <p>
     *     This method creates the Java class object from the Canonical name of the class provided as input to the method.
     * </p>
     * @param classCanonicalName
     * @return
     * @throws HBaseDataIntegrationException
     */
    public static final Object createObject(final String classCanonicalName) throws HBaseDataIntegrationException{
        Class<?> clazz = null;
        Object object = null;

        try{
            clazz = Class.forName(classCanonicalName);
            Constructor<?> ctor = clazz.getConstructor();
            object = ctor.newInstance();
        }catch(Exception e){
            throw new HBaseDataIntegrationException.HBaseDataIntegrationExceptionBuilder()
                    .errorMessage("Exception occured while creating instance of the HBase entity class")
                    .throwable(e).build();
        }
        return object;
    }
}
