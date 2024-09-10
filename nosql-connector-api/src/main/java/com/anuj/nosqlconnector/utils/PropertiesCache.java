package com.anuj.nosqlconnector.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesCache {

    private static final Properties prop = new Properties();
    private static volatile PropertiesCache INSTANCE = new PropertiesCache();

    private PropertiesCache(){}

    public static PropertiesCache getInstance(){

        return INSTANCE;
    }

    public static void loadProperties(String propertyFileFullyQualifiedPath){
        System.out.println("propertyFileFullyQualifiedPath ==> " + propertyFileFullyQualifiedPath);
        if(prop.entrySet().size() == 0){
            try{
                InputStream input = new FileInputStream(propertyFileFullyQualifiedPath);
                prop.load(input);
            }catch(IOException e){
                e.printStackTrace();
            }
        }

        System.out.println(prop);
    }

    public String getProperty(String key){
        return prop.getProperty(key);
    }

}
