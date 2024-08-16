package com.anuj.nosqlconnector.connection;

import com.anuj.nosqlconnector.logger.Loggable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * <p>
 *     Factory class for creating hbase connection objects.
 * </p>
 */
public class HBaseConnectionFactory implements Loggable {

    private static volatile Configuration config = null;
    private static volatile Admin admin = null;
    private static volatile Connection connection = null;

    static{
        synchronized(HBaseConnectionFactory.class){
           try{
               config = HBaseConfiguration.create();
               connection = ConnectionFactory.createConnection(config);
               admin = connection.getAdmin();
           }catch(Exception e){
               e.printStackTrace();
               System.exit(-1);
           }
        }
    }

    private HBaseConnectionFactory(){}

    public static Connection getConnection(){
        return connection;
    }

    public static Configuration getConfiguration(){
        return config;
    }

    public static Admin getAdmin(){
        if(null == admin){
            synchronized(HBaseConnectionFactory.class){
                if(null == admin){
                    try{
                        config = HBaseConfiguration.create();
                        connection = ConnectionFactory.createConnection(config);
                        admin = connection.getAdmin();
                    }catch(Exception e){
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            }
        }

        return admin;
    }

}
