package com.anuj.nosqlconnector.utils;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

/**
 * <p>
 *     Utility class to fetch and transform <code>byte</code> array.
 * </p>
 */
public final class CustomBytes {

    public static byte[] getBytes(final Object fieldObject) throws IOException {
        byte[] value = null;
        if(null != fieldObject){
            if(fieldObject instanceof String){
                value = Bytes.toBytes((String)fieldObject);
            } else if(fieldObject instanceof Integer){
                value = Bytes.toBytes((int)fieldObject);
            } else if(fieldObject instanceof Double){
                value = Bytes.toBytes((double)fieldObject);
            } else if(fieldObject instanceof Float){
                value = Bytes.toBytes((float)fieldObject);
            } else if(fieldObject instanceof Long){
                value = Bytes.toBytes((long)fieldObject);
            } else if(fieldObject instanceof Serializable){
                value = CustomBytes.toBytes((Serializable)fieldObject); // for objects like list/map/set
            }
        }

        return value;
    }

    /**
     * <p>
     *     Method accepts a <code>Serializable</code> object as input and returns the corresponding byte array.
     * </p>
     * @param serializable
     * @return
     * @throws IOException
     */
    public static byte[] toBytes(final Serializable serializable) throws IOException {
        byte[] array = null;
        if(null != serializable){
            try{
                try(ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bos);){
                    oos.writeObject(serializable);
                    array = bos.toByteArray();
                }
            }catch(IOException e){
                throw e;
            }
        }

        return array;
    }

    public static <T> T toObject(final byte[] array, final HBaseRowKeyType hbaseRowKeyType) throws IOException, ClassNotFoundException {
        Object object = null;

        switch(hbaseRowKeyType){
            case STRING:
                object = Bytes.toString(array);
                break;
            case LONG:
                object = Bytes.toLong(array);
                break;
            case DOUBLE:
                object = Bytes.toDouble(array);
                break;
            case INTEGER:
                object = Bytes.toInt(array);
                break;
        }

        return (T)object;
    }

    public static Serializable toObject(final byte[] array) throws IOException, ClassNotFoundException {
        Object object = null;

        if(array != null){
            try{
                try(ByteArrayInputStream bis = new ByteArrayInputStream(array); ObjectInputStream ois = new ObjectInputStream(bis);){
                    object = ois.readObject();
                }
            }catch(final IOException e){
                throw e;
            }catch(final ClassNotFoundException e){
                throw e;
            }
        }

        return object == null ? null : (Serializable) object;
    }
}
