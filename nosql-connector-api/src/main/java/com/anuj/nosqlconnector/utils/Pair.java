package com.anuj.nosqlconnector.utils;

import java.io.Serializable;

/**
 * <p>
 *     Utility class to handle the key-value pair.
 * </p>
 * @param <K> key
 * @param <V> value
 */
public class Pair<K extends Serializable, V extends Serializable> implements Serializable {

    private static final long serialVersionUID = 1L;

    public K key;

    public V value;

    public Pair(final K key, final V value){
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
