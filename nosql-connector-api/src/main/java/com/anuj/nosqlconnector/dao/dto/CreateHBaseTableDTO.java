package com.anuj.nosqlconnector.dao.dto;

import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.Serializable;

public class CreateHBaseTableDTO implements Serializable{

    private static final long serialVersionUID = 1L;

    private String tableName;
    private String[] columnFamilies;

    // Optional Parameters
    private Integer totalSplits;
    private Compression.Algorithm tableCompressionAlgo;
    private boolean enableBloomFilter;
    private Integer maxColumnHistory;

    private CreateHBaseTableDTO(CreateTableBuilder builder){
        this.tableName = tableName;
        this.columnFamilies = columnFamilies;
        this.totalSplits = totalSplits;
        this.tableCompressionAlgo = tableCompressionAlgo;
        this.enableBloomFilter = enableBloomFilter;
        this.maxColumnHistory = maxColumnHistory;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColumnFamilies() {
        return columnFamilies;
    }

    public Integer getTotalSplits() {
        return totalSplits;
    }

    public Compression.Algorithm getTableCompressionAlgo() {
        return tableCompressionAlgo;
    }

    public boolean isEnableBloomFilter() {
        return enableBloomFilter;
    }

    public Integer getMaxColumnHistory() {
        return maxColumnHistory;
    }

    public static class CreateTableBuilder implements Serializable {

        private static final long serialVersionUID = 1L;

        // Required Parameters
        private final String tableName;
        private final String[] columnFamilies;

        // Optional Parameters
        Integer totalSplits;
        Compression.Algorithm tableCompressionAlgo;
        boolean enableBloomFilter;
        Integer maxColumnHistory;

        public CreateTableBuilder(String tableName, String[] columnFamilies){
            this.tableName = tableName;
            this.columnFamilies = columnFamilies;
        }

        public CreateTableBuilder totalSplits(final Integer totalSplits){
            this.totalSplits = totalSplits;
            return this;
        }

        public CreateTableBuilder tableCompressionAlgo(final Compression.Algorithm tableCompressionAlgo){
            this.tableCompressionAlgo = tableCompressionAlgo;
            return this;
        }

        public CreateTableBuilder enableBloomFilter(final boolean enableBloomFilter){
            this.enableBloomFilter = enableBloomFilter;
            return this;
        }

        public CreateTableBuilder maxColumnHistory(final Integer maxColumnHistory){
            this.maxColumnHistory = maxColumnHistory;
            return this;
        }

        public CreateHBaseTableDTO build(){
            return new CreateHBaseTableDTO(this);
        }
    }
}
