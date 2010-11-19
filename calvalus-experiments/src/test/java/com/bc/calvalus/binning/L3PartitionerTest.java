package com.bc.calvalus.binning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static org.junit.Assert.*;

public class L3PartitionerTest {
    @Test
    public void testConfig() {
        L3Partitioner l3Partitioner = new L3Partitioner();

        assertNull(l3Partitioner.getBinningGrid());

        Configuration conf = new Configuration();
        int numRows = 113;
        conf.setInt(L3ProcessingMapper.CONFNAME_L3_NUM_ROWS, numRows);
        l3Partitioner.setConf(conf);
        IsinBinningGrid binningGrid = l3Partitioner.getBinningGrid();

        assertNotNull(binningGrid);
        assertEquals(numRows, binningGrid.getNumRows());
    }

    @Test
    public void test6Rows2Partitions() {
        L3Partitioner l3Partitioner = new L3Partitioner();
        Configuration conf = new Configuration();
        int numRows = 6;
        conf.setInt(L3ProcessingMapper.CONFNAME_L3_NUM_ROWS, numRows);
        l3Partitioner.setConf(conf);
        IsinBinningGrid binningGrid = l3Partitioner.getBinningGrid();

        assertEquals(3, binningGrid.getNumCols(0));
        assertEquals(8, binningGrid.getNumCols(1));
        assertEquals(12, binningGrid.getNumCols(2));
        
        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        int numPartitions = 2;

        assertEquals(0, l3Partitioner.getPartition(new IntWritable(0), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new IntWritable(3), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new IntWritable(3 + 8 - 1), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new IntWritable(3 + 8), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12 - 1), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12 + 8), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12 + 8 + 3 - 1), null, numPartitions));
    }

    @Test
    public void test6Rows3Partitions() {
        L3Partitioner l3Partitioner = new L3Partitioner();
        Configuration conf = new Configuration();
        int numRows = 6;
        conf.setInt(L3ProcessingMapper.CONFNAME_L3_NUM_ROWS, numRows);
        l3Partitioner.setConf(conf);
        IsinBinningGrid binningGrid = l3Partitioner.getBinningGrid();

        assertEquals(3, binningGrid.getNumCols(0));
        assertEquals(8, binningGrid.getNumCols(1));

        assertEquals(12, binningGrid.getNumCols(2));
        assertEquals(12, binningGrid.getNumCols(3));

        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        int numPartitions = 3;

        assertEquals(0, l3Partitioner.getPartition(new IntWritable(0), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new IntWritable(3), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new IntWritable(3 + 8 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12 - 1), null, numPartitions));

        assertEquals(2, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12), null, numPartitions));
        assertEquals(2, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12 + 8), null, numPartitions));
        assertEquals(2, l3Partitioner.getPartition(new IntWritable(3 + 8 + 12 + 12 + 8 + 3 - 1), null, numPartitions));
    }

    @Test
    public void test8Rows3Partitions() {
        L3Partitioner l3Partitioner = new L3Partitioner();
        Configuration conf = new Configuration();
        int numRows = 8;
        conf.setInt(L3ProcessingMapper.CONFNAME_L3_NUM_ROWS, numRows);
        l3Partitioner.setConf(conf);
        IsinBinningGrid binningGrid = l3Partitioner.getBinningGrid();

        assertEquals(3, binningGrid.getNumCols(0));
        assertEquals(9, binningGrid.getNumCols(1));
        assertEquals(13, binningGrid.getNumCols(2));

        assertEquals(16, binningGrid.getNumCols(3));
        assertEquals(16, binningGrid.getNumCols(4));
        assertEquals(13, binningGrid.getNumCols(5));

        assertEquals(9, binningGrid.getNumCols(6));
        assertEquals(3, binningGrid.getNumCols(7));

        int numPartitions = 3;

        assertEquals(0, l3Partitioner.getPartition(new IntWritable(0), null, numPartitions));
        assertEquals(0, l3Partitioner.getPartition(new IntWritable(3 + 9 + 13 - 1), null, numPartitions));

        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 9 + 13), null, numPartitions));
        assertEquals(1, l3Partitioner.getPartition(new IntWritable(3 + 9 + 13 + 16 + 16 + 13 - 1), null, numPartitions));

        assertEquals(2, l3Partitioner.getPartition(new IntWritable(3 + 9 + 13 + 16 + 16 + 13), null, numPartitions));
        assertEquals(2, l3Partitioner.getPartition(new IntWritable(3 + 9 + 13 + 16 + 16 + 13 + 9 + 3 -1), null, numPartitions));
    }
}
