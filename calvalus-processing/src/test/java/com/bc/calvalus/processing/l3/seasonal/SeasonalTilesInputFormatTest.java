package com.bc.calvalus.processing.l3.seasonal;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;
import org.mockito.Mockito;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class SeasonalTilesInputFormatTest {
    @Test
    public void testCreateSplits() throws Exception {
        SeasonalTilesInputFormat inputFormat = new SeasonalTilesInputFormat();
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(fs.getFileBlockLocations((FileStatus) Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).
                thenReturn(new BlockLocation[]{new BlockLocation(new String[]{"name"}, new String[]{"host"}, 0, 99)});
        FileStatus[] files = new FileStatus[] {
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20090108-v2.0.nc")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20090115-v2.0.nc")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v09-20090115-v2.0.nc")),
        };
        List<InputSplit> splits = new ArrayList<InputSplit>();

        inputFormat.createSplits(fs, null, files, splits);

        assertEquals(2, splits.size());
    }
}