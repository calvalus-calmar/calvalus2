package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.AbstractGridReducer;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import org.apache.hadoop.io.Text;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;

import java.io.IOException;

public class S2GridReducer extends AbstractGridReducer {

    private static final int S2_CHUNK_SIZE = 8;
    private final NcFileFactory s2NcFileFactory;

    public S2GridReducer() {
        this.s2NcFileFactory = new S2NcFileFactory();
    }

    @Override
    protected void reduce(Text key, Iterable<GridCells> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        GridCells currentGridCells = getCurrentGridCells();
        try {
            int x = getX(key.toString());
            int y = getY(key.toString());
            writeFloatChunk(x, y, ncFile, "fraction_of_burnable_area", currentGridCells.burnableFraction);
            writeFloatChunk(x, y, ncFile, "fraction_of_observed_area", currentGridCells.coverage);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected String getFilename(String year, String month, String version) {
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MSI-f%s.nc", year, month, "01", version);
    }

    @Override
    protected NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        return s2NcFileFactory.createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays);
    }

    @Override
    protected int getTargetSize() {
        return S2_CHUNK_SIZE;
    }

    @Override
    protected int getX(String key) {
        key = key.split("-")[2];
        return Integer.parseInt(key.split("y")[0].substring(1)) * 4;
    }

    @Override
    protected int getY(String key) {
        key = key.split("-")[2];
        int y = Integer.parseInt(key.split("y")[1]);
        return (180 - y) * 4;
    }

}
