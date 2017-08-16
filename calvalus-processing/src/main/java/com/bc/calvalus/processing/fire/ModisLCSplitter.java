package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;
import org.esa.snap.core.gpf.common.resample.ResamplingOp;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Splits LC map into tiles relevant for Fire-CCI processing.
 */
public class ModisLCSplitter {

    public static void main(String[] args) {
        FileSystem fileSystem = FileSystems.getDefault();
        String[] years = {"2000", "2005", "2010"};
        for (String year : years) {
            Path lcInputPath = fileSystem.getPath("D:\\workspace\\fire-cci\\lc-data-to-split", "ESACCI-LC-L4-LCCS-Map-300m-P5Y-" + year + "-v1.6.1.nc");

        }
    }

    public static void main2(String[] args) throws IOException {
        FileSystem fileSystem = FileSystems.getDefault();

        String modisInputPath = "D:\\workspace\\fire-cci\\modis-for-lc";
        String outputPath = "D:\\workspace\\fire-cci\\splitted-lc-data\\modis";

        String[] years = {"2000", "2005", "2010"};
        List<Runnable> tasks = new ArrayList<>();

        for (String year : years) {
            Path lcInputPath = fileSystem.getPath("D:\\workspace\\fire-cci\\lc-data-to-split", "ESACCI-LC-L4-LCCS-Map-300m-P5Y-" + year + "-v1.6.1.nc");
            Product lcInputProduct = ProductIO.readProduct(lcInputPath.toFile());
            ReprojectionOp reprojectionOp = new ReprojectionOp();
            File wktFile = new File(ModisLCSplitter.class.getResource("modis-wkt").getPath());
            reprojectionOp.setParameter("wktFile", wktFile);
            reprojectionOp.setSourceProduct(lcInputProduct);
            reprojectionOp.setParameterDefaultValues();

            Product lcProduct = reprojectionOp.getTargetProduct();

            ResamplingOp resamplingOp = new ResamplingOp();
            resamplingOp.setParameter("targetWidth", 36 * 4800);
            resamplingOp.setParameter("targetHeight", 18 * 4800);
            resamplingOp.setParameterDefaultValues();
            resamplingOp.setSourceProduct(lcProduct);

            Files.list(Paths.get(modisInputPath)).filter(path -> path.toString().endsWith(".hdf")).forEach(
                    path -> tasks.add(() -> {
                        try {
                            Product reference = ProductIO.readProduct(path.toFile());
                            String tile = reference.getName();
                            int x = Integer.parseInt(tile.substring(1, 3));
                            int y = Integer.parseInt(tile.substring(4, 6));
                            String outputFilename = outputPath + "\\" + tile + "-" + year + ".nc";
                            if (Files.exists(Paths.get(outputFilename))) {
                                System.out.println("Already existing tile: " + tile);
                                return;
                            }

                            SubsetOp subsetOp = new SubsetOp();
                            subsetOp.setRegion(new Rectangle(x * 4800, y * 4800, 4800, 4800));
                            subsetOp.setSourceProduct(resamplingOp.getTargetProduct());
                            subsetOp.setBandNames(new String[]{"lccs_class"});
                            Product lcSubset = subsetOp.getTargetProduct();
                            lcSubset.setSceneGeoCoding(null);

                            ProductIO.writeProduct(lcSubset, outputFilename, "NetCDF4-CF");

                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    })
            );
        }

        ExecutorService exec = Executors.newFixedThreadPool(1);
        try {
            for (Runnable task : tasks) {
                exec.submit(task);
            }
        } finally {
            exec.shutdown();
        }
    }

}