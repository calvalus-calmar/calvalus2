/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.mosaic2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.DataPeriod;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.SpatialBinConsumer;
import org.esa.snap.binning.SpatialBinner;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.SpatialProductBinner;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * processes and reprojects one input, applies aggregators, writes micro tiles (instead of single pixels).
 *
 * @author Martin
 */
public class Mosaic2Mapper extends Mapper<NullWritable, NullWritable, TileIndexWritable, L3SpatialBinMicroTileWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        final boolean generateEmptyAggregate = conf.getBoolean("calvalus.generateEmptyAggregate", false);
        final int microTileSize = conf.getInt("microTileSize", 256);
        final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        final DataPeriod dataPeriod = HadoopBinManager.createDataPeriod(conf, binningConfig.getMinDataHour());
        final BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, dataPeriod, regionGeometry);
        final SpatialBinMicroTileEmitter spatialBinEmitter = new SpatialBinMicroTileEmitter(context, binningConfig.getNumRows());
        final SpatialBinner spatialBinner = new SpatialMicroTileBinner(binningContext, spatialBinEmitter);
        final ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        final ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        final int progressForProcessing = processorAdapter.supportsPullProcessing() ? 5 : 90;
        final int progressForBinning = processorAdapter.supportsPullProcessing() ? 90 : 20;

        LOG.info("processing input " + processorAdapter.getInputPath() + " ...");
        pm.beginTask("Level 3", progressForProcessing + progressForBinning);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, progressForProcessing));
            if (product != null) {
                product.setPreferredTileSize(microTileSize*4, microTileSize*4);
                HashMap<Product, List<Band>> addedBands = new HashMap<>();
                long numObs;
                try {
                    numObs = SpatialProductBinner.processProduct(product,
                            spatialBinner,
                            addedBands,
                            SubProgressMonitor.create(pm, progressForBinning));
                    spatialBinEmitter.flush();
                } catch (IllegalArgumentException e) {
                    boolean isSmallProduct = product.getSceneRasterHeight() <= 2 || product.getSceneRasterWidth() <= 2;
                    boolean cannotConstructGeoCoding = isSmallProduct && e.getMessage().equals("The specified region, if not null, must intersect with the image`s bounds.");
                    if (cannotConstructGeoCoding) {
                        // ignore this product, but don't fail the process
                        numObs = 0;
                    } else {
                        // something else is wrong that must be handled elsewhere.
                        throw e;
                    }
                }
                if (numObs > 0L) {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixels").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Pixel processed").increment(numObs);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product without pixels").increment(1);
                }
                if (numObs > 0 || generateEmptyAggregate) {
                    final String metaXml = extractProcessingGraphXml(product);
                    context.write(new TileIndexWritable(L3SpatialBin.METADATA_MAGIC_NUMBER,L3SpatialBin.METADATA_MAGIC_NUMBER,
                                                        L3SpatialBin.METADATA_MAGIC_NUMBER,L3SpatialBin.METADATA_MAGIC_NUMBER),
                                  new L3SpatialBinMicroTileWritable(metaXml));
                }
            } else {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not used").increment(1);
                LOG.info("Product not used");
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }

        final Exception[] exceptions = spatialBinner.getExceptions();
        for (Exception exception : exceptions) {
            String m = MessageFormat.format("Failed to process input slice of {0}", processorAdapter.getInputPath());
            LOG.log(Level.SEVERE, m, exception);
        }
        LOG.info(MessageFormat.format("Finishes processing of {0}  ({1} observations seen, {2} bins produced)",
                                      processorAdapter.getInputPath(),
                                      spatialBinEmitter.numObsTotal,
                                      spatialBinEmitter.numBinsTotal));
    }

    static String extractProcessingGraphXml(Product product) {
        final MetadataElement metadataRoot = product.getMetadataRoot();
        final MetadataElement processingGraph = metadataRoot.getElement("Processing_Graph");
        final MetadataSerializer metadataSerializer = new MetadataSerializer();
        return metadataSerializer.toXml(processingGraph);
    }

    /**
     * Wraps the processing of an observation slice, tracks micro tiles actively filled, and flushes inactive ones afterwards. Cares for a small memory footprint.
     */
    private static class SpatialMicroTileBinner extends SpatialBinner {
        SpatialBinMicroTileEmitter consumer;
        public SpatialMicroTileBinner(BinningContext binningContext, SpatialBinConsumer consumer) {
            super(binningContext, consumer);
            this.consumer = (SpatialBinMicroTileEmitter) consumer;
        }

        @Override
        public long processObservationSlice(Iterable<Observation> observations) {
            try {
                consumer.flushInactiveMicroTiles();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return super.processObservationSlice(observations);
        }
    }

    static class SpatialBinMicroTileEmitter implements SpatialBinConsumer {
        int numObsTotal = 0;
        int numBinsTotal = 0;
        Map<Long, SpatialBin[]> microTiles = new HashMap<>();
        Set<Long> finalisedMicroTileRows = new HashSet<>();
        private Context context;
        int numRowsGlobal;
        int microTileHeight;
        int microTileWidth;
        int macroTileHeight;
        int macroTileWidth;
        int microTileRows;
        int microTileCols;

        public SpatialBinMicroTileEmitter(Context context, int numRowsGlobal) {
            this.context = context;
            this.numRowsGlobal = numRowsGlobal;
            macroTileHeight = context.getConfiguration().getInt("tileHeight", context.getConfiguration().getInt("tileSize", numRowsGlobal));
            macroTileWidth = context.getConfiguration().getInt("tileWidth", context.getConfiguration().getInt("tileSize", numRowsGlobal * 2));
            microTileHeight = context.getConfiguration().getInt("microTileHeight", context.getConfiguration().getInt("microTileSize", macroTileHeight));
            microTileWidth = context.getConfiguration().getInt("microTileWidth", context.getConfiguration().getInt("microTileSize", macroTileWidth));
            microTileRows = numRowsGlobal / microTileHeight;
            microTileCols = 2 * numRowsGlobal / microTileWidth;
            LOG.info(String.format("tile configuration with %d*%d tiles, %d*%d micro tiles, %d lines per tile, %d lines per micro tile", 2 * numRowsGlobal / macroTileWidth, numRowsGlobal / macroTileHeight, microTileCols, microTileRows, macroTileHeight, microTileHeight));
        }

        @Override
        public void consumeSpatialBins(BinningContext binningContext, List<SpatialBin> spatialBins) throws Exception {
            for (SpatialBin spatialBin : spatialBins) {
                long index = microTileIndex(spatialBin.getIndex());
                SpatialBin[] bin = microTiles.get(index);
                if (bin == null) {
                    //LOG.info("collecting for micro tile x" + index%microTileCols + "y" + index/microTileCols + " pixel bin " + spatialBin.getIndex());
                    bin = new SpatialBin[microTileHeight * microTileWidth];
                    microTiles.put(index, bin);
                }
                bin[microTilePosition(spatialBin.getIndex())] = spatialBin; // do we need a copy here?
                finalisedMicroTileRows.remove(microTileRow(index));
                numObsTotal += spatialBin.getNumObs();
                numBinsTotal++;
            }
        }

        public void flush() throws IOException, InterruptedException {
            for (Map.Entry<Long, SpatialBin[]> entry : microTiles.entrySet()) {
                TileIndexWritable key = tileIndex(entry.getKey());
                LOG.info("writing micro tile x" + entry.getKey()%microTileCols + "y" + entry.getKey()/microTileCols + " with " + countValid(entry.getValue()) + " spatial bins");
                context.write(key, new L3SpatialBinMicroTileWritable(entry.getValue()));
            }
        }

        int countValid(SpatialBin[] bins) {
            int count = 0;
            for (SpatialBin bin : bins) {
                if (bin != null) {
                    ++count;
                }
            }
            return count;
        }

        /** Returns position of bin within micro tile */
        int microTilePosition(long binIndex) {
            int row = (int) (binIndex / (numRowsGlobal * 2));
            int col = (int) (binIndex % (numRowsGlobal * 2));
            int mRow = row % microTileHeight;
            int mCol = col % microTileWidth;
            return mRow * microTileWidth + mCol;
        }

        /** Returns position of micro tile within global grid of micro tiles */
        long microTileIndex(long binIndex) {
            int row = (int) (binIndex / (numRowsGlobal * 2));
            int col = (int) (binIndex % (numRowsGlobal * 2));
            int mRow = row / microTileHeight;
            int mCol = col / microTileWidth;
            return mRow * microTileCols + mCol;
        }

        /** Returns micro tile row of a micro tile index */
        long microTileRow(long microTileIndex) {
            return microTileIndex / microTileCols;
        }

        /** Converts micro tile position into index */
        TileIndexWritable tileIndex(Long microTileIndex) {
            int mRow = (int) (microTileIndex / microTileCols);
            int mCol = (int) (microTileIndex % microTileCols);
            int tRow = mRow / (macroTileHeight / microTileHeight);
            int tCol = mCol / (macroTileWidth / microTileWidth);
            return new TileIndexWritable(tCol, tRow, mCol, mRow);
        }

        public void flushInactiveMicroTiles() throws IOException, InterruptedException {
            microTiles.entrySet().removeIf(entry -> flushFinalisedRowEntry(entry));
            finalisedMicroTileRows.clear();
            for (long index : microTiles.keySet()) {
                finalisedMicroTileRows.add(microTileRow(index));
            }
        }

        boolean flushFinalisedRowEntry(Map.Entry<Long,SpatialBin[]> entry) {
            long index = entry.getKey();
            long row = microTileRow(index);
            if (finalisedMicroTileRows.contains(row)) {
                TileIndexWritable key = tileIndex(index);
                LOG.info("writing micro tile x" + index%microTileCols + "y" + index/microTileCols + " with " + countValid(entry.getValue()) + " spatial bins");
                try {
                    context.write(key, new L3SpatialBinMicroTileWritable(entry.getValue()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
