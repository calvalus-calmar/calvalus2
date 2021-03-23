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
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l2.L2FormattingMapper;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3Formatter;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.TemporalBinSource;
import org.esa.snap.binning.TemporalBinner;
import org.esa.snap.binning.cellprocessor.CellProcessorChain;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.metadata.GlobalMetadata;
import org.esa.snap.binning.operator.metadata.MetadataAggregator;
import org.esa.snap.binning.operator.metadata.MetadataAggregatorFactory;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.ProductData;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * Reduces mosaics of spatial bins to a temporal bin.
 *
 * @author Martin
 */
public class Mosaic2Reducer extends Reducer<TileIndexWritable, L3SpatialBinMicroTileWritable, LongWritable, L3TemporalBin> {

    private Configuration conf;
    private TemporalBinner temporalBinner;
    private CellProcessorChain cellChain;
    private boolean computeOutput;
    private BinningConfig binningConfig;
    private BinningContext binningContext;
    private MetadataElement processingGraphMetadata;
    private MetadataSerializer metadataSerializer = new MetadataSerializer();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            int numReducers = conf.getInt(JobContext.NUM_REDUCES, 8);
            final boolean generateEmptyAggregate = conf.getBoolean("calvalus.generateEmptyAggregate", false);
            String format = conf.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
            if ((numReducers == 1 || "org.esa.snap.binning.support.IsinPlanetaryGrid".equals(binningConfig.getPlanetaryGrid())) && format != null) {
                // if only one reducer and output format parameter set, format directly

                // handle metadata
                // it is always the first key in some reducer
                // unless we had not inputs at all
                // TODO what happens if there is no metadata key or does this never happen ??? cell-l3-workflow !!
                final boolean lookingAtNext = context.nextKey();
                if (! lookingAtNext) {
                    if (generateEmptyAggregate) {
                        CalvalusLogger.getLogger().info("no contributions, generating empty output");
                        context = new WrappedContext(context, lookingAtNext);
                    } else {
                        return;
                    }
                } else if (context.getCurrentKey().getMacroTileX() == L3SpatialBin.METADATA_MAGIC_NUMBER) {
                    CalvalusLogger.getLogger().info("metadata record seen");
                    processingGraphMetadata = aggregateMetadata(context.getValues());
                    final String aggregatedMetadataXml = metadataSerializer.toXml(processingGraphMetadata);
                    conf.set(JobConfigNames.PROCESSING_HISTORY, aggregatedMetadataXml);
                } else {
                    CalvalusLogger.getLogger().info("no metadata record seen, resetting iterator");
                    context = new WrappedContext(context, lookingAtNext);
                }

                String dateStart = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
                String dateStop = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
                String outputPrefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");
                //String regionName = conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME);
                //String regionWKT = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
                final int numRowsGlobal = binningConfig.getNumRows();
                final int macroTileHeight = conf.getInt("tileHeight", conf.getInt("tileSize", numRowsGlobal));
                final int macroTileWidth = conf.getInt("tileWidth", conf.getInt("tileSize", numRowsGlobal * 2));
                final int macroTileRows = numRowsGlobal / macroTileHeight;
                final int macroTileCols = 2 * numRowsGlobal / macroTileWidth;

                final Mosaic2TemporalBinSource temporalBinSource = new Mosaic2TemporalBinSource(context, macroTileCols);
                while (true) {
                    int tileIndex = temporalBinSource.nextTile();
                    if (tileIndex < 0) {
                        break;
                    }
                    String tileName = tileNameOf(tileIndex, macroTileRows, macroTileCols);
                    String tileWkt = tileWktOf(tileIndex, macroTileRows, macroTileCols);
                    String productName = String.format("%s_%s_%s_%s", outputPrefix, dateStart, dateStop, tileName);
                    if (context.getConfiguration().get(JobConfigNames.CALVALUS_OUTPUT_REGEX) != null
                            && context.getConfiguration().get(JobConfigNames.CALVALUS_OUTPUT_REPLACEMENT) != null) {
                        productName = L2FormattingMapper.getProductName(context.getConfiguration(), productName);
                    }
                    L3Formatter.write(context, temporalBinSource,
                                      dateStart, dateStop,
                                      tileName, tileWkt,
                                      productName);
                }
            } else {
                while (context.nextKey()) {
                    reduce(context.getCurrentKey(), context.getValues(), context);
                }
            }
        } finally {
            cleanup(context);
        }
    }

    private String tileWktOf(int tileIndex, int macroTileRows, int macroTileCols) {
        int tileRow = tileIndex / macroTileCols;
        int tileCol = tileIndex % macroTileCols;
        double latMax = 90.0 - tileRow * 180.0 / macroTileRows;
        double lonMin = -180.0 + tileCol * 360.0 / macroTileCols;
        double latMin = latMax - 180.0 / macroTileRows;
        double lonMax = lonMin + 360.0 / macroTileCols;
        return String.format("POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))",
                             lonMin, latMin, lonMax, latMin, lonMax, latMax, lonMin, latMax, lonMin, latMin);
    }

    private String tileNameOf(int tileIndex, int macroTileRows, int macroTileCols) {
        int tileRow = tileIndex / macroTileCols;
        int tileCol = tileIndex % macroTileCols;
        if (macroTileRows < 100 && macroTileCols < 100) {
            return String.format("h%02dv%02d", tileCol, tileRow);
        } else {
            return String.format("h%03dv%03d", tileCol, tileRow);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        final String l3ConfXML = conf.get(JobConfigNames.CALVALUS_CELL_PARAMETERS, conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        try {
            binningConfig = BinningConfig.fromXml(l3ConfXML);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid L3 configuration: " + e.getMessage(), e);
        }
        final Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        binningContext = HadoopBinManager.createBinningContext(binningConfig, null, regionGeometry);
        conf.setStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, binningContext.getBinManager().getResultFeatureNames());
        temporalBinner = new TemporalBinner(binningContext);
        cellChain = new CellProcessorChain(binningContext);
        computeOutput = conf.getBoolean(JobConfigNames.CALVALUS_L3_COMPUTE_OUTPUTS, true);
    }

    @Override
    protected void reduce(TileIndexWritable binIndex, Iterable<L3SpatialBinMicroTileWritable> spatialBins, Context context) throws IOException, InterruptedException {

        TemporalBin[] temporalBins = aggregate(binIndex, spatialBins);
        if (temporalBins != null) {
            for (TemporalBin temporalBin : temporalBins) {
                if (temporalBin != null) {
                    context.write(new LongWritable(temporalBin.getIndex()), (L3TemporalBin) temporalBin);
                }
            }
        }
    }

    private TemporalBin[] aggregate(TileIndexWritable binIndex, Iterable<L3SpatialBinMicroTileWritable> spatialBinTiles) throws IOException, InterruptedException {
        final long idx = binIndex.getMacroTileX();
        if (idx == L3SpatialBin.METADATA_MAGIC_NUMBER) {
            processingGraphMetadata = aggregateMetadata(spatialBinTiles);
            return null;
        } else {
            TemporalBin[] temporalBins = null;
            for (L3SpatialBinMicroTileWritable spatialBinMicroTile : spatialBinTiles) {
                SpatialBin[] spatialBins = spatialBinMicroTile.getSamples();
                // lazy creation of temporalBin array, we need the micro tile size
                if (temporalBins == null) {
                    temporalBins = new TemporalBin[spatialBins.length];
                }
                for (int i=0; i<temporalBins.length; ++i) {
                    if (spatialBins[i] != null) {
                        // lazy creation of the temporal bin, we do not do it for out-of-area pixels
                        if (temporalBins[i] == null) {
                            temporalBins[i] = binningContext.getBinManager().createTemporalBin(spatialBins[i].getIndex());
                        }
                        binningContext.getBinManager().aggregateTemporalBin(spatialBins[i], temporalBins[i]);
                    }
                }
            }
            for (int i=0; i<temporalBins.length; ++i) {
                if (temporalBins[i] != null) {
                    binningContext.getBinManager().completeTemporalBin(temporalBins[i]);
                    if (computeOutput) {
                        temporalBins[i] = temporalBinner.computeOutput(idx, temporalBins[i]);
                        temporalBins[i] = cellChain.process(temporalBins[i]);
                    }
                }
            }
            return temporalBins;
        }
    }

    private MetadataElement aggregateMetadata(Iterable<L3SpatialBinMicroTileWritable> spatialBins) {
        String metadataAggregatorName = binningConfig.getMetadataAggregatorName();
        final MetadataAggregator metadataAggregator = MetadataAggregatorFactory.create(metadataAggregatorName);
        for (L3SpatialBinMicroTileWritable metadataBin : spatialBins) {
            final String metadataXml = metadataBin.getMetadata();
            final MetadataElement metadataElement = metadataSerializer.fromXml(metadataXml);
            metadataAggregator.aggregateMetadata(metadataElement);
        }
        MetadataElement sourcesMetadata = metadataAggregator.getMetadata();
        return createL3Metadata(sourcesMetadata, binningConfig, conf);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // only write this file in the first reducer
        final int partition = context.getTaskAttemptID().getTaskID().getId();
        if (partition == 0) {
            final Map<String, String> metadata = ProcessingMetadata.config2metadata(conf, JobConfigNames.LEVEL3_METADATA_KEYS);
            final String aggregatedMetadataXml = metadataSerializer.toXml(processingGraphMetadata);
            metadata.put(JobConfigNames.PROCESSING_HISTORY, aggregatedMetadataXml);
            final Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            ProcessingMetadata.write(workOutputPath, conf, metadata);
        }
    }

    private static MetadataElement createL3Metadata(MetadataElement sourcesMetadata, BinningConfig binningConfig, Configuration conf) {
        final GlobalMetadata globalMetadata = GlobalMetadata.create(binningConfig);
        final MetadataElement processingGraphMetadata = globalMetadata.asMetadataElement();
        final MetadataElement node_0 = processingGraphMetadata.getElement("node.0");
        final MetadataElement parameters = node_0.getElement("parameters");

        addCalvalusMetadata(node_0, conf);
        addCalvalusParameters(parameters, conf);
        node_0.addElement(sourcesMetadata);

        return processingGraphMetadata;
    }

    private static void addCalvalusMetadata(MetadataElement element, Configuration conf) {
        addAttributeToMetadataElement(element, "operator", conf.get(JobConfigNames.CALVALUS_PRODUCTION_TYPE));
        addAttributeToMetadataElement(element, "calvalusVersion", conf.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE));
        addAttributeToMetadataElement(element, "snapVersion", conf.get(JobConfigNames.CALVALUS_SNAP_BUNDLE));
        addAttributeToMetadataElement(element, "user", conf.get(JobConfigNames.CALVALUS_USER));
    }

    private static void addCalvalusParameters(MetadataElement element, Configuration conf) {
        addAttributeToMetadataElement(element, "aggregation_period_start", conf.get(JobConfigNames.CALVALUS_MIN_DATE));
        addAttributeToMetadataElement(element, "aggregation_period_end", conf.get(JobConfigNames.CALVALUS_MAX_DATE));
        addAttributeToMetadataElement(element, "region_name", conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME));
        addAttributeToMetadataElement(element, "region", conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
    }

    private static void addAttributeToMetadataElement(MetadataElement parent, String name, String value) {
        if (value != null) {
            parent.addAttribute(new MetadataAttribute(name, ProductData.createInstance(value), true));
        }
    }

    private class Mosaic2TemporalBinSource implements TemporalBinSource {

        private final ReducingIterator iterator;

        public Mosaic2TemporalBinSource(Context context, int macroTileCols) throws IOException, InterruptedException {
            iterator = new ReducingIterator(context, macroTileCols);
        }

        @Override
        public int open() throws IOException {
            return 1;
        }

        @Override
        public Iterator<? extends TemporalBin> getPart(int index) throws IOException {
            return iterator;
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }

        public int nextTile() {
            return iterator.nextTile();
        }
    }

    private class ReducingIterator implements Iterator<TemporalBin> {

        private final Context context;
        private final int macroTileCols;
        private TemporalBin[] temporalBins;
        private int previousTile = -1;
        private int currentTile = -1;
        private int cursor = 0;

        public ReducingIterator(Context context, int macroTileCols) {
            this.context = context;
            this.macroTileCols = macroTileCols;
            hasNext();
        }

        public int nextTile() {
            previousTile = currentTile;
            if (hasNext()) {
                return currentTile;
            } else {
                return -1;
            }
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (temporalBins == null || cursor >= temporalBins.length) {
                    try {
                        if (!context.nextKey()) {
                            return false;
                        }
                        TileIndexWritable binIndex = context.getCurrentKey();
                        currentTile = binIndex.getMacroTileY() * macroTileCols + binIndex.getMacroTileX();
                        Iterable<L3SpatialBinMicroTileWritable> spatialBins = context.getValues();
                        temporalBins = aggregate(binIndex, spatialBins);
                        cursor = 0;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                if (currentTile != previousTile) {
                    return false;
                }
                if (temporalBins[cursor] != null) {
                    return true;
                }
                ++cursor;
            }
        }

        @Override
        public TemporalBin next() {
            if (temporalBins == null || cursor >= temporalBins.length) {
                throw new IllegalStateException("next() called with hasNext() false in temporal binning");
            }
            try {
                context.write(new LongWritable(temporalBins[cursor].getIndex()), (L3TemporalBin) temporalBins[cursor]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return temporalBins[cursor++];
        }
    }


    class WrappedContext extends Context {
        final Context delegate;
        boolean lookingAtNext;
        public WrappedContext(Context delegate, boolean lookingAtNext) {
            this.delegate = delegate;
            this.lookingAtNext = lookingAtNext;
        }
        @Override
        public boolean nextKey() throws IOException, InterruptedException {
            if (lookingAtNext) {
                lookingAtNext = false;
                return true;
            }
            return delegate.nextKey();
        }

        @Override
        public Iterable<L3SpatialBinMicroTileWritable> getValues() throws IOException, InterruptedException {
            return delegate.getValues();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return delegate.nextKeyValue();
        }

        @Override
        public TileIndexWritable getCurrentKey() throws IOException, InterruptedException {
            return delegate.getCurrentKey();
        }

        @Override
        public L3SpatialBinMicroTileWritable getCurrentValue() throws IOException, InterruptedException {
            return delegate.getCurrentValue();
        }

        @Override
        public void write(LongWritable key, L3TemporalBin value) throws IOException, InterruptedException {
            delegate.write(key, value);

        }

        @Override
        public OutputCommitter getOutputCommitter() {
            return delegate.getOutputCommitter();
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return delegate.getTaskAttemptID();
        }

        @Override
        public void setStatus(String msg) {
            delegate.setStatus(msg);
        }

        @Override
        public String getStatus() {
            return delegate.getStatus();
        }

        @Override
        public float getProgress() {
            return delegate.getProgress();
        }

        @Override
        public Counter getCounter(Enum<?> counterName) {
            return delegate.getCounter(counterName);
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return delegate.getCounter(groupName, counterName);
        }

        @Override
        public Configuration getConfiguration() {
            return delegate.getConfiguration();
        }

        @Override
        public Credentials getCredentials() {
            return delegate.getCredentials();
        }

        @Override
        public JobID getJobID() {
            return delegate.getJobID();
        }

        @Override
        public int getNumReduceTasks() {
            return delegate.getNumReduceTasks();
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return delegate.getWorkingDirectory();
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return delegate.getOutputKeyClass();
        }

        @Override
        public Class<?> getOutputValueClass() {
            return delegate.getOutputValueClass();
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return delegate.getMapOutputKeyClass();
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return delegate.getMapOutputValueClass();
        }

        @Override
        public String getJobName() {
            return delegate.getJobName();
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
            return delegate.getMapperClass();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
            return delegate.getCombinerClass();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
            return delegate.getReducerClass();
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
            return delegate.getOutputFormatClass();
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
            return delegate.getPartitionerClass();
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return delegate.getSortComparator();
        }

        @Override
        public String getJar() {
            return delegate.getJar();
        }

        @Override
        public RawComparator<?> getCombinerKeyGroupingComparator() {
            return delegate.getCombinerKeyGroupingComparator();
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return delegate.getGroupingComparator();
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return delegate.getJobSetupCleanupNeeded();
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return delegate.getTaskCleanupNeeded();
        }

        @Override
        public boolean getProfileEnabled() {
            return delegate.getProfileEnabled();
        }

        @Override
        public String getProfileParams() {
            return delegate.getProfileParams();
        }

        @Override
        public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
            return delegate.getProfileTaskRange(isMap);
        }

        @Override
        public String getUser() {
            return delegate.getUser();
        }

        @Override
        public boolean getSymlink() {
            return delegate.getSymlink();
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return delegate.getArchiveClassPaths();
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return delegate.getCacheArchives();
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return delegate.getCacheFiles();
        }

        @Override
        public Path[] getLocalCacheArchives() throws IOException {
            return delegate.getLocalCacheArchives();
        }

        @Override
        public Path[] getLocalCacheFiles() throws IOException {
            return delegate.getLocalCacheFiles();
        }

        @Override
        public Path[] getFileClassPaths() {
            return delegate.getFileClassPaths();
        }

        @Override
        public String[] getArchiveTimestamps() {
            return delegate.getArchiveTimestamps();
        }

        @Override
        public String[] getFileTimestamps() {
            return delegate.getFileTimestamps();
        }

        @Override
        public int getMaxMapAttempts() {
            return delegate.getMaxMapAttempts();
        }

        @Override
        public int getMaxReduceAttempts() {
            return delegate.getMaxReduceAttempts();
        }

        @Override
        public void progress() {
            delegate.progress();
        }
    }
    
}
