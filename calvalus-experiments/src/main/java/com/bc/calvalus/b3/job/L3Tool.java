package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.Aggregator;
import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.AggregatorAverageML;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinManagerImpl;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.SpatialBin;
import com.bc.calvalus.b3.TemporalBin;
import com.bc.calvalus.b3.Vector;
import com.bc.calvalus.b3.WritableVector;
import com.bc.calvalus.experiments.processing.N1InputFormat;
import com.bc.calvalus.experiments.util.Args;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Creates and runs Hadoop job for L3 processing. Expects input directory
 * with MERIS L1 product(s) and output directory path to be created and filled
 * with outputs.
 * <p/>
 * Call with:
 * <pre>
 *    hadoop jar target/calvalus-experiments-0.1-SNAPSHOT-job.jar \
 *    com.bc.calvalus.experiments.processing.L2ProcessingTool \
 *    hdfs://cvmaster00:9000/input \
 *    hdfs://cvmaster00:9000/output \
 *    (n1|n3|sliced|lineinterleaved) \
 *    [ndvi|radiometry|c2r] [-splits=n] [-tileHeight=h]
 * </pre>
 */
public class L3Tool extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public int run(String[] args) throws Exception {

        try {
            // parse command line arguments
            Args options = new Args(args);
            String destination = options.getArgs()[0];
            LOG.info(MessageFormat.format("start L3 processing to {0}", destination));
            long startTime = System.nanoTime();

            // construct job and set parameters and handlers
            Job job = new Job(getConf(), "L3");
            job.setJarByClass(getClass());

            Configuration configuration = job.getConfiguration();
            configuration.setInt("io.file.buffer.size", 1024 * 1024); // default is 4096

            configuration.set("mapred.map.tasks.speculative.execution", "false");
            configuration.set("mapred.reduce.tasks.speculative.execution", "false");
            // disable reuse for now....
            // job.getConfiguration().set("mapred.job.reuse.jvm.num.tasks", "-1");
            configuration.set("mapred.child.java.opts", "-Xmx1024m");
            int numReducers = 10;
            job.setNumReduceTasks(numReducers);

            if (configuration.get(L3Mapper.CONFNAME_L3_NUM_SCANS_PER_SLICE) == null) {
                configuration.setInt(L3Mapper.CONFNAME_L3_NUM_SCANS_PER_SLICE, 64);
            }
            if (configuration.get(L3Mapper.CONFNAME_L3_NUM_ROWS) == null) {
                configuration.setInt(L3Mapper.CONFNAME_L3_NUM_ROWS, 3 * IsinBinningGrid.DEFAULT_NUM_ROWS);
            }

            job.setInputFormatClass(N1InputFormat.class);
            configuration.setInt(N1InputFormat.NUMBER_OF_SPLITS, 1);

            job.setMapperClass(L3Mapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(SpatialBin.class);

            job.setPartitionerClass(L3Partitioner.class);

            job.setReducerClass(L3Reducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(TemporalBin.class);

            if (false) {
                job.setOutputFormatClass(TextOutputFormat.class);
            } else {
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
            }

            // provide input and output directories to job
            for (int day = 1; day <= 30; day++) {
                String pathName = String.format("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/%02d", day);
                FileInputFormat.addInputPath(job, new Path(pathName));
            }
            Path output = new Path(destination);
            FileOutputFormat.setOutputPath(job, output);

            int result = 0;
            //result = job.waitForCompletion(true) ? 0 : 1;
            long stopTime = System.nanoTime();
            LOG.info(MessageFormat.format("stop L3 processing after {0} sec", (stopTime - startTime) / 1E9));

            processL3Output(configuration, output, job.getNumReduceTasks());

            return result;

        } catch (Exception ex) {

            System.err.println("failed: " + ex.getMessage());
            ex.printStackTrace(System.err);
            return 1;

        }

    }

    private void processL3Output(Configuration configuration, Path output, int numParts) throws IOException {
        // todo - use config to construct the correct list of aggregators
        AggregatorAverage aggregator = new AggregatorAverage(new MyVariableContext(), "ndvi");
        BinManager binManager = new BinManagerImpl(aggregator);

        LOG.info(MessageFormat.format("start reprojection, collecting {0} parts", numParts));
        int height = configuration.getInt(L3Mapper.CONFNAME_L3_NUM_ROWS, -1);
        BinningGrid binningGrid = new IsinBinningGrid(height);
        int width = height * 2;
        float[] imageData = new float[width * height];

        int numObsMaxTotal = -1;
        int numPassesMaxTotal = -1;

        long startTime = System.nanoTime();

        for (int i = 0; i < numParts; i++) {
            Path partFile = new Path(output, String.format("part-r-%05d", i));
            SequenceFile.Reader reader = new SequenceFile.Reader(partFile.getFileSystem(configuration), partFile, configuration);

            int numObsMaxPart = -1;
            int numPassesMaxPart = -1;

            LOG.info(MessageFormat.format("reading part {0}", partFile));

            try {
                int lastRowIndex = -1;
                ArrayList<TemporalBin> binRow = new ArrayList<TemporalBin>();
                while (true) {
                    IntWritable binIndex = new IntWritable();
                    TemporalBin temporalBin = new TemporalBin();
                    if (!reader.next(binIndex, temporalBin)) {
                        processBinRow(binningGrid, binManager, lastRowIndex, binRow, imageData, width, height);
                        binRow.clear();
                        break;
                    }
                    int rowIndex = binningGrid.getRowIndex(binIndex.get());
                    if (rowIndex != lastRowIndex) {
                        processBinRow(binningGrid, binManager, lastRowIndex, binRow, imageData, width, height);
                        binRow.clear();
                        lastRowIndex = rowIndex;
                    }
                    temporalBin.setIndex(binIndex.get());
                    binRow.add(temporalBin);

                    numObsMaxPart = Math.max(numObsMaxPart, temporalBin.getNumObs());
                    numPassesMaxPart = Math.max(numPassesMaxPart, temporalBin.getNumPasses());
                }
            } finally {
                reader.close();
            }

            LOG.info(MessageFormat.format("numObsMaxPart = {0}, numPassesMaxPart = {1}", numObsMaxPart, numPassesMaxPart));

            numObsMaxTotal = Math.max(numObsMaxTotal, numObsMaxPart);
            numPassesMaxTotal = Math.max(numPassesMaxTotal, numPassesMaxPart);
        }
        long stopTime = System.nanoTime();

        LOG.info(MessageFormat.format("numObsMaxTotal = {0}, numPassesMaxTotal = {1}", numObsMaxTotal, numPassesMaxTotal));
        LOG.info(MessageFormat.format("stop reprojection after {0} sec", (stopTime - startTime) / 1E9));

        File outputImageFile = new File("level3.png");
        LOG.info(MessageFormat.format("writing image {0}", outputImageFile));

        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
        byte[] data = dataBuffer.getData();
        float factor = 255 / 0.8f;
        for (int i = 0; i < imageData.length; i++) {
            int sample = (int) (factor * imageData[i]);
            if (sample < 0) {
                sample = 0;
            } else if (sample > 255) {
                sample = 255;
            }
            data[i] = (byte) (sample);
        }
        ImageIO.write(image, "PNG", outputImageFile);
    }

    static void processBinRow(BinningGrid binningGrid, BinManager binManager, int y, List<TemporalBin> binRow, float[] imageData, int width, int height) {
        if (y >= 0 && !binRow.isEmpty()) {
//            LOG.info("row " + y + ": processing " + binRow.size() + " bins, bin #0 = " + binRow.get(0));
            processBinRow0(binningGrid, binManager, y, binRow, imageData, width, height, false);
        } else {
//            LOG.info("row " + y + ": no bins");
        }
    }

    static void processBinRow0(BinningGrid binningGrid, BinManager binManager, int y, List<TemporalBin> binRow, float[] imageData, int width, int height, boolean debug) {
        int offset = y * width;
        double lat = -90.0 + (y + 0.5) * 180.0 / height;
        int lastBinIndex = -1;
        TemporalBin temporalBin = null;
        int rowIndex = -1;
        float lastMean = Float.NaN;
         WritableVector outputVector = binManager.createOutputVector();
        for (int x = 0; x < width; x++) {
            double lon = -180.0 + (x + 0.5) * 360.0 / width;
            int wantedBinIndex = binningGrid.getBinIndex(lat, lon);
            if (lastBinIndex != wantedBinIndex) {
                //search
                temporalBin = null;
                for (int i = rowIndex + 1; i < binRow.size(); i++) {
                    if (wantedBinIndex == binRow.get(i).getIndex()) {
                        temporalBin = binRow.get(i);
                        binManager.computeOutput(temporalBin, outputVector);
                        lastMean = outputVector.get(0);
                        lastBinIndex = wantedBinIndex;
                        rowIndex = i;
                        break;
                    }
                }
            }
            if (temporalBin != null) {
                if (debug) {
                    imageData[offset + x] = temporalBin.getNumObs();
                } else {
                    imageData[offset + x] = lastMean;
                }
            } else {
                imageData[offset + x] = Float.NaN;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new L3Tool(), args));
    }
}
