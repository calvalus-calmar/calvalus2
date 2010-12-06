package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.Aggregator;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinningContext;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.TemporalBin;
import com.bc.calvalus.b3.WritableVector;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import javax.imageio.ImageIO;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Formatter for the outputs generated by the L3Tool.
 * <pre>
 *   Usage:
 *       <b>L3Formatter</b> <i>input-dir</i> <i>output-file</i> <b>RGB</b>  <i>r-band</i> <i>r-v-min</i> <i>r-v-max</i>  <i>g-band</i> <i>g-v-min</i> <i>g-v-max</i>  <i>b-band</i> <i>b-v-min</i> <i>b-v-max</i>
 *   or
 *       <b>L3Formatter</b> <i>input-dir</i> <i>output-file</i> <b>Grey</b>  <i>band</i> <i>v-min</i> <i>v-max</i>  [ <i>band</i> <i>v-min</i> <i>v-max</i> ... ]
 * </pre>
 *
 * @author Norman Fomferra
 */
public class L3Formatter extends Configured implements Tool {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private Properties request;
    private String outputType;
    private String outputFormat;
    private Path l3OutputDir;
    private BinningContext binningContext;
    private File outputFile;
    private String outputFileNameBase;
    private String outputFileNameExt;
    private double pixelSize;
    private Rectangle outputRegion;

    public static void main(String[] args) {
        if (Boolean.getBoolean("hadoop.debug")) {
            waitForDebuggerToConnect();
        }
        int result;
        try {
            result = ToolRunner.run(new L3Formatter(), args);
        } catch (Exception ex) {
            System.err.println("Error: " + ex.getMessage());
            ex.printStackTrace(System.err);
            result = -1;
        }
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {

        final String requestFile = args.length > 0 ? args[0] : "l3formatter.properties";
        request = L3Config.readProperties(new FileReader(requestFile));

        outputType = request.getProperty("calvalus.l3.formatter.outputType");
        if (outputType == null) {
            throw new IllegalArgumentException("No output type given");
        }
        if (!outputType.equalsIgnoreCase("Product")
                && !outputType.equalsIgnoreCase("RGB")
                && !outputType.equalsIgnoreCase("Grey")) {
            throw new IllegalArgumentException("Unknown output type: " + outputType);
        }

        outputFile = new File(request.getProperty("calvalus.l3.formatter.output"));
        final String fileName = outputFile.getName();
        final int extPos = fileName.lastIndexOf(".");
        outputFileNameBase = fileName.substring(0, extPos);
        outputFileNameExt = fileName.substring(extPos + 1);

        outputFormat = request.getProperty("calvalus.l3.formatter.outputFormat");
        if (outputFormat == null) {
            outputFormat =
                    outputFileNameExt.equalsIgnoreCase("nc") ? "NetCDF"
                            : outputFileNameExt.equalsIgnoreCase("dim") ? "BEAM-DIMAP"
                            : outputFileNameExt.equalsIgnoreCase("png") ? "PNG"
                            : outputFileNameExt.equalsIgnoreCase("jpg") ? "JPEG" : null;
        }
        if (outputFormat == null) {
            throw new IllegalArgumentException("No output format given");
        }
        if (!outputFormat.equalsIgnoreCase("NetCDF")
                && !outputFormat.equalsIgnoreCase("BEAM-DIMAP")
                && !outputFormat.equalsIgnoreCase("PNG")
                && !outputFormat.equalsIgnoreCase("JPEG")) {
            throw new IllegalArgumentException("Unknown output format: " + outputFormat);
        }


        l3OutputDir = new Path(request.getProperty(L3Config.CONFNAME_L3_OUTPUT));
        L3Config l3Config = L3Config.load(l3OutputDir.getFileSystem(getConf()),
                                          new Path(l3OutputDir, L3Config.L3_REQUEST_PROPERTIES_FILENAME));

        binningContext = l3Config.getBinningContext();
        final BinManager binManager = binningContext.getBinManager();
        final int aggregatorCount = binManager.getAggregatorCount();
        if (aggregatorCount == 0) {
            throw new IllegalArgumentException("Illegal binning context: aggregatorCount == 0");
        }

        LOG.info("aggregators.length = " + aggregatorCount);
        for (int i = 0; i < aggregatorCount; i++) {
            Aggregator aggregator = binManager.getAggregator(i);
            LOG.info("aggregators." + i + " = " + aggregator);
        }


        computeOutputRegion(l3Config);

        if (outputType.equalsIgnoreCase("Product")) {
            writeProductFile(l3Config.getStartTime(), l3Config.getEndTime());
        } else {
            writeImageFiles();
        }

        return 0;
    }

    private void computeOutputRegion(L3Config l3Config) {
        final Geometry roiGeometry = l3Config.getRegionOfInterest();
        final BinningGrid binningGrid = binningContext.getBinningGrid();

        final int gridWidth = binningGrid.getNumRows() * 2;
        final int gridHeight = binningGrid.getNumRows();
        pixelSize = 180.0 / gridHeight;
        outputRegion = new Rectangle(gridWidth, gridHeight);
        if (roiGeometry != null) {
            final Coordinate[] coordinates = roiGeometry.getBoundary().getCoordinates();
            double gxmin = Double.POSITIVE_INFINITY;
            double gxmax = Double.NEGATIVE_INFINITY;
            double gymin = Double.POSITIVE_INFINITY;
            double gymax = Double.NEGATIVE_INFINITY;
            for (Coordinate coordinate : coordinates) {
                gxmin = Math.min(gxmin, coordinate.x);
                gxmax = Math.max(gxmax, coordinate.x);
                gymin = Math.min(gymin, coordinate.y);
                gymax = Math.max(gymax, coordinate.y);
            }
            final int x = (int) Math.floor((180.0 + gxmin) / pixelSize);
            final int y = (int) Math.floor((90.0 - gymax) / pixelSize);
            final int width = (int) Math.ceil((gxmax - gxmin) / pixelSize);
            final int height = (int) Math.ceil((gymax - gymin) / pixelSize);
            final Rectangle unclippedOutputRegion = new Rectangle(x, y, width, height);
            LOG.info("unclippedOutputRegion = " + unclippedOutputRegion);
            outputRegion = unclippedOutputRegion.intersection(outputRegion);
        }
        LOG.info("outputRegion = " + outputRegion);
    }


    private void writeProductFile(ProductData.UTC startTime, ProductData.UTC endTime) throws Exception {
        final ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }

        CrsGeoCoding geoCoding;
        try {
            geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                                         outputRegion.width,
                                         outputRegion.height,
                                         180.0 + pixelSize * outputRegion.x,
                                         90.0 - pixelSize * outputRegion.y,
                                         pixelSize,
                                         -pixelSize,
                                         0.0, 0.0);
        } catch (FactoryException e) {
            throw new IllegalStateException(e);
        } catch (TransformException e) {
            throw new IllegalStateException(e);
        }
        final Product product = new Product(outputFile.getName(), "CALVALUS-L3", outputRegion.width, outputRegion.height);
        product.setGeoCoding(geoCoding);
        product.setStartTime(startTime);
        product.setEndTime(endTime);

        final Band indexBand = product.addBand("index", ProductData.TYPE_INT32);
        indexBand.setNoDataValue(-1);
        final ProductData indexLine = indexBand.createCompatibleRasterData(outputRegion.width, 1);

        final Band numObsBand = product.addBand("num_obs", ProductData.TYPE_INT16);
        numObsBand.setNoDataValue(-1);
        final ProductData numObsLine = numObsBand.createCompatibleRasterData(outputRegion.width, 1);

        final Band numPassesBand = product.addBand("num_passes", ProductData.TYPE_INT16);
        numPassesBand.setNoDataValue(-1);
        final ProductData numPassesLine = numPassesBand.createCompatibleRasterData(outputRegion.width, 1);

        int outputPropertyCount = binningContext.getBinManager().getOutputPropertyCount();
        final Band[] outputBands = new Band[outputPropertyCount];
        final ProductData[] outputLines = new ProductData[outputPropertyCount];
        for (int i = 0; i < outputPropertyCount; i++) {
            String name = binningContext.getBinManager().getOutputPropertyName(i);
            outputBands[i] = product.addBand(name, ProductData.TYPE_FLOAT32);
            outputBands[i].setNoDataValue(Double.NaN);
            outputLines[i] = outputBands[i].createCompatibleRasterData(outputRegion.width, 1);
        }

        productWriter.writeProductNodes(product, outputFile);
        final ProductDataWriter dataWriter = new ProductDataWriter(productWriter,
                                                                   indexBand, indexLine,
                                                                   numObsBand, numObsLine,
                                                                   numPassesBand, numPassesLine,
                                                                   outputBands, outputLines);
        L3Reprojector.reproject(getConf(), binningContext, outputRegion, l3OutputDir, dataWriter);
        productWriter.close();
    }

    private void writeImageFiles() throws Exception {
        int[] indices = new int[16];
        String[] names = new String[16];
        float[] v1s = new float[16];
        float[] v2s = new float[16];
        int numBands = 0;
        for (int i = 0; i < indices.length; i++) {
            String indexStr = request.getProperty(String.format("calvalus.l3.formatter.bands.%d.index", i));
            String nameStr = request.getProperty(String.format("calvalus.l3.formatter.bands.%d.name", i));
            String v1Str = request.getProperty(String.format("calvalus.l3.formatter.bands.%d.v1", i));
            String v2Str = request.getProperty(String.format("calvalus.l3.formatter.bands.%d.v2", i));
            if (indexStr == null) {
                break;
            }
            indices[numBands] = Integer.parseInt(indexStr);
            names[numBands] = nameStr != null ? nameStr : binningContext.getBinManager().getOutputPropertyName(indices[numBands]);
            v1s[numBands] = Float.parseFloat(v1Str);
            v2s[numBands] = Float.parseFloat(v2Str);
            numBands++;
        }
        if (numBands == 0) {
            throw new IllegalArgumentException("No output band given.");
        }
        indices = Arrays.copyOf(indices, numBands);
        names = Arrays.copyOf(names, numBands);
        v1s = Arrays.copyOf(v1s, numBands);
        v2s = Arrays.copyOf(v2s, numBands);

        final ImageRaster raster = new ImageRaster(outputRegion.width, outputRegion.height, indices);
        L3Reprojector.reproject(getConf(), binningContext, outputRegion, l3OutputDir, raster);

        if (outputType.equalsIgnoreCase("RGB")) {
            writeRgbImage(outputRegion.width, outputRegion.height, raster.getBandData(), v1s, v2s, outputFormat, outputFile);
        } else {
            for (int i = 0; i < numBands; i++) {
                final String fileName = String.format("%s_%s.%s", outputFileNameBase, names[i], outputFileNameExt);
                final File imageFile = new File(outputFile.getParentFile(), fileName);
                writeGrayScaleImage(outputRegion.width, outputRegion.height, raster.getBandData(i), v1s[i], v2s[i], outputFormat, imageFile);
            }
        }
    }


    private static void writeGrayScaleImage(int width, int height,
                                            float[] rawData,
                                            float rawValue1, float rawValue2,
                                            String outputFormat, File outputImageFile) throws IOException {

        LOG.info(MessageFormat.format("writing image {0}", outputImageFile));
        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        final DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
        final byte[] data = dataBuffer.getData();
        final float a = 255f / (rawValue2 - rawValue1);
        final float b = -255f * rawValue1 / (rawValue2 - rawValue1);
        for (int i = 0; i < rawData.length; i++) {
            data[i] = toByte(rawData[i], a, b);
        }
        ImageIO.write(image, outputFormat, outputImageFile);
    }

    private static void writeRgbImage(int width, int height,
                                      float[][] rawData,
                                      float[] rawValue1, float[] rawValue2,
                                      String outputFormat, File outputImageFile) throws IOException {
        LOG.info(MessageFormat.format("writing image {0}", outputImageFile));
        final BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_3BYTE_BGR);
        final DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
        final byte[] data = dataBuffer.getData();
        final float[] rawDataR = rawData[0];
        final float[] rawDataG = rawData[1];
        final float[] rawDataB = rawData[2];
        final float aR = 255f / (rawValue2[0] - rawValue1[0]);
        final float bR = -255f * rawValue1[0] / (rawValue2[0] - rawValue1[0]);
        final float aG = 255f / (rawValue2[1] - rawValue1[1]);
        final float bG = -255f * rawValue1[1] / (rawValue2[1] - rawValue1[1]);
        final float aB = 255f / (rawValue2[2] - rawValue1[2]);
        final float bB = -255f * rawValue1[2] / (rawValue2[2] - rawValue1[2]);
        final int n = width * height;
        for (int i = 0, j = 0; i < n; i++, j += 3) {
            data[j + 2] = toByte(rawDataR[i], aR, bR);
            data[j + 1] = toByte(rawDataG[i], aG, bG);
            data[j] = toByte(rawDataB[i], aB, bB);
        }
        ImageIO.write(image, outputFormat, outputImageFile);
    }

    private static byte toByte(float s, float a, float b) {
        int sample = (int) (a * s + b);
        if (sample < 0) {
            sample = 0;
        } else if (sample > 255) {
            sample = 255;
        }
        return (byte) sample;
    }

    private static void waitForDebuggerToConnect() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        // Halt here so that Norman can start the IDE debugger
        System.out.print("Connect debugger to the JVM and press return!");
        try {
            reader.readLine();
        } catch (IOException e) {
            // ?
        }
    }

    private final static class ImageRaster extends L3Reprojector.TemporalBinProcessor {
        private final int rasterWidth;
        private final int[] bandIndices;
        private final float[][] bandData;

        private final int bandCount;


        public ImageRaster(int rasterWidth, int rasterHeight, int[] bandIndices) {
            this.rasterWidth = rasterWidth;
            this.bandIndices = bandIndices.clone();
            this.bandCount = bandIndices.length;
            this.bandData = new float[bandCount][rasterWidth * rasterHeight];
            for (int i = 0; i < bandCount; i++) {
                Arrays.fill(bandData[i], Float.NaN);
            }
        }

        public float[][] getBandData() {
            return bandData;
        }

        public float[] getBandData(int bandIndex) {
            return this.bandData[bandIndex];
        }

        @Override
        public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) {
            for (int i = 0; i < bandCount; i++) {
                bandData[i][rasterWidth * y + x] = outputVector.get(bandIndices[i]);
            }
        }

        @Override
        public void processMissingBin(int x, int y) throws Exception {
            for (int i = 0; i < bandCount; i++) {
                bandData[i][rasterWidth * y + x] = Float.NaN;
            }
        }
    }

    private final static class ProductDataWriter extends L3Reprojector.TemporalBinProcessor {
        private final int width;
        private final ProductData indexLine;
        private final ProductData numObsLine;
        private final ProductData numPassesLine;
        private final Band[] outputBands;
        private final ProductData[] outputLines;
        private final ProductWriter productWriter;
        private final Band indexBand;
        private final Band numObsBand;
        private final Band numPassesBand;
        int yLast;

        public ProductDataWriter(ProductWriter productWriter, Band indexBand, ProductData indexLine, Band numObsBand, ProductData numObsLine, Band numPassesBand, ProductData numPassesLine, Band[] outputBands, ProductData[] outputLines) {
            this.indexLine = indexLine;
            this.numObsLine = numObsLine;
            this.numPassesLine = numPassesLine;
            this.outputBands = outputBands;
            this.outputLines = outputLines;
            this.productWriter = productWriter;
            this.indexBand = indexBand;
            this.numObsBand = numObsBand;
            this.numPassesBand = numPassesBand;
            this.width = indexBand.getSceneRasterWidth();
            this.yLast = 0;
            initLine();
        }

        @Override
        public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception {
            setData(x, temporalBin, outputVector);
            if (y != yLast) {
                completeLine();
                yLast = y;
            }
        }

        @Override
        public void processMissingBin(int x, int y) throws Exception {
            setNoData(x);
            if (y != yLast) {
                completeLine();
                yLast = y;
            }
        }

        @Override
        public void end(BinningContext ctx) throws Exception {
            completeLine();
        }

        private void completeLine() throws IOException {
            writeLine(yLast);
            initLine();
        }

        private void writeLine(int y) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
            productWriter.writeBandRasterData(indexBand, 0, y, width, 1, indexLine, ProgressMonitor.NULL);
            productWriter.writeBandRasterData(numObsBand, 0, y, width, 1, numObsLine, ProgressMonitor.NULL);
            productWriter.writeBandRasterData(numPassesBand, 0, y, width, 1, numPassesLine, ProgressMonitor.NULL);
            for (int i = 0; i < outputBands.length; i++) {
                productWriter.writeBandRasterData(outputBands[i], 0, y, width, 1, outputLines[i], ProgressMonitor.NULL);
            }
        }

        private void initLine() {
            for (int x = 0; x < width; x++) {
                setNoData(x);
            }
        }

        private void setData(int x, TemporalBin temporalBin, WritableVector outputVector) {
            indexLine.setElemIntAt(x, temporalBin.getIndex());
            numObsLine.setElemIntAt(x, temporalBin.getNumObs());
            numPassesLine.setElemIntAt(x, temporalBin.getNumPasses());
            for (int i = 0; i < outputBands.length; i++) {
                outputLines[i].setElemFloatAt(x, outputVector.get(i));
            }
        }

        private void setNoData(int x) {
            indexLine.setElemIntAt(x, -1);
            numObsLine.setElemIntAt(x, -1);
            numPassesLine.setElemIntAt(x, -1);
            for (int i = 0; i < outputBands.length; i++) {
                outputLines[i].setElemFloatAt(x, Float.NaN);
            }
        }

    }
}

