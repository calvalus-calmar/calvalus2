package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.*;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

/**
 * Generates output records from a {@link Product} using an input reference record.
 *
 * @author MarcoZ
 * @author Norman
 */
public class PixelExtractor {
    private final Product product;
    private final PixelTimeProvider pixelTimeProvider;
    private final boolean copyInput;
    private final boolean usePixelMask;
    private final boolean useMaxTimeDifference;
    private final double maxTimeDifference;
    private final int macroPixelSize;
    private final Header header;
    private final Mask pixelMask;

    public PixelExtractor(Header inputHeader,
                          Product product,
                          int macroPixelSize,
                          boolean usePixelMask,
                          boolean useMaxTimeDifference,
                          double maxTimeDifference,
                          boolean copyInput) {
        this.product = product;
        if (usePixelMask) {
            this.pixelMask = product.getMaskGroup().get(ProductRecordSource.GOOD_PIXEL_MASK_NAME);
            if (pixelMask == null) {
                throw new IllegalArgumentException(String.format("Missing mask '%s' in product",
                                                                 ProductRecordSource.GOOD_PIXEL_MASK_NAME));
            }
        } else {
            this.pixelMask = null;
        }
        this.usePixelMask = usePixelMask;
        this.macroPixelSize = macroPixelSize;
        this.useMaxTimeDifference = useMaxTimeDifference;
        this.maxTimeDifference = maxTimeDifference;
        this.pixelTimeProvider = PixelTimeProvider.create(product);
        this.copyInput = copyInput;

        // Important note: createHeader() is dependent on a number of field values,
        // so we call it at last
        this.header = createHeader(inputHeader);
    }

    public Header getHeader() {
        return header;
    }

    /**
     * Extracts an output record.
     *
     * @param inputRecord The input record.
     * @return The output record or {@code null}, if a certain inclusion criterion is not met.
     * @throws java.io.IOException If any I/O error occurs
     */
    public Record extract(Record inputRecord) throws IOException {
        Assert.notNull(inputRecord, "inputRecord");
        final PixelPos pixelPos = getTemporallyAndSpatiallyValidPixelPos(inputRecord);
        if (pixelPos != null) {
            return extract(inputRecord, pixelPos);
        }
        return null;
    }

    /**
     * Extracts an output record.
     *
     * @param inputRecord The input record.
     * @param pixelPos    The validated pixel pos.
     * @return The output record or {@code null}, if a certain inclusion criterion is not met.
     * @throws java.io.IOException If an I/O error occurs
     */
    public Record extract(Record inputRecord, PixelPos pixelPos) throws IOException {


        final Rectangle macroPixelRect = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight()).intersection(
                new Rectangle((int) pixelPos.x - macroPixelSize / 2,
                              (int) pixelPos.y - macroPixelSize / 2,
                              macroPixelSize, macroPixelSize));


        int x0 = macroPixelRect.x;
        int y0 = macroPixelRect.y;
        int width = macroPixelRect.width;
        int height = macroPixelRect.height;

        final int[] maskSamples;
        if (pixelMask != null) {
            maskSamples = new int[width * height];
            pixelMask.readPixels(macroPixelRect.x, macroPixelRect.y, macroPixelRect.width, macroPixelRect.height, maskSamples);
            boolean allBad = true;
            for (int i = 0; i < maskSamples.length; i++) {
                int sample = maskSamples[i];
                if (sample != 0) {
                    maskSamples[i] = 1;
                    allBad = false;
                }
            }
            if (allBad) {
                return null;
            }
        } else {
            maskSamples = null;
        }

        final Object[] values = new Object[header.getAttributeNames().length];

        int index = 0;
        if (copyInput) {
            Object[] inputValues = inputRecord.getAttributeValues();
            System.arraycopy(inputValues, 0, values, 0, inputValues.length);
            index = inputValues.length;
        }

        final int[] pixelXPositions = new int[width * height];
        final int[] pixelYPositions = new int[width * height];
        final float[] pixelLatitudes = new float[width * height];
        final float[] pixelLongitudes = new float[width * height];

        for (int i = 0, y = y0; y < y0 + height; y++) {
            for (int x = x0; x < x0 + width; x++, i++) {
                PixelPos pp = new PixelPos(x + 0.5F, y + 0.5F);
                GeoPos gp = product.getGeoCoding().getGeoPos(pp, null);
                // todo - compute actual source pixel positions (need offsets here!)  (mz,nf)
                pixelXPositions[i] = x;
                pixelYPositions[i] = y;
                pixelLatitudes[i] = gp.lat;
                pixelLongitudes[i] = gp.lon;
            }
        }

        // field "source_name"
        values[index++] = product.getName();
        // field "pixel_x"
        values[index++] = pixelXPositions;
        // field "pixel_y"
        values[index++] = pixelYPositions;
        // field "pixel_lat"
        values[index++] = pixelLatitudes;
        // field "pixel_lon"
        values[index++] = pixelLongitudes;
        // field "pixel_time"
        values[index++] = pixelTimeProvider != null ? pixelTimeProvider.getTime(pixelPos) : null;
        if (maskSamples != null) {
            // field "pixel_mask"
            values[index++] = maskSamples;
        }

        final Band[] productBands = product.getBands();
        for (Band band : productBands) {
            if (!band.isFlagBand()) {
                if (band.isFloatingPointType()) {
                    final float[] floatSamples = new float[macroPixelRect.width * macroPixelRect.height];
                    band.readPixels(x0, y0, width, height, floatSamples);
                    values[index++] = floatSamples;
                    maskNaN(band, x0, y0, width, height, floatSamples);
                } else {
                    final int[] intSamples = new int[macroPixelRect.width * macroPixelRect.height];
                    band.readPixels(x0, y0, width, height, intSamples);
                    values[index++] = intSamples;
                }
            }
        }

        for (TiePointGrid tiePointGrid : product.getTiePointGrids()) {
            final float[] floatSamples = new float[macroPixelRect.width * macroPixelRect.height];
            tiePointGrid.readPixels(x0, y0, width, height, floatSamples);
            values[index++] = floatSamples;
        }

        return new DefaultRecord(inputRecord.getLocation(), inputRecord.getTime(), values);
    }

    public PixelPos getPixelPos(Record referenceRecord) {
        return getTemporallyAndSpatiallyValidPixelPos(referenceRecord);
    }

    private PixelPos getTemporallyAndSpatiallyValidPixelPos(Record referenceRecord) {

        if (useMaxTimeDifference) {

            long minReferenceTime = getMinReferenceTime(referenceRecord);
            if (minReferenceTime > product.getEndTime().getAsDate().getTime()) {
                return null;
            }

            long maxReferenceTime = getMaxReferenceTime(referenceRecord);
            if (maxReferenceTime < product.getStartTime().getAsDate().getTime()) {
                return null;
            }

            PixelPos pixelPos = getSpatiallyValidPixelPos(referenceRecord);
            if (pixelPos != null) {
                long pixelTime = pixelTimeProvider.getTime(pixelPos).getTime();
                if (pixelTime >= minReferenceTime && pixelTime <= maxReferenceTime) {
                    return pixelPos;
                }
            }
        } else {
            PixelPos pixelPos = getSpatiallyValidPixelPos(referenceRecord);
            if (pixelPos != null) {
                return pixelPos;
            }
        }
        return null;
    }

    private PixelPos getSpatiallyValidPixelPos(Record referenceRecord) {
        GeoPos location = referenceRecord.getLocation();
        if (location == null) {
            return null;
        }
        final PixelPos pixelPos = product.getGeoCoding().getPixelPos(location, null);
        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            return pixelPos;
        }
        return null;
    }

    private long getMinReferenceTime(Record referenceRecord) {
        return referenceRecord.getTime().getTime() - getMaxTimeDifferenceInMillis();
    }

    private long getMaxReferenceTime(Record referenceRecord) {
        return referenceRecord.getTime().getTime() + getMaxTimeDifferenceInMillis();
    }

    private long getMaxTimeDifferenceInMillis() {
        return Math.round(maxTimeDifference * 60 * 60 * 1000);
    }

    private void maskNaN(Band band, int x0, int y0, int width, int height, float[] samples) {
        for (int i = 0, y = y0; y < y0 + height; y++) {
            for (int x = x0; x < x0 + width; x++, i++) {
                if (!band.isPixelValid(x0, y0)) {
                    samples[i] = Float.NaN;
                }
            }
        }
    }

    private Header createHeader(Header inputHeader) {
        final java.util.List<String> attributeNames = new ArrayList<String>();

        if (copyInput) {
            Collections.addAll(attributeNames, inputHeader.getAttributeNames());
        }

        // 0. derived information
        attributeNames.add(ProductRecordSource.SOURCE_NAME_ATT_NAME);
        attributeNames.add(ProductRecordSource.PIXEL_X_ATT_NAME);
        attributeNames.add(ProductRecordSource.PIXEL_Y_ATT_NAME);
        attributeNames.add(ProductRecordSource.PIXEL_LAT_ATT_NAME);
        attributeNames.add(ProductRecordSource.PIXEL_LON_ATT_NAME);
        attributeNames.add(ProductRecordSource.PIXEL_TIME_ATT_NAME);
        if (usePixelMask) {
            attributeNames.add(ProductRecordSource.PIXEL_MASK_ATT_NAME);
        }

        // 1. bands
        Band[] productBands = product.getBands();
        for (Band band : productBands) {
            if (!band.isFlagBand()) {
                attributeNames.add(band.getName());
            }
        }

        // 2. flags (virtual bands)
        for (Band band : productBands) {
            if (band.isFlagBand()) {
                FlagCoding flagCoding = band.getFlagCoding();
                String[] flagNames = flagCoding.getFlagNames();
                for (String flagName : flagNames) {
                    attributeNames.add(band.getName() + "." + flagName);
                    // todo - move this side-effect into a separate method
                    product.addBand("flag_" + band.getName() + "_" + flagName, band.getName() + "." + flagName, ProductData.TYPE_INT8);
                }
            }
        }

        // 3. tie-points
        attributeNames.addAll(Arrays.asList(product.getTiePointGridNames()));

        return new DefaultHeader(true,
                                 pixelTimeProvider != null,
                                 attributeNames.toArray(new String[attributeNames.size()]));
    }

    private static class PixelTimeProvider {

        private final double startMJD;
        private final double deltaMJD;

        static PixelTimeProvider create(Product product) {
            final ProductData.UTC startTime = product.getStartTime();
            final ProductData.UTC endTime = product.getEndTime();
            final int rasterHeight = product.getSceneRasterHeight();
            if (startTime != null && endTime != null && rasterHeight > 1) {
                return new PixelTimeProvider(startTime.getMJD(),
                                             (endTime.getMJD() - startTime.getMJD()) / (rasterHeight - 1));
            } else {
                return null;
            }
        }

        private PixelTimeProvider(double startMJD, double deltaMJD) {
            this.startMJD = startMJD;
            this.deltaMJD = deltaMJD;
        }

        public Date getTime(PixelPos pixelPos) {
            return getUTC(pixelPos).getAsDate();
        }

        private ProductData.UTC getUTC(PixelPos pixelPos) {
            return new ProductData.UTC(startMJD + Math.floor(pixelPos.y) * deltaMJD);
        }
    }
}
