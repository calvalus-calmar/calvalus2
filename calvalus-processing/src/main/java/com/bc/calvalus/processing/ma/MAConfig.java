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

package com.bc.calvalus.processing.ma;


import com.bc.calvalus.processing.xml.XmlBinding;
import com.bc.calvalus.processing.xml.XmlConvertible;
import org.esa.beam.framework.gpf.annotations.Parameter;

/**
 * The configuration for the match-up analysis.
 *
 * @author Norman
 */
public class MAConfig implements XmlConvertible {
    /**
     * If {@code copyInput = true}, all fields of an input (reference) record will be
     * copied into a corresponding output record.
     */
    @Parameter(defaultValue = "true")
    private boolean copyInput;

    /**
     * Size of the macro pixel given as number {@code n} of 'normal' pixels. An area comprising
     * {@code n x n} pixels will be considered in the match-up process. Should be an odd integer,
     * so that {@code n/2 - 1} pixels are considered around a given center pixel.
     * The default value size is {@code 5} pixels so that an area of 5 x 5 pixels will be considered.
     * @see #aggregateMacroPixel
     */
    @Parameter(defaultValue = "5")
    private int macroPixelSize;

    /**
     * If {@code aggregateMacroPixel = true}, all 'good' macro pixel values will be aggregated (averaged).
     * If {@code aggregateMacroPixel = false}, all pixels comprising the macro pixel will be extracted.
     */
    @Parameter(defaultValue = "true")
    private boolean aggregateMacroPixel;

    /**
     * Maximum time difference in hours between reference and EO pixel.
     * If {@code maxTimeDifference = null}, the criterion will not be used and match-ups are found for all times.
     * The default value is {@code 3.0} hours.
     */
    @Parameter (defaultValue = "3.0")
    private Double maxTimeDifference;

    /**
     * The band maths expression that identifies the "good" pixels in the macro pixel.
     * If not given, the criterion will not be used, thus all pixels will be considered being "good".
     */
    @Parameter
    private String goodPixelExpression;

    /**
     * Coefficient for <i>filtered mean criterion</i>.
     * If {@code filteredMeanCoeff = null}, the criterion will not be used.
     * The default value is {@code 1.5}.
     */
    @Parameter(defaultValue = "1.5")
    private Double filteredMeanCoeff;

    /**
     * The band maths expression that identifies the "good" records in the macro pixel.
     * If not given, the criterion will not be used, thus all pixels will be considered being "good".
     */
    @Parameter
    private String goodRecordExpression;

    // Replaced by goodRecordExpression (nf, 2011-08-24)
    /**
     * Threshold for the <i>NGP/NTP criterion</i>.
     * If {@code minNgpToNtpRatio = 0.0}, the criterion will not be used.
     */
    //@Parameter
    //double minNgpToNtpRatio;

    // Replaced by goodRecordExpression (nf, 2011-08-24)
    /**
     * Band name for <i>filtered mean criterion</i>.
     * If not given, the criterion will not be used.
     */
    //@Parameter
    //String filteredMeanBandName;

    /**
     * The time format used in the output.
     * Default is {@code "dd-MMM-yyyy HH:mm:ss"} (as used by Envisat).
     */
    @Parameter(defaultValue = "dd-MMM-yyyy HH:mm:ss")
    private String outputTimeFormat;

    /**
     * The name of a class that implements the {@link RecordSourceSpi} interface.
     * Instances of this class are used to create {@link RecordSource} objects which are in turn used to
     * provide {@link Record}s.
     */
    @Parameter(defaultValue = "com.bc.calvalus.processing.ma.PlacemarkRecordSource$Spi")
    private String recordSourceSpiClassName;

    /**
     * The URL of a {@link RecordSource}.
     * General parameter for many types of record sources.
     */
    @Parameter
    private String recordSourceUrl;


    public MAConfig() {
    }

    public static MAConfig fromXml(String xml) {
        return new XmlBinding().convertXmlToObject(xml, new MAConfig());
    }

    @Override
    public String toXml()  {
        return new XmlBinding().convertObjectToXml(this);
    }

    public RecordSource createRecordSource() throws Exception {
        RecordSourceSpi service = RecordSourceSpi.get(recordSourceSpiClassName);
        return service != null ? service.createRecordSource(recordSourceUrl) : null;
    }

    public String getRecordSourceSpiClassName() {
        return recordSourceSpiClassName;
    }

    public void setRecordSourceSpiClassName(String recordSourceSpiClassName) {
        this.recordSourceSpiClassName = recordSourceSpiClassName;
    }

    public String getRecordSourceUrl() {
        return recordSourceUrl;
    }

    public void setRecordSourceUrl(String recordSourceUrl) {
        this.recordSourceUrl = recordSourceUrl;
    }

    public String getOutputTimeFormat() {
        return outputTimeFormat;
    }

    public void setOutputTimeFormat(String outputTimeFormat) {
        this.outputTimeFormat = outputTimeFormat;
    }

    public boolean getCopyInput() {
        return copyInput;
    }

    public void setCopyInput(boolean copyInput) {
        this.copyInput = copyInput;
    }

    public int getMacroPixelSize() {
        return macroPixelSize;
    }

    public void setMacroPixelSize(int macroPixelSize) {
        this.macroPixelSize = macroPixelSize;
    }

    public boolean isAggregateMacroPixel() {
        return aggregateMacroPixel;
    }

    public void setAggregateMacroPixel(boolean aggregateMacroPixel) {
        this.aggregateMacroPixel = aggregateMacroPixel;
    }

    public Double getMaxTimeDifference() {
        return maxTimeDifference;
    }

    public void setMaxTimeDifference(Double maxTimeDifference) {
        this.maxTimeDifference = maxTimeDifference;
    }

    public Double getFilteredMeanCoeff() {
        return filteredMeanCoeff;
    }

    public void setFilteredMeanCoeff(Double filteredMeanCoeff) {
        this.filteredMeanCoeff = filteredMeanCoeff;
    }

    public String getGoodPixelExpression() {
        return goodPixelExpression;
    }

    public void setGoodPixelExpression(String goodPixelExpression) {
        this.goodPixelExpression = goodPixelExpression;
    }

    public String getGoodRecordExpression() {
        return goodRecordExpression;
    }

    public void setGoodRecordExpression(String goodRecordExpression) {
        this.goodRecordExpression = goodRecordExpression;
    }
}
