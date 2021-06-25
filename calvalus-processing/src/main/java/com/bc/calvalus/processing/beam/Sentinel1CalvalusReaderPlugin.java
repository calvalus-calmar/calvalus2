/*
 * Copyright (C) 2019 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.dataio.AbstractProductReader;
import org.esa.snap.core.dataio.DecodeQualification;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.io.SnapFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A reader for handing Sentinel-1 data on calvalus.
 * It unzips the products and open from the local file.
 */
public class Sentinel1CalvalusReaderPlugin implements ProductReaderPlugIn {

    private static final String FORMAT_NAME_S1 = "CALVALUS-SENTINEL-1";

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            String filename = pathConfig.getPath().getName();
            // Only supporting Sentinel-1 Level-2 OCN product first
            if (filename.matches("^S1.*OCN__2.*") ) {
                return DecodeQualification.INTENDED;
            }
        }
        return DecodeQualification.UNABLE;
    }

    @Override
    public Class[] getInputTypes() {
        return new Class[]{PathConfiguration.class};
    }

    @Override
    public ProductReader createReaderInstance() {
        return new Sentinel1CalvalusReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{FORMAT_NAME_S1};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".zip"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Sentinel-1 OCN Level 2 on Calvalus";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    static class Sentinel1CalvalusReader extends AbstractProductReader {

        Sentinel1CalvalusReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                Configuration configuration = pathConfig.getConfiguration();
                File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToCWD(pathConfig.getPath(), configuration);

                // find manifest file
                File productManifest = null;
                for (File file : unzippedFiles) {
                    if (file.getName().equalsIgnoreCase("manifest.safe")) {
                        productManifest = file;
                        break;
                    }
                }
                if (productManifest == null) {
                    throw new IllegalFileFormatException("input has no mainfest file.");
                }
                String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT,FORMAT_NAME_S1);
                CalvalusLogger.getLogger().info("inputFormat = " + inputFormat);

                String snapFormatName =  "SENTINEL-1";
                Product product = ProductIO.readProduct(productManifest, snapFormatName);
                CalvalusLogger.getLogger().info("Band names: " + Arrays.toString(product.getBandNames()));

                Map<String, Object> params = new HashMap<>();
                String referenceBand = "vv_001_owiWindSpeed";
                params.put("referenceBand", referenceBand);
                CalvalusLogger.getLogger().info("resampling input to " + referenceBand);
                product = GPF.createProduct("Resample", params, product);
                return product;
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}
