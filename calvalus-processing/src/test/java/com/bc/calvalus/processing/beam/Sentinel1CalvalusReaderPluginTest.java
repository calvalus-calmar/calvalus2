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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.DecodeQualification;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class Sentinel1CalvalusReaderPluginTest {

    @Test
    public void testDecodeQualification() throws Exception {
        Sentinel1CalvalusReaderPlugin plugin = new Sentinel1CalvalusReaderPlugin();
        assertSame(DecodeQualification.UNABLE, testFilename(plugin, "foo"));
        assertSame(DecodeQualification.INTENDED, testFilename(plugin, "S1B_IW_OCN__2SDV_20181102T065536_20181102T065601_013422_018D51_CC65.zip"));
    }

    public DecodeQualification testFilename(Sentinel1CalvalusReaderPlugin plugin, String path) {
        return plugin.getDecodeQualification(new PathConfiguration(new Path(path), new Configuration()));
    }
}