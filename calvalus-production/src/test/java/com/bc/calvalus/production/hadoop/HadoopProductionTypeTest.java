/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.production.hadoop;

import org.esa.beam.framework.datamodel.ProductData;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class HadoopProductionTypeTest {

    @Test
    public void testGetPathPatterns() throws Exception {
        List<String> pathGlobs = HadoopProductionType.getInputPathGlobs("foo", null, null, null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("foo", pathGlobs.get(0));

        pathGlobs = HadoopProductionType.getInputPathGlobs("/foo/", null, null, null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/", pathGlobs.get(0));

        pathGlobs = HadoopProductionType.getInputPathGlobs("/foo/${region}/*.N1", "northsea", null, null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/northsea/*.N1", pathGlobs.get(0));

        pathGlobs = HadoopProductionType.getInputPathGlobs("/foo/${region}/bar/${region}/*.N1", "northsea", null, null);
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/northsea/bar/northsea/*.N1", pathGlobs.get(0));

        pathGlobs = HadoopProductionType.getInputPathGlobs("/foo/${yyyy}/${MM}/${dd}/*.N1", null, date("2005-01-01"), date("2005-01-10"));
        assertNotNull(pathGlobs);
        assertEquals(10, pathGlobs.size());
        assertEquals("/foo/2005/01/01/*.N1", pathGlobs.get(0));
        assertEquals("/foo/2005/01/02/*.N1", pathGlobs.get(1));
        assertEquals("/foo/2005/01/03/*.N1", pathGlobs.get(2));
        assertEquals("/foo/2005/01/04/*.N1", pathGlobs.get(3));
        assertEquals("/foo/2005/01/05/*.N1", pathGlobs.get(4));
        assertEquals("/foo/2005/01/06/*.N1", pathGlobs.get(5));
        assertEquals("/foo/2005/01/07/*.N1", pathGlobs.get(6));
        assertEquals("/foo/2005/01/08/*.N1", pathGlobs.get(7));
        assertEquals("/foo/2005/01/09/*.N1", pathGlobs.get(8));
        assertEquals("/foo/2005/01/10/*.N1", pathGlobs.get(9));

        pathGlobs = HadoopProductionType.getInputPathGlobs("/foo/${yyyy}/${MM}/${dd}/*.N1", null, date("2005-12-30"), date("2006-01-02"));
        assertNotNull(pathGlobs);
        assertEquals(4, pathGlobs.size());
        assertEquals("/foo/2005/12/30/*.N1", pathGlobs.get(0));
        assertEquals("/foo/2005/12/31/*.N1", pathGlobs.get(1));
        assertEquals("/foo/2006/01/01/*.N1", pathGlobs.get(2));
        assertEquals("/foo/2006/01/02/*.N1", pathGlobs.get(3));

        pathGlobs = HadoopProductionType.getInputPathGlobs("/foo/${yyyy}/${MM}/*.N1", null, date("2005-01-01"), date("2005-01-03"));
        assertNotNull(pathGlobs);
        assertEquals(1, pathGlobs.size());
        assertEquals("/foo/2005/01/*.N1", pathGlobs.get(0));

        pathGlobs = HadoopProductionType.getInputPathGlobs("/foo/MER_RR__1P*${yyyy}${MM}${dd}*.N1", null, date("2005-01-01"), date("2005-01-03"));
        assertNotNull(pathGlobs);
        assertEquals(3, pathGlobs.size());
        assertEquals("/foo/MER_RR__1P*20050101*.N1", pathGlobs.get(0));
        assertEquals("/foo/MER_RR__1P*20050102*.N1", pathGlobs.get(1));
        assertEquals("/foo/MER_RR__1P*20050103*.N1", pathGlobs.get(2));
    }

    private Date date(String dateAsString) throws ParseException {
        return ProductData.UTC.createDateFormat("yyyy-MM-dd").parse(dateAsString);
    }

}
