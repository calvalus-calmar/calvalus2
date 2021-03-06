/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

package com.bc.calvalus.processing.productinventory;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProductInventoryMapperTest {
    @Test
    public void testIsWholeLineSuspect() throws Exception {
        int[] invalidFlags = new int[]{1, 1, 1, 0, 0, 0, 1, 1, 1};
        int[] suspectFlags = new int[]{0, 0, 0, 1, 1, 1, 0, 0, 0};
        assertTrue(ProductInventoryMapper.isWholeLineSuspect(invalidFlags, suspectFlags));

        assertFalse(ProductInventoryMapper.isWholeLineSuspect(invalidFlags, new int[]{0, 0, 0, 0, 1, 1, 0, 0, 0}));
        assertFalse(ProductInventoryMapper.isWholeLineSuspect(invalidFlags, new int[]{0, 0, 0, 1, 1, 0, 0, 0, 0}));
        assertFalse(ProductInventoryMapper.isWholeLineSuspect(invalidFlags, new int[]{0, 0, 0, 0, 1, 0, 0, 0, 0}));
        assertFalse(ProductInventoryMapper.isWholeLineSuspect(invalidFlags, new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0}));
        assertFalse(ProductInventoryMapper.isWholeLineSuspect(invalidFlags, new int[]{1, 0, 0, 1, 1, 1, 0, 0, 0}));

    }
}
