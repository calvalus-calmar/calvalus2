package com.bc.calvalus.experiments.matchup;

import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import javax.media.jai.operator.ConstantDescriptor;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class MatchupOpTest {

    @Test
    public void testInitialisation() throws ParseException, TransformException, FactoryException {
        MatchupOp op = new MatchupOp();
        Product sourceProduct = createTestProduct();
        op.setSourceProduct(sourceProduct);
        op.setParameter("startTime", date("2001"));
        op.setParameter("endTime", date("2010"));
        Product targetProduct = op.getTargetProduct();
        assertSame(sourceProduct, targetProduct);
        assertNull(op.getTargetProperty("matchupDatasets"));
    }

    private static ProductData.UTC date(String date) throws ParseException {
        return ProductData.UTC.parse(date, "yyyy");
    }

    @Test
    public void testDateRanges() throws ParseException {
        Product product = new Product("name", "type", 2, 2);
        product.setStartTime(date("2003"));
        product.setEndTime(date("2005"));

        assertTrue(MatchupOp.isProductInTimeRange(product, null, null));

        assertTrue(MatchupOp.isProductInTimeRange(product, date("2002"), null));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2004"), null));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), null));

        assertFalse(MatchupOp.isProductInTimeRange(product, null, date("2002")));
        assertTrue(MatchupOp.isProductInTimeRange(product, null, date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, null, date("2010")));

        assertFalse(MatchupOp.isProductInTimeRange(product, date("2000"), date("2002")));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), date("2010")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2000"), date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2004"), date("2007")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2000"), date("2010")));
    }

    @Test
    public void testCreateSubset() throws ParseException, TransformException, FactoryException {
        Product product = createTestProduct();

        GeoPos subsetCenterLocation = new GeoPos(0.5f, 0.5f);
        Product subsetProduct = MatchupOp.createSubset(product, subsetCenterLocation, 5);

        assertNotNull(subsetProduct);
        assertEquals(5, subsetProduct.getSceneRasterWidth());
        assertEquals(5, subsetProduct.getSceneRasterHeight());

        GeoPos topLeftCorner = subsetProduct.getGeoCoding().getGeoPos(new PixelPos(0f, 0f), null);
        assertEquals(3f, topLeftCorner.lat, 0.001);
        assertEquals(-2f, topLeftCorner.lon, 0.001);
    }

    @Test
    public void testSimplestCase() throws ParseException, TransformException, FactoryException {
        final ProductData.UTC referenceTime = ProductData.UTC.parse("01-JAN-2005 03:45:00"); //"dd-MMM-yyyy HH:mm:ss"
        final GeoPos location = new GeoPos(0, 0);
        MatchupOp op = new MatchupOp();
        op.setReferenceDatabase(new ReferenceDatabase() {
            @Override
            public List<ReferenceMeasurement> findReferenceMeasurement(String site, Product product, double deltaTime) {

                ReferenceMeasurement measurement = new ReferenceMeasurement("id0", "northsea", referenceTime.getAsDate(), location);
                List<ReferenceMeasurement> arrayList = new ArrayList<ReferenceMeasurement>(1);
                arrayList.add(measurement);
                return arrayList;
            }
        });
        Product sourceProduct = createTestProduct();
        sourceProduct.setStartTime(ProductData.UTC.parse("20050101", "yyyyMMdd"));
        sourceProduct.setEndTime(ProductData.UTC.parse("20050102", "yyyyMMdd"));
        op.setSourceProduct(sourceProduct);
        Product targetProduct = op.getTargetProduct();
        assertSame(sourceProduct, targetProduct);
        Object targetProperty = op.getTargetProperty("matchupDatasets");
        assertNotNull(targetProperty);
        assertTrue(targetProperty instanceof MatchupDataset[]);
        MatchupDataset[] matchupDatasets = (MatchupDataset[]) targetProperty;
        assertEquals(1, matchupDatasets.length);
        MatchupDataset dataset = matchupDatasets[0];
        assertEquals("test", dataset.getName());
        assertSame(location, dataset.getLocation());

    }

    private static Product createTestProduct() throws FactoryException, TransformException, ParseException {
        Rectangle bounds = new Rectangle(360, 180);
        Product product = new Product("test", "TEST", bounds.width, bounds.height);

        product.setStartTime(date("2003"));
        product.setEndTime(date("2005"));

        AffineTransform i2mTransform = new AffineTransform();
        final int northing = 90;
        final int easting = -180;
        i2mTransform.translate(easting, northing);
        final double scaleX = 360 / bounds.width;
        final double scaleY = 180 / bounds.height;
        i2mTransform.scale(scaleX, -scaleY);
        CrsGeoCoding geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, bounds, i2mTransform);
        product.setGeoCoding(geoCoding);

        Band band = product.addBand("band", ProductData.TYPE_FLOAT32);
        band.setSourceImage(ConstantDescriptor.create(360f, 180f, new Float[]{42f}, null));

        return product;
    }

}