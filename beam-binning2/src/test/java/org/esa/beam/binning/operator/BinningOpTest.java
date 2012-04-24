package org.esa.beam.binning.operator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.TiePointGeoCoding;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.main.GPT;
import org.esa.beam.util.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static java.lang.Math.*;
import static org.junit.Assert.*;

/**
 * Test that creates a local and a global L3 product from 5 source files.
 * The {@link BinningOp} is tested directly using an operator instance,
 * and indirectly using the GPF facade and the GPT command-line tool.
 *
 * @author Norman Fomferra
 */
public class BinningOpTest {

    static final File TESTDATA_DIR = new File("target/binning-test-io");

    static {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    @Before
    public void setUp() throws Exception {
        TESTDATA_DIR.mkdirs();
        if (!TESTDATA_DIR.isDirectory()) {
            fail("Can't create test I/O directory: " + TESTDATA_DIR);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (!FileUtils.deleteTree(TESTDATA_DIR)) {
            System.out.println("Warning: failed to completely delete test I/O directory:" + TESTDATA_DIR);
        }
    }

    /**
     * The following configuration generates a 1-degree resolution global product (360 x 180 pixels) from 5 observations.
     * Values are only generated for pixels at x=180..181 and y=87..89.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testGlobalBinning() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final BinningOp binningOp = new BinningOp();

        binningOp.setSourceProducts(createSourceProduct(obs1),
                                    createSourceProduct(obs2),
                                    createSourceProduct(obs3),
                                    createSourceProduct(obs4),
                                    createSourceProduct(obs5));

        binningOp.setStartDate("2002-01-01");
        binningOp.setEndDate("2002-01-10");
        binningOp.setBinningConfig(binningConfig);
        binningOp.setFormatterConfig(formatterConfig);

        final Product targetProduct = binningOp.getTargetProduct();
        assertNotNull(targetProduct);
        try {
            assertGlobalBinningProductIsOk(targetProduct, null, obs1, obs2, obs3, obs4, obs5);
        } catch (Exception e) {
            targetProduct.dispose();
        }
    }

    /*
    @Test
    public void testGlobalBinning_WithMemoryMappedFile() throws Exception {
        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final BinningOp binningOp = new BinningOp(new MemoryMappedFileSpatialBinStore());

        binningOp.setSourceProducts(createSourceProduct(obs1),
                                    createSourceProduct(obs2),
                                    createSourceProduct(obs3),
                                    createSourceProduct(obs4),
                                    createSourceProduct(obs5));

        binningOp.setStartDate("2002-01-01");
        binningOp.setEndDate("2002-01-10");
        binningOp.setBinningConfig(binningConfig);
        binningOp.setFormatterConfig(formatterConfig);

        final Product targetProduct = binningOp.getTargetProduct();
        assertNotNull(targetProduct);
        try {
            assertGlobalBinningProductIsOk(targetProduct, null, obs1, obs2, obs3, obs4, obs5);
        } catch (Exception e) {
            targetProduct.dispose();
        }
    }
    */

    /**
     * The following configuration generates a 1-degree resolution local product (4 x 4 pixels) from 5 observations.
     * The local region is lon=-1..+3 and lat=-1..+3 degrees.
     * Values are only generated for pixels at x=1..2 and y=1..2.
     *
     * @throws Exception if something goes badly wrong
     * @see #testGlobalBinning()
     */
    @Test
    public void testLocalBinning() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final BinningOp binningOp = new BinningOp();

        binningOp.setSourceProducts(createSourceProduct(obs1),
                                    createSourceProduct(obs2),
                                    createSourceProduct(obs3),
                                    createSourceProduct(obs4),
                                    createSourceProduct(obs5));

        GeometryFactory gf = new GeometryFactory();
        binningOp.setRegion(gf.createPolygon(gf.createLinearRing(new Coordinate[]{
                new Coordinate(-1.0, -1.0),
                new Coordinate(3.0, -1.0),
                new Coordinate(3.0, 3.0),
                new Coordinate(-1.0, 3.0),
                new Coordinate(-1.0, -1.0),
        }), null));
        binningOp.setStartDate("2002-01-01");
        binningOp.setEndDate("2002-01-10");
        binningOp.setBinningConfig(binningConfig);
        binningOp.setFormatterConfig(formatterConfig);

        final Product targetProduct = binningOp.getTargetProduct();
        assertNotNull(targetProduct);
        try {
            assertLocalBinningProductIsOk(targetProduct, null, obs1, obs2, obs3, obs4, obs5);
        } catch (IOException e) {
            targetProduct.dispose();
        }
    }


    /**
     * Same as {@link #testGlobalBinning}, but this time via the GPF facade.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testGlobalBinningViaGPF() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final HashMap<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("startDate", "2002-01-01");
        parameters.put("endDate", "2002-01-10");
        parameters.put("binningConfig", binningConfig);
        parameters.put("formatterConfig", formatterConfig);

        final Product targetProduct = GPF.createProduct("Binning", parameters,
                                                        createSourceProduct(obs1),
                                                        createSourceProduct(obs2),
                                                        createSourceProduct(obs3),
                                                        createSourceProduct(obs4),
                                                        createSourceProduct(obs5));

        assertNotNull(targetProduct);
        try {
            assertGlobalBinningProductIsOk(targetProduct, null, obs1, obs2, obs3, obs4, obs5);
        } catch (Exception e) {
            targetProduct.dispose();
        }
    }

    /**
     * Same as {@link #testLocalBinning}, but this time via the GPF facade.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testLocalBinningViaGPF() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final HashMap<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("region", "POLYGON((-1 -1, 3 -1, 3 3, -1 3, -1 -1))");
        parameters.put("startDate", "2002-01-01");
        parameters.put("endDate", "2002-01-10");
        parameters.put("binningConfig", binningConfig);
        parameters.put("formatterConfig", formatterConfig);

        final Product targetProduct = GPF.createProduct("Binning",
                                                        parameters,
                                                        createSourceProduct(obs1),
                                                        createSourceProduct(obs2),
                                                        createSourceProduct(obs3),
                                                        createSourceProduct(obs4),
                                                        createSourceProduct(obs5));
        assertNotNull(targetProduct);
        try {
            assertLocalBinningProductIsOk(targetProduct, null, obs1, obs2, obs3, obs4, obs5);
        } catch (IOException e) {
            targetProduct.dispose();
        }
    }

    /**
     * Same as {@link #testGlobalBinning}, but this time via the 'gpt' command-line tool.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testGlobalBinningViaGPT() throws Exception {

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final File parameterFile = new File(getClass().getResource("BinningParamsGlobal.xml").toURI());
        final File targetFile = getTestFile("output.dim");
        final File sourceFile1 = getTestFile("obs1.dim");
        final File sourceFile2 = getTestFile("obs2.dim");
        final File sourceFile3 = getTestFile("obs3.dim");
        final File sourceFile4 = getTestFile("obs4.dim");
        final File sourceFile5 = getTestFile("obs5.dim");

        try {
            ProductIO.writeProduct(createSourceProduct(obs1), sourceFile1, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs2), sourceFile2, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs3), sourceFile3, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs4), sourceFile4, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs5), sourceFile5, "BEAM-DIMAP", false);

            GPT.run(new String[]{
                    "Binning",
                    "-p", parameterFile.getPath(),
                    "-t", targetFile.getPath(),
                    sourceFile1.getPath(),
                    sourceFile2.getPath(),
                    sourceFile3.getPath(),
                    sourceFile4.getPath(),
                    sourceFile5.getPath(),
            });

            assertTrue(targetFile.exists());

            final Product targetProduct = ProductIO.readProduct(targetFile);
            assertNotNull(targetProduct);
            try {
                assertGlobalBinningProductIsOk(targetProduct, targetFile, obs1, obs2, obs3, obs4, obs5);
            } catch (IOException e) {
                targetProduct.dispose();
            }
        } finally {
            FileUtils.deleteTree(TESTDATA_DIR);
        }
    }

    /**
     * Same as {@link #testLocalBinning}, but this time via the 'gpt' command-line tool.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testLocalBinningViaGPT() throws Exception {

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final File parameterFile = new File(getClass().getResource("BinningParamsLocal.xml").toURI());
        final String fileName = "output.dim";
        final File targetFile = getTestFile(fileName);
        final File sourceFile1 = getTestFile("obs1.dim");
        final File sourceFile2 = getTestFile("obs2.dim");
        final File sourceFile3 = getTestFile("obs3.dim");
        final File sourceFile4 = getTestFile("obs4.dim");
        final File sourceFile5 = getTestFile("obs5.dim");

        ProductIO.writeProduct(createSourceProduct(obs1), sourceFile1, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs2), sourceFile2, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs3), sourceFile3, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs4), sourceFile4, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs5), sourceFile5, "BEAM-DIMAP", false);

        GPT.run(new String[]{
                "Binning",
                "-p", parameterFile.getPath(),
                "-t", targetFile.getPath(),
                sourceFile1.getPath(),
                sourceFile2.getPath(),
                sourceFile3.getPath(),
                sourceFile4.getPath(),
                sourceFile5.getPath(),
        });

        assertTrue(targetFile.exists());

        final Product targetProduct = ProductIO.readProduct(targetFile);
        assertNotNull(targetProduct);
        try {
            assertLocalBinningProductIsOk(targetProduct, targetFile, obs1, obs2, obs3, obs4, obs5);
        } catch (IOException e) {
            targetProduct.dispose();
        }
    }

    /**
     * Same as {@link #testGlobalBinning}, but this time via the 'gpt' command-line tool.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testGlobalBinningViaGPT_FilePattern() throws Exception {

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final File parameterFile = new File(getClass().getResource("BinningParamsGlobal_FilePattern.xml").toURI());
        final File targetFile = getTestFile("output.dim");
        final File sourceFile1 = getTestFile("obs1.dim");
        final File sourceFile2 = getTestFile("obs2.dim");
        final File sourceFile3 = getTestFile("obs3.dim");
        final File sourceFile4 = getTestFile("obs4.dim");
        final File sourceFile5 = getTestFile("obs5.dim");

        try {
            ProductIO.writeProduct(createSourceProduct(obs1), sourceFile1, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs2), sourceFile2, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs3), sourceFile3, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs4), sourceFile4, "BEAM-DIMAP", false);
            ProductIO.writeProduct(createSourceProduct(obs5), sourceFile5, "BEAM-DIMAP", false);

            GPT.run(new String[]{
                    "Binning",
                    "-p", parameterFile.getPath(),
                    "-t", targetFile.getPath(),
            });

            assertTrue(targetFile.exists());

            final Product targetProduct = ProductIO.readProduct(targetFile);
            assertNotNull(targetProduct);
            try {
                assertGlobalBinningProductIsOk(targetProduct, targetFile, obs1, obs2, obs3, obs4, obs5);
            } catch (IOException e) {
                targetProduct.dispose();
            }
        } finally {
            FileUtils.deleteTree(TESTDATA_DIR);
        }
    }


    /**
     * Same as {@link #testLocalBinning}, but this time via the 'gpt' command-line tool.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testLocalBinningViaGPT_FilePattern() throws Exception {

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final File parameterFile = new File(getClass().getResource("BinningParamsLocal_FilePattern.xml").toURI());
        final String fileName = "output.dim";
        final File targetFile = getTestFile(fileName);
        final File sourceFile1 = getTestFile("obs1.dim");
        final File sourceFile2 = getTestFile("obs2.dim");
        final File sourceFile3 = getTestFile("obs3.dim");
        final File sourceFile4 = getTestFile("obs4.dim");
        final File sourceFile5 = getTestFile("obs5.dim");

        ProductIO.writeProduct(createSourceProduct(obs1), sourceFile1, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs2), sourceFile2, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs3), sourceFile3, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs4), sourceFile4, "BEAM-DIMAP", false);
        ProductIO.writeProduct(createSourceProduct(obs5), sourceFile5, "BEAM-DIMAP", false);

        GPT.run(new String[]{
                "Binning",
                "-p", parameterFile.getPath(),
                "-t", targetFile.getPath(),
        });

        assertTrue(targetFile.exists());

        final Product targetProduct = ProductIO.readProduct(targetFile);
        assertNotNull(targetProduct);
        try {
            assertLocalBinningProductIsOk(targetProduct, targetFile, obs1, obs2, obs3, obs4, obs5);
        } catch (IOException e) {
            targetProduct.dispose();
        }
    }


    private void assertGlobalBinningProductIsOk(Product targetProduct, File location, float obs1, float obs2, float obs3, float obs4, float obs5) throws IOException {
        assertTargetProductIsOk(targetProduct, location, obs1, obs2, obs3, obs4, obs5, 360, 180, 179, 87);
    }

    private void assertLocalBinningProductIsOk(Product targetProduct, File location, float obs1, float obs2, float obs3, float obs4, float obs5) throws IOException {
        assertTargetProductIsOk(targetProduct, location, obs1, obs2, obs3, obs4, obs5, 4, 4, 0, 0);
    }

    private void assertTargetProductIsOk(Product targetProduct, File location, float obs1, float obs2, float obs3, float obs4, float obs5, int sceneRasterWidth, int sceneRasterHeight, int x0, int y0) throws IOException {
        final int w = 4;
        final int h = 4;

        final int _o_ = -1;
        final float _x_ = Float.NaN;

        assertEquals(location, targetProduct.getFileLocation());
        assertEquals(sceneRasterWidth, targetProduct.getSceneRasterWidth());
        assertEquals(sceneRasterHeight, targetProduct.getSceneRasterHeight());
        assertNotNull(targetProduct.getStartTime());
        assertNotNull(targetProduct.getEndTime());
        assertEquals("01-JAN-2002 00:00:00.000000", targetProduct.getStartTime().format());
        assertEquals("10-JAN-2002 00:00:00.000000", targetProduct.getEndTime().format());
        assertNotNull(targetProduct.getBand("num_obs"));
        assertNotNull(targetProduct.getBand("num_passes"));
        assertNotNull(targetProduct.getBand("chl_mean"));
        assertNotNull(targetProduct.getBand("chl_sigma"));
        assertEquals(_o_, targetProduct.getBand("num_obs").getNoDataValue(), 1e-10);
        assertEquals(_o_, targetProduct.getBand("num_passes").getNoDataValue(), 1e-10);
        assertEquals(_x_, targetProduct.getBand("chl_mean").getNoDataValue(), 1e-10);
        assertEquals(_x_, targetProduct.getBand("chl_sigma").getNoDataValue(), 1e-10);

        // Test pixel values of band "num_obs"
        //
        final int nob = 5;
        final int[] expectedNobs = new int[]{
                _o_, _o_, _o_, _o_,
                _o_, nob, nob, _o_,
                _o_, nob, nob, _o_,
                _o_, _o_, _o_, _o_,
        };
        final int[] actualNobs = new int[w * h];
        targetProduct.getBand("num_obs").readPixels(x0, y0, w, h, actualNobs);
        assertArrayEquals(expectedNobs, actualNobs);

        // Test pixel values of band "num_passes"
        //
        final int npa = 5;
        final int[] expectedNpas = new int[]{
                _o_, _o_, _o_, _o_,
                _o_, npa, npa, _o_,
                _o_, npa, npa, _o_,
                _o_, _o_, _o_, _o_,
        };
        final int[] actualNpas = new int[w * h];
        targetProduct.getBand("num_passes").readPixels(x0, y0, w, h, actualNpas);
        assertArrayEquals(expectedNpas, actualNpas);

        // Test pixel values of band "chl_mean"
        //
        final float mea = (obs1 + obs2 + obs3 + obs4 + obs5) / nob;
        final float[] expectedMeas = new float[]{
                _x_, _x_, _x_, _x_,
                _x_, mea, mea, _x_,
                _x_, mea, mea, _x_,
                _x_, _x_, _x_, _x_,
        };
        final float[] actualMeas = new float[w * h];
        targetProduct.getBand("chl_mean").readPixels(x0, y0, w, h, actualMeas);
        assertArrayEquals(expectedMeas, actualMeas, 1e-4F);

        // Test pixel values of band "chl_sigma"
        //
        final float sig = (float) sqrt((obs1 * obs1 + obs2 * obs2 + obs3 * obs3 + obs4 * obs4 + obs5 * obs5) / nob - mea * mea);
        final float[] expectedSigs = new float[]{
                _x_, _x_, _x_, _x_,
                _x_, sig, sig, _x_,
                _x_, sig, sig, _x_,
                _x_, _x_, _x_, _x_,
        };
        final float[] actualSigs = new float[w * h];
        targetProduct.getBand("chl_sigma").readPixels(x0, y0, w, h, actualSigs);
        assertArrayEquals(expectedSigs, actualSigs, 1e-4F);
    }

    static BinningConfig createBinningConfig() {
        final AggregatorConfig aggregatorConfig = new AggregatorConfig();
        aggregatorConfig.setAggregatorName("AVG");
        aggregatorConfig.setVarName("chl");
        final BinningConfig binningConfig = new BinningConfig();
        binningConfig.setAggregatorConfigs(aggregatorConfig);
        binningConfig.setNumRows(180);
        binningConfig.setMaskExpr("true");
        return binningConfig;
    }

    static int sourceProductCounter = 1;
    static int targetProductCounter = 1;

    static FormatterConfig createFormatterConfig() throws IOException {
        final File targetFile = getTestFile("target-" + (targetProductCounter++) + ".dim");
        final FormatterConfig formatterConfig = new FormatterConfig();
        formatterConfig.setOutputFile(targetFile.getPath());
        formatterConfig.setOutputType("Product");
        formatterConfig.setOutputFormat("BEAM-DIMAP");
        return formatterConfig;
    }

    static Product createSourceProduct() {
        return createSourceProduct(1.0F);
    }

    static Product createSourceProduct(float value) {
        final Product p = new Product("P" + sourceProductCounter++, "T", 2, 2);
        final TiePointGrid latitude = new TiePointGrid("latitude", 2, 2, 0.5F, 0.5F, 1.0F, 1.0F, new float[]{
                1.0F, 1.0F,
                0.0F, 0.0F,
        });
        final TiePointGrid longitude = new TiePointGrid("longitude", 2, 2, 0.5F, 0.5F, 1.0F, 1.0F, new float[]{
                0.0F, 1.0F,
                0.0F, 1.0F,
        });
        p.addTiePointGrid(latitude);
        p.addTiePointGrid(longitude);
        p.setGeoCoding(new TiePointGeoCoding(latitude, longitude));
        p.addBand("chl", value + "");
        return p;
    }

    static File getTestFile(String fileName) {
        return new File(TESTDATA_DIR, fileName);
    }

}
