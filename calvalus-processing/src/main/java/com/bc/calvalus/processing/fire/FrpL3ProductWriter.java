package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.DateUtils;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNode;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingDefault;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class FrpL3ProductWriter extends AbstractProductWriter {

    private static final String DIM_STRING = "time lat lon";

    private Map<String, VariableTemplate> variableTemplates;
    private NetcdfFileWriter fileWriter;

    FrpL3ProductWriter(ProductWriterPlugIn writerPlugIn) {
        super(writerPlugIn);

        fileWriter = null;

        createVariableTemplates();
    }

    static String getOutputPath(Object output) {
        final String filePath;
        if (output instanceof File) {
            filePath = ((File) output).getAbsolutePath();
        } else if (output instanceof String) {
            filePath = (String) output;
        } else {
            throw new IllegalArgumentException("invalid input type");
        }
        return filePath;
    }

    static void addDimensions(NetcdfFileWriter fileWriter, Product product) {
        fileWriter.addDimension("time", 1);
        fileWriter.addDimension("lon", product.getSceneRasterWidth());
        fileWriter.addDimension("lat", product.getSceneRasterHeight());
        fileWriter.addDimension("bounds", 2);
    }

    static void addGlobalMetadata(NetcdfFileWriter fileWriter, Product product) {
        final SimpleDateFormat COMPACT_ISO_FORMAT = DateUtils.createDateFormat("yyyyMMdd'T'HHmmss'Z'");
        final String dateStringNow = COMPACT_ISO_FORMAT.format(new Date());

        fileWriter.addGlobalAttribute("title", "ECMWF C3S Gridded OLCI Fire Radiative Power product");
        fileWriter.addGlobalAttribute("institution", "King's College London, Brockmann Consult GmbH");
        fileWriter.addGlobalAttribute("source", "ESA Sentinel-3 A+B SLSTR FRP");
        fileWriter.addGlobalAttribute("history", "Created on " + dateStringNow);
        fileWriter.addGlobalAttribute("references", "See https://climate.copernicus.eu/");
        fileWriter.addGlobalAttribute("tracking_id", UUID.randomUUID().toString());
        fileWriter.addGlobalAttribute("Conventions", "CF-1.7");
        fileWriter.addGlobalAttribute("summary", "TODO!");
        fileWriter.addGlobalAttribute("keywords", "Fire Radiative Power, Climate Change, ESA, C3S, GCOS");
        fileWriter.addGlobalAttribute("id", product.getName());
        fileWriter.addGlobalAttribute("naming_authority", "org.esa-cci");
        fileWriter.addGlobalAttribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science keywords");
        fileWriter.addGlobalAttribute("cdm_data_type", "Grid");
        fileWriter.addGlobalAttribute("comment", "These data were produced as part of the Copernicus Climate Change Service programme.");
        fileWriter.addGlobalAttribute("date_created", dateStringNow);
        fileWriter.addGlobalAttribute("creator_name", "Brockmann Consult GmbH");
        fileWriter.addGlobalAttribute("creator_url", "https://www.brockmann-consult.de");
        fileWriter.addGlobalAttribute("creator_email", "martin.boettcher@brockmann-consult.de");
        fileWriter.addGlobalAttribute("contact", "http://copernicus-support.ecmwf.int");
        fileWriter.addGlobalAttribute("project", "EC C3S Fire Radiative Power");
        fileWriter.addGlobalAttribute("geospatial_lat_min", "-90");
        fileWriter.addGlobalAttribute("geospatial_lat_max", "90");
        fileWriter.addGlobalAttribute("geospatial_lon_min", "-180");
        fileWriter.addGlobalAttribute("geospatial_lon_max", "180");
        fileWriter.addGlobalAttribute("geospatial_vertical_min", "0");
        fileWriter.addGlobalAttribute("geospatial_vertical_max", "0");
        fileWriter.addGlobalAttribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention");
        fileWriter.addGlobalAttribute("platform", "Sentinel-3");
        fileWriter.addGlobalAttribute("sensor", "SLSTR");
        fileWriter.addGlobalAttribute("geospatial_lon_units", "degrees_east");
        fileWriter.addGlobalAttribute("geospatial_lat_units", "degrees_north");
    }

    static void addAxesAndBoundsVariables(NetcdfFileWriter fileWriter) {
        Variable variable = fileWriter.addVariable("lon", DataType.FLOAT, "lon");
        variable.addAttribute(new Attribute(CF.UNITS, "degrees_east"));
        variable.addAttribute(new Attribute(CF.STANDARD_NAME, "longitude"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "longitude"));
        variable.addAttribute(new Attribute("bounds", "lon_bounds"));

        variable = fileWriter.addVariable("lat", DataType.FLOAT, "lat");
        variable.addAttribute(new Attribute(CF.UNITS, "degrees_north"));
        variable.addAttribute(new Attribute(CF.STANDARD_NAME, "latitude"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "latitude"));
        variable.addAttribute(new Attribute("bounds", "lat_bounds"));

        // @todo 2 tb/** is this standard? Double seems to be a too large datatype 2020-09-28
        variable = fileWriter.addVariable("time", DataType.DOUBLE, "time");
        variable.addAttribute(new Attribute(CF.UNITS, "days since 1970-01-01 00:00:00"));
        variable.addAttribute(new Attribute(CF.STANDARD_NAME, "time"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "time"));
        variable.addAttribute(new Attribute("bounds", "time_bounds"));
        variable.addAttribute(new Attribute("calendar", "standard"));

        fileWriter.addVariable("lon_bounds", DataType.FLOAT, "lon bounds");
        fileWriter.addVariable("lat_bounds", DataType.FLOAT, "lat bounds");
        fileWriter.addVariable("time_bounds", DataType.FLOAT, "time bounds");
    }

    private void createVariableTemplates() {
        variableTemplates = new HashMap<>();
        variableTemplates.put("s3a_day_pixel", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime pixels"));
        variableTemplates.put("s3a_day_cloud", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime cloudy pixels"));
        variableTemplates.put("s3a_day_water", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime water pixels"));
        variableTemplates.put("s3a_day_fire", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime active fire pixels"));
        variableTemplates.put("s3a_day_frp", new VariableTemplate(DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3A during daytime"));
        variableTemplates.put("s3a_night_pixel", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime pixels"));
        variableTemplates.put("s3a_night_cloud", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime cloudy pixels"));
        variableTemplates.put("s3a_night_water", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime water pixels"));
        variableTemplates.put("s3a_night_fire", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime active fire pixels"));
        variableTemplates.put("s3a_night_frp", new VariableTemplate(DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3A during nighttime"));
        variableTemplates.put("s3b_day_pixel", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime pixels"));
        variableTemplates.put("s3b_day_cloud", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime cloudy pixels"));
        variableTemplates.put("s3b_day_water", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime water pixels"));
        variableTemplates.put("s3b_day_fire", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime active fire pixels"));
        variableTemplates.put("s3b_day_frp", new VariableTemplate(DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3B during daytime"));
        variableTemplates.put("s3b_night_pixel", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime pixels"));
        variableTemplates.put("s3b_night_cloud", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime cloudy pixels"));
        variableTemplates.put("s3b_night_water", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime water pixels"));
        variableTemplates.put("s3b_night_fire", new VariableTemplate(DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime active fire pixels"));
        variableTemplates.put("s3b_night_frp", new VariableTemplate(DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3B during nighttime"));
    }

    @Override
    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) throws IOException {

    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        final String filePath = getOutputPath(getOutput());

        final Nc4Chunking chunking = Nc4ChunkingDefault.factory(Nc4Chunking.Strategy.standard, 5, true);
        fileWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, filePath, chunking);

        final Product sourceProduct = getSourceProduct();
        addDimensions(fileWriter, sourceProduct);
        addGlobalMetadata(fileWriter, sourceProduct);
        addAxesAndBoundsVariables(fileWriter);

        addProductVariables(sourceProduct);
        addWeightedFRPVariables();

        fileWriter.create();

        // write time, lon, lat bounds
        // write time lon, lat axis variables
    }

    private void addWeightedFRPVariables() {
        Variable variable = fileWriter.addVariable("s3a_day_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3A during daytime, weighted by cloud coverage"));

        variable = fileWriter.addVariable("s3a_night_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3A during nighttime, weighted by cloud coverage"));

        variable = fileWriter.addVariable("s3b_day_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3B during daytime, weighted by cloud coverage"));

        variable = fileWriter.addVariable("s3b_night_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3B during nighttime, weighted by cloud coverage"));
    }

    private void addProductVariables(Product sourceProduct) {
        final Band[] bands = sourceProduct.getBands();
        for (final Band band: bands) {
            final String bandName = band.getName();
            final VariableTemplate template = getTemplate(bandName);
            final Variable variable = fileWriter.addVariable(bandName, template.dataType, DIM_STRING);
            variable.addAttribute(new Attribute(CF.FILL_VALUE, template.fillValue,  template.dataType.isUnsigned()));
            variable.addAttribute(new Attribute(CF.UNITS, template.units));
            variable.addAttribute(new Attribute(CF.LONG_NAME, template.longName));
        }
    }

    @Override
    public void flush() throws IOException {
        if (fileWriter != null) {
            fileWriter.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
            fileWriter = null;
        }
    }

    @Override
    public boolean shouldWrite(ProductNode node) {
        return false;
    }

    @Override
    public boolean isIncrementalMode() {
        return false;
    }

    @Override
    public void setIncrementalMode(boolean enabled) {

    }

    @Override
    public void deleteOutput() throws IOException {

    }

    @Override
    public void removeBand(Band band) {

    }

    @Override
    public void setFormatName(String formatName) {

    }

    VariableTemplate getTemplate(String variableName) {
        final VariableTemplate variableTemplate = variableTemplates.get(variableName);
        if(variableTemplate == null) {
            throw new IllegalArgumentException("Unsupported variable:" + variableName);
        }
        return variableTemplate;
    }

    static class VariableTemplate {
        final DataType dataType;
        final Number fillValue;
        final String units;
        final String longName;

        VariableTemplate(DataType dataType, Number fillValue, String units, String longName) {
            this.dataType = dataType;
            this.fillValue = fillValue;
            this.units = units;
            this.longName = longName;
        }
    }
}
