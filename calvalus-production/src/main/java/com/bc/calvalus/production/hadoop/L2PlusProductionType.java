package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l2.L2FormattingWorkflowItem;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2PlusProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing,
                                     StagingService staging) {
            return new L2PlusProductionType(inventory, processing, staging);
        }
    }

    L2PlusProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                         StagingService stagingService) {
        super("L2Plus", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProdcutionName(
                createProductionName("Level 2 ", productionRequest));

        List<DateRange> dateRanges = productionRequest.getDateRanges();

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        String processorBundle = processorProductionRequest.getProcessorBundle();

        boolean FRESHMON = false;
        if (processorBundle.toLowerCase().startsWith("freshmon")) {
            FRESHMON = true;  // TODO generalize
        }

        String globalOutputDir = "";
        String formattingInputDir;
        Workflow.Sequential l2fWorkflowItem = new Workflow.Sequential();
        if (processorProductionRequest.getProcessorName().equals("Formatting")) {
            formattingInputDir = productionRequest.getString("inputPath");
        } else {
            HadoopWorkflowItem processingItem = createProcessingItem(productionId, productionName, dateRanges,
                                                                     productionRequest, processorProductionRequest);
            globalOutputDir = processingItem.getOutputDir();
            formattingInputDir = createPathPattern(globalOutputDir);
            l2fWorkflowItem.add(processingItem);
        }

        String outputFormat = productionRequest.getString("outputFormat", null);
        if (outputFormat != null && !outputFormat.equals("SEQ")) {
            String formattingOutputDir = getOutputPath(productionRequest, productionId, "-output");
            globalOutputDir = formattingOutputDir;

            Workflow.Parallel formattingItem = new Workflow.Parallel();
            String outputBandList = productionRequest.getString("outputBandList", "");
            if (outputFormat.equals("Multi-GeoTIFF")) {
                if (!outputBandList.isEmpty()) {
                    for (String bandName : StringUtils.csvToArray(outputBandList)) {
                        HadoopWorkflowItem item = createFormattingItem(productionName + " Format: " + bandName,
                                                                       dateRanges,
                                                                       formattingInputDir, formattingOutputDir,
                                                                       productionRequest,
                                                                       processorProductionRequest, bandName, "GeoTIFF");
                        if (FRESHMON) {
                            // TODO generalize
                            Configuration jobConfig = item.getJobConfig();
                            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_REGEX,
                                          "L2_of_MER_..._1.....(........_......).*");
                            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_REPLACEMENT,
                                          String.format("%s_%s_BC_$1", bandName, productionRequest.getRegionName()));
                        }
                        formattingItem.add(item);
                    }
                } else {
                    throw new ProductionException(
                            "If Multi-GeoTiff is specified as format also bands must be selected");
                }
            } else {
                formattingItem.add(createFormattingItem(productionName + " Format", dateRanges,
                                                        formattingInputDir, formattingOutputDir, productionRequest,
                                                        processorProductionRequest, outputBandList,
                                                        outputFormat));
            }
            l2fWorkflowItem.add(formattingItem);
        }
        if (l2fWorkflowItem.getItems().length == 0) {
            throw new ProductionException("Neither Processing nor Formatting selected.");
        }

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              globalOutputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              l2fWorkflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new CopyStaging(production,
                               getProcessingService().getJobClient().getConf(),
                               getStagingService().getStagingDir());
    }


    HadoopWorkflowItem createFormattingItem(String productionName,
                                            List<DateRange> dateRanges,
                                            String formattingInputDir,
                                            String formattingOutputDir,
                                            ProductionRequest productionRequest,
                                            ProcessorProductionRequest processorProductionRequest, String bandList,
                                            String outputFormat) throws
                                                                 ProductionException {

        Configuration formatJobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, formatJobConfig);
        setRequestParameters(productionRequest, formatJobConfig);


        String processorBundle = processorProductionRequest.getProcessorBundle();
        if (processorBundle != null) {
            formatJobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
        }

        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, formattingInputDir);
        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, formattingOutputDir);
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

        Geometry regionGeom = productionRequest.getRegionGeometry(null);
        formatJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                            regionGeom != null ? regionGeom.toString() : "");
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_CRS, productionRequest.getString("outputCRS", ""));
        if (productionRequest.getBoolean("replaceNans", false)) {
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_REPLACE_NAN_VALUE,
                                String.valueOf(productionRequest.getDouble("replaceValue", 0.0)));
        }
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_BANDLIST, bandList);
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_QUICKLOOKS,
                            productionRequest.getString("quicklooks", "false"));

        return new L2FormattingWorkflowItem(getProcessingService(), productionName, formatJobConfig);
    }

    private HadoopWorkflowItem createProcessingItem(String productionId, String productionName,
                                                    List<DateRange> dateRanges, ProductionRequest productionRequest,
                                                    ProcessorProductionRequest processorProductionRequest) throws
                                                                                                           ProductionException {

        productionRequest.ensureParameterSet("processorName");

        String outputDir = getOutputPath(productionRequest, productionId, "");

        Configuration l2JobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, l2JobConfig);
        setRequestParameters(productionRequest, l2JobConfig);
        processorProductionRequest.configureProcessor(l2JobConfig);

        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        Geometry regionGeom = productionRequest.getRegionGeometry(null);
        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeom != null ? regionGeom.toString() : "");

        Date startDate = dateRanges.get(0).getStartDate();
        Date stopDate = dateRanges.get(dateRanges.size() - 1).getStopDate();
        String pathPattern = createPathPattern(outputDir);
        String regionWKT = regionGeom != null ? regionGeom.toString() : null;
        ProcessorDescriptor processorDesc = processorProductionRequest.getProcessorDescriptor(getProcessingService());
        ProductSet productSet = new ProductSet(getResultingProductionType(processorDesc),
                                               productionName, pathPattern, startDate, stopDate,
                                               productionRequest.getRegionName(), regionWKT);

        HadoopWorkflowItem l2Item = new L2WorkflowItem(getProcessingService(), productionName, l2JobConfig);
        l2Item.addWorkflowStatusListener(new ProductSetSaver(l2Item, productSet, outputDir));
        return l2Item;
    }

    // TODO consider l2Gen output here too; this is only valid for sequential and MERIS files
    private String createPathPattern(String basePath) {
        return basePath + "/.*${yyyy}${MM}${dd}.*.seq$";
    }

    private String getResultingProductionType(ProcessorDescriptor processorDescriptor) {
        if (processorDescriptor != null) {
            return processorDescriptor.getOutputProductType();
        }
        return "L2_PRODUCT";
    }

}