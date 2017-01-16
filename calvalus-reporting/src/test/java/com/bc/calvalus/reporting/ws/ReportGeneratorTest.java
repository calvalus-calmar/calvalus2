package com.bc.calvalus.reporting.ws;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.io.JSONExtractor;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;

import java.util.List;

/**
 * @author hans
 */
public class ReportGeneratorTest {

    private ReportGenerator reportGenerator;

    private JSONExtractor jsonExtractor;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
        jsonExtractor = new JSONExtractor();
    }

    @Test
    public void canGenerateTextSingleJob() throws Exception {
        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("job_1481485063251_20052");
        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateTextSingleJob(usageStatistic), equalTo("Usage statistic for job 'job_1481485063251_20052'\n" +
                                                                                  "\n" +
                                                                                  "Project : fire\n" +
                                                                                  "Start time : 12.01.2017 17:03:40\n" +
                                                                                  "Finish time : 12.01.2017 17:33:06\n" +
                                                                                  "Total time : 00:29:26\nStatus :  SUCCEEDED\n" +
                                                                                  "Total file writing (MB) : 89\n" +
                                                                                  "Total file reading (MB) : 262\n" +
                                                                                  "Total CPU time spent : 00:29:58\n" +
                                                                                  "Total Memory used (MB s) :  7,500,170\n" +
                                                                                  "Total vCores used (vCore s) :  1,762\n"));
    }

    @Ignore // to avoid creating pdf in every maven install
    @Test
    public void canGeneratePdfSingleJob() throws Exception {
        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("job_1481485063251_7037");
        reportGenerator = new ReportGenerator();
        String pdfPath = reportGenerator.generatePdfSingleJob(usageStatistic);

        assertThat(pdfPath, containsString("job_1481485063251_7037.pdf"));
    }

    @Test
    public void canGenerateTextMonthly() throws Exception {
        List<UsageStatistic> usageStatistics = jsonExtractor.getAllStatistics();

        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateTextMonthly(usageStatistics), equalTo("Usage statistic for user $USER in $MONTH $YEAR\n" +
                                                                                 "\n" +
                                                                                 "Jobs processed : 3546" +
                                                                                 "\nTotal file writing (MB) : 27,181,659\n" +
                                                                                 "Total file reading (MB) : 73,412,230\n" +
                                                                                 "Total CPU time spent : 26023:57:08\n" +
                                                                                 "Total Memory used (MB s) :  334,310,830,833\n" +
                                                                                 "Total vCores used (vCore s) :  92,738,732\n" +
                                                                                 "\n" +
                                                                                 "\n" +
                                                                                 "Price breakdown\n" +
                                                                                 "\n" +
                                                                                 "CPU usage price = (Total vCores used) x € 0.0013 = € 34.05\n" +
                                                                                 "Memory usage price = (Total Memory used) x € 0.00022 = € 20.22\n" +
                                                                                 "Disk space usage price = (Total file writing GB + Total file reading GB) x € 0.011 = € 1100.24\n" +
                                                                                 "\n" +
                                                                                 "Total = € 1154.51\n"));
    }

    @Ignore // to avoid creating pdf in every maven install
    @Test
    public void canGeneratePdfMonthly() throws Exception {
        List<UsageStatistic> usageStatistics = jsonExtractor.getAllStatistics();

        reportGenerator = new ReportGenerator();
        String pdfPath = reportGenerator.generatePdfMonthly(usageStatistics);

        assertThat(pdfPath, containsString("monthly.pdf"));
    }
}