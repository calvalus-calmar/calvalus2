package com.bc.calvalus.production;

import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ProductionServiceImplTest {
    private TestProcessingService processingServiceMock;
    private ProductionServiceImpl productionServiceUnderTest;
    private TestProductionStore productionStoreMock;
    private TestStagingService stagingServiceMock;
    private TestProductionType productionTypeMock;


    @Before
    public void setUp() throws Exception {
        processingServiceMock = new TestProcessingService();
        stagingServiceMock = new TestStagingService();
        productionTypeMock = new TestProductionType(processingServiceMock,
                                                    stagingServiceMock);
        productionStoreMock = new TestProductionStore();
        productionServiceUnderTest = new ProductionServiceImpl(processingServiceMock,
                                                               stagingServiceMock,
                                                               productionStoreMock,
                                                               productionTypeMock);
    }

    @Test
    public void testGetProductSets() throws ProductionException {
        ProductSet[] productSets = productionServiceUnderTest.getProductSets(null);
        assertNotNull(productSets);
        assertEquals(3 + 3 * 3, productSets.length);
        assertEquals("entry3", productSets[0].getPath());
        assertEquals("entry3/entry1", productSets[1].getPath());
        assertEquals("entry3/entry2", productSets[2].getPath());
        assertEquals("entry3/entry3", productSets[3].getPath());
        assertEquals("entry2", productSets[4].getPath());
        assertEquals("entry2/entry1", productSets[5].getPath());
        assertEquals("entry2/entry2", productSets[6].getPath());
        assertEquals("entry2/entry3", productSets[7].getPath());
        assertEquals("entry1", productSets[8].getPath());
        assertEquals("entry1/entry1", productSets[9].getPath());
        assertEquals("entry1/entry2", productSets[10].getPath());
        assertEquals("entry1/entry3", productSets[11].getPath());
    }

    @Test
    public void testOrderProduction() throws ProductionException {

        ProductionRequest request = new ProductionRequest("test", "ewa");
        ProductionResponse productionResponse = productionServiceUnderTest.orderProduction(request);
        assertNotNull(productionResponse);
        assertNotNull(productionResponse.getProduction());
        assertEquals("id_1", productionResponse.getProduction().getId());
        assertEquals("name_1", productionResponse.getProduction().getName());
        assertNotNull(productionResponse.getProduction().getJobIds());
        assertEquals(2, productionResponse.getProduction().getJobIds().length);
        assertEquals("job_1_1", productionResponse.getProduction().getJobIds()[0]);
        assertEquals("job_1_2", productionResponse.getProduction().getJobIds()[1]);
        assertNotNull(productionResponse.getProduction().getProductionRequest());
        assertEquals(request, productionResponse.getProduction().getProductionRequest());
        assertEquals("stagingPath_1", productionResponse.getProduction().getStagingPath());
    }

    @Test
    public void testOrderUnknownProductionType() throws ProductionException {
        try {
            productionServiceUnderTest.orderProduction(new ProductionRequest("erase-hdfs", "devil"));
            fail("ProductionException expected, since 'erase-hdfs' is not a valid production type");
        } catch (ProductionException e) {
            // expected
        }
    }

    @Test
    public void testGetProductions() throws ProductionException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);
        assertEquals("id_1", productions[0].getId());
        assertEquals("id_2", productions[1].getId());
        assertEquals("id_3", productions[2].getId());

        // Make sure data store is used
        assertSame(productions[0], productionStoreMock.getProduction("id_1"));
        assertSame(productions[1], productionStoreMock.getProduction("id_2"));
        assertSame(productions[2], productionStoreMock.getProduction("id_3"));
        assertNull(productionStoreMock.getProduction("id_x"));
    }


    @Test
    public void testGetProductionStatusPropagation() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);

        assertEquals(ProcessStatus.UNKNOWN, productions[0].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[1].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[2].getProcessingStatus());

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.RUNNING, 0.2f));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.RUNNING, 0.4f));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.SCHEDULED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.RUNNING, 0.8f));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses();

        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.3f), productions[0].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), productions[1].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.4f), productions[2].getProcessingStatus());
    }


    @Test
    public void testDeleteProductions() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        productionServiceUnderTest.deleteProductions("id_2", "id_4");

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(5, productions.length);// cannot delete, because its status is not 'done'

        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.RUNNING));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses();

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);// now can delete id_2, because its status is not 'done'

        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.COMPLETED));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses();

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);// now can delete id_4, because its status is not 'done'
    }

    @Test
    public void testDeleteUnknownProduction() {
        try {
            productionServiceUnderTest.deleteProductions("id_45");
            fail("ProductionException expected, because we don't have production 'id_45'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testCancelProductions() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.SCHEDULED));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.RUNNING));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses();

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.RUNNING, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[2].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[3].getProcessingStatus().getState());

        productionServiceUnderTest.cancelProductions("id_1", "id_2", "id_4");

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses();

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.CANCELLED, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[2].getProcessingStatus().getState());
        assertEquals(ProcessState.CANCELLED, productions[3].getProcessingStatus().getState());
    }

    @Test
    public void testCancelUnknownProduction() {
        try {
            productionServiceUnderTest.cancelProductions("id_25");
            fail("ProductionException expected, because we don't have production 'id_25'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testStageProduction() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "false"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "false"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "true"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "true"));

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.COMPLETED));

        assertTrue(stagingServiceMock.getStagings().isEmpty());

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses();

        assertEquals(2, stagingServiceMock.getStagings().size());

        productionServiceUnderTest.stageProductions("id_1", "id_2");

        assertEquals(4, stagingServiceMock.getStagings().size());
    }

    @Test
    public void testStageUnknownProduction() {
        try {
            productionServiceUnderTest.stageProductions("id_98");
            fail("ProductionException expected, because we don't have production 'id_98'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testClose() throws IOException {
        assertEquals(false, stagingServiceMock.closed);
        assertEquals(false, processingServiceMock.closed);
        assertEquals(false, productionStoreMock.closed);
        productionServiceUnderTest.close();
        assertEquals(true, stagingServiceMock.closed);
        assertEquals(true, processingServiceMock.closed);
        assertEquals(true, productionStoreMock.closed);
    }
}