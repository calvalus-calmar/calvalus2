package com.bc.calvalus.processing.fire;

import com.bc.calvalus.processing.l3.L3SpatialBin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;

import static com.bc.calvalus.commons.DateUtils.createCalendar;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class FrpReducerTest {

    @Test
    public void testGetDateTime() {
        final Date date = new Date(1603948559000L);

        final String[] dateTime = FrpReducer.getDateTime(date, createCalendar());
        assertEquals(2, dateTime.length);
        assertEquals("20201029", dateTime[0]);
        assertEquals("051559", dateTime[1]);
    }

    @Test
    public void testWriteL2CSV_noMeasurement() throws IOException, InterruptedException {
        final Reducer.Context context = mock(Reducer.Context.class);
        when(context.nextKey()).thenReturn(false);

        final StringWriter out = new StringWriter();
        FrpReducer.writeL2CSV(context, createCalendar(), out);

        assertEquals("Column\tRow\tDate\tTime\tLatitude\tLongitude\tsat_zenith\tFRP_MWIR\tFRP_MWIR_uncertainty\tFRP_SWIR\tFRP_SWIR_uncertainty\tConfidence\tF1_flag\tDay_flag\tArea\tPlatform\tLand/Ocean\tHotspot_class\n",
                out.toString());

        verify(context, times(1)).nextKey();
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testWriteL2CSV_oneMeasurement() throws IOException, InterruptedException {
        final Reducer.Context context = mock(Reducer.Context.class);
        when(context.nextKey())
                .thenReturn(true)
                .thenReturn(false);

        final LongWritable binIndex = new LongWritable(654149120561988L);
        when(context.getCurrentKey()).thenReturn(binIndex);

        final L3SpatialBin spatialBin = new L3SpatialBin(binIndex.get(), 14, 0);
        final float[] featureValues = spatialBin.getFeatureValues();
        featureValues[0] = 1.f; // platform
        featureValues[1] = 2.f; // lat
        featureValues[2] = 3.f; // lon
        featureValues[3] = 4.f; // row
        featureValues[4] = 5.f; // col
        featureValues[5] = 6.f; // frp_mwir
        featureValues[6] = 7.f; // frp_mwir_uncertainty
        featureValues[7] = 8.f; // frp_swir
        featureValues[8] = 9.f; // frp_swir_uncertainty
        featureValues[9] = 10.f; // area
        featureValues[10] = 11.f; // flags -> night | ocean
        featureValues[11] = 12.f; // channel -> F1_flag
        featureValues[12] = 13.f; // classification
        featureValues[13] = 14.f; // confidence
        spatialBin.setNumObs(1);

        final ArrayList<L3SpatialBin> binList = new ArrayList<>();
        binList.add(spatialBin);
        when(context.getValues()).thenReturn(binList);

        final StringWriter out = new StringWriter();
        FrpReducer.writeL2CSV(context, createCalendar(), out);

        assertEquals("Column\tRow\tDate\tTime\tLatitude\tLongitude\tsat_zenith\tFRP_MWIR\tFRP_MWIR_uncertainty\tFRP_SWIR\tFRP_SWIR_uncertainty\tConfidence\tF1_flag\tDay_flag\tArea\tPlatform\tLand/Ocean\tHotspot_class\n" +
                        "5\t4\t20200923\t040520\t2.00000\t3.00000\tNaN\t6.000000\t7.000000\t8.000000\t9.000000\t14.000000\t12\t0\t10.000000\tS3A\t0\t13\n",
                out.toString());
    }

    @Test
    public void testWriteL2CSV_oneMeasurement_differentValues() throws IOException, InterruptedException {
        final Reducer.Context context = mock(Reducer.Context.class);
        when(context.nextKey())
                .thenReturn(true)
                .thenReturn(false);

        final LongWritable binIndex = new LongWritable(654149120561990L);
        when(context.getCurrentKey()).thenReturn(binIndex);

        final L3SpatialBin spatialBin = new L3SpatialBin(binIndex.get(), 14, 0);
        final float[] featureValues = spatialBin.getFeatureValues();
        featureValues[0] = 2.f; // platform
        featureValues[1] = 3.f; // lat
        featureValues[2] = 4.f; // lon
        featureValues[3] = 5.f; // row
        featureValues[4] = 6.f; // col
        featureValues[5] = 7.f; // frp_mwir
        featureValues[6] = 8.f; // frp_mwir_uncertainty
        featureValues[7] = 9.f; // frp_swir
        featureValues[8] = 10.f; // frp_swir_uncertainty
        featureValues[9] = 11.f; // area
        featureValues[10] = 64.f; // flags -> day | land
        featureValues[11] = 13.f; // channel -> F1_flag
        featureValues[12] = 14.f; // classification
        featureValues[13] = 15.f; // confidence
        spatialBin.setNumObs(1);

        final ArrayList<L3SpatialBin> binList = new ArrayList<>();
        binList.add(spatialBin);
        when(context.getValues()).thenReturn(binList);

        final StringWriter out = new StringWriter();
        FrpReducer.writeL2CSV(context, createCalendar(), out);

        assertEquals("Column\tRow\tDate\tTime\tLatitude\tLongitude\tsat_zenith\tFRP_MWIR\tFRP_MWIR_uncertainty\tFRP_SWIR\tFRP_SWIR_uncertainty\tConfidence\tF1_flag\tDay_flag\tArea\tPlatform\tLand/Ocean\tHotspot_class\n" +
                        "6\t5\t20200923\t040520\t3.00000\t4.00000\tNaN\t7.000000\t8.000000\t9.000000\t10.000000\t15.000000\t13\t1\t11.000000\tS3B\t1\t14\n",
                out.toString());
    }
}