package com.bc.calvalus.processing.ma.expr;

import com.bc.calvalus.processing.ma.AggregatedNumber;
import com.bc.calvalus.processing.ma.DefaultHeader;
import com.bc.calvalus.processing.ma.DefaultRecord;
import com.bc.jexp.Term;
import com.bc.jexp.impl.ParserImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class RecordExprTest {
    @Test
    public void testRecordsWithArrays() throws Exception {
        DefaultHeader header = new DefaultHeader("chl");
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(header);
        HeaderNamespace namespace = new HeaderNamespace(header);

        DefaultRecord record1 = new DefaultRecord(null, null, new Object[]{
                new AggregatedNumber(25, 24, 16, 2.6f, 0.4f, 2.4f, 0.2f),
        }
        );
        DefaultRecord record2 = new DefaultRecord(null, null, new Object[]{
                new AggregatedNumber(25, 16, 12, 1.7f, 0.3f, 2.5f, 0.1f),
        }
        );

        ParserImpl parser = new ParserImpl(namespace);

        Term t1 = parser.parse("chl.filteredMean - 1.5 * chl.filteredStdDev");
        Term t2 = parser.parse("feq(chl.mean, 2.6)");
        Term t3 = parser.parse("chl.CV");
        Term t4 = parser.parse("chl.CV > 0.15");

        recordEvalEnv.setValues(record1);
        assertEquals(2.4 - 1.5 * 0.2, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(true, t2.evalB(recordEvalEnv));
        assertEquals(0.2 / 2.4, t3.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t4.evalB(recordEvalEnv));

        recordEvalEnv.setValues(record2);
        assertEquals(2.5 - 1.5 * 0.1, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t2.evalB(recordEvalEnv));
        assertEquals(0.1 / 2.5, t3.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t4.evalB(recordEvalEnv));
    }

    @Test
    public void testRecordsWithScalars() throws Exception {
        DefaultHeader header = new DefaultHeader("b", "s", "i", "f");
        RecordEvalEnv recordEvalEnv = new RecordEvalEnv(header);
        HeaderNamespace namespace = new HeaderNamespace(header);

        DefaultRecord record1 = new DefaultRecord(null, null, new Object[]{
                false, "x", 4, 0.6F
        }
        );
        DefaultRecord record2 = new DefaultRecord(null, null, new Object[]{
                true, "y", 3, 0.5F
        }
        );

        ParserImpl parser = new ParserImpl(namespace);

        Term t1 = parser.parse("i + 1.5 * f");
        Term t2 = parser.parse("feq(f, 0.6)");
        Term t3 = parser.parse("b");
        Term t4 = parser.parse("i > 3");

        recordEvalEnv.setValues(record1);
        assertEquals(4 + 1.5 * 0.6, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(true, t2.evalB(recordEvalEnv));
        assertEquals(false, t3.evalB(recordEvalEnv));
        assertEquals(true, t4.evalB(recordEvalEnv));

        recordEvalEnv.setValues(record2);
        assertEquals(3 + 1.5 * 0.5, t1.evalD(recordEvalEnv), 1e-6);
        assertEquals(false, t2.evalB(recordEvalEnv));
        assertEquals(true, t3.evalB(recordEvalEnv));
        assertEquals(false, t4.evalB(recordEvalEnv));
    }
}
