/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.table.FieldDef.Type;

import org.junit.Test;

/**
 * Test CRDT Counter path in JsonDef
 */
public class JsonDefImplCounterPathTest extends TestBase {

    @Test
    public void testCounterPath() {
        /* path is null */
        JsonDefImpl def = new JsonDefImpl();
        List<String[]> pathSteps = def.allMRCounterSteps();
        assertTrue(pathSteps == null);

        /* use link map to keep insert order */
        Map<String, Type> mrcounterFields = new LinkedHashMap<>();
        /* path is empty */
        def = new JsonDefImpl(mrcounterFields, null);
        pathSteps = def.allMRCounterSteps();
        assertTrue(pathSteps.isEmpty());

        /* normal path */
        mrcounterFields.put("a", Type.INTEGER);
        mrcounterFields.put("a.b", Type.INTEGER);
        mrcounterFields.put("a.\"b\".c", Type.INTEGER);
        /* quote path */
        mrcounterFields.put("\"a.b\".c", Type.INTEGER);
        mrcounterFields.put("a.\"b.c\"", Type.INTEGER);
        mrcounterFields.put("\"a.b.c\"", Type.INTEGER);
        mrcounterFields.put("\"_a\".b.c", Type.INTEGER);
        /* escape path */
        mrcounterFields.put("\"\\\"a\".b.c", Type.INTEGER);
        mrcounterFields.put("a.\"\\\"b\".c", Type.INTEGER);
        mrcounterFields.put("a.b.\"c\\\"\"", Type.INTEGER);
        mrcounterFields.put("\"\\\\a\".b.c", Type.INTEGER);
        mrcounterFields.put("a.\"\\\\b\".c", Type.INTEGER);
        mrcounterFields.put("a.b.\"c\\\\\"", Type.INTEGER);
        /* escape path + quote path */
        mrcounterFields.put("\"a.\\\"b.\\\"c\"", Type.INTEGER);
        mrcounterFields.put("\"\\\"a\".\"b.c\".\"d\\\"e\"", Type.INTEGER);

        def = new JsonDefImpl(mrcounterFields, null);
        pathSteps = def.allMRCounterSteps();

        assertEquals(15, pathSteps.size());

        /* normal path verify */
        String[] steps = pathSteps.get(0);
        assertEquals(1, steps.length);
        assertEquals("a", steps[0]);
        String mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a", mergedPath);

        steps = pathSteps.get(1);
        assertEquals(2, steps.length);
        assertEquals("a", steps[0]);
        assertEquals("b", steps[1]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a.b", mergedPath);

        steps = pathSteps.get(2);
        assertEquals(3, steps.length);
        assertEquals("a", steps[0]);
        assertEquals("b", steps[1]);
        assertEquals("c", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a.b.c", mergedPath);

        /* quote path verify */
        steps = pathSteps.get(3);
        assertEquals(2, steps.length);
        assertEquals("a.b", steps[0]);
        assertEquals("c", steps[1]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("\"a.b\".c", mergedPath);

        steps = pathSteps.get(4);
        assertEquals(2, steps.length);
        assertEquals("a", steps[0]);
        assertEquals("b.c", steps[1]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a.\"b.c\"", mergedPath);

        steps = pathSteps.get(5);
        assertEquals(1, steps.length);
        assertEquals("a.b.c", steps[0]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("\"a.b.c\"", mergedPath);

        steps = pathSteps.get(6);
        assertEquals(3, steps.length);
        assertEquals("_a", steps[0]);
        assertEquals("b", steps[1]);
        assertEquals("c", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("\"_a\".b.c", mergedPath);

        /* escape path verify */
        steps = pathSteps.get(7);
        assertEquals(3, steps.length);
        assertEquals("\"a", steps[0]);
        assertEquals("b", steps[1]);
        assertEquals("c", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("\"\\\"a\".b.c", mergedPath);

        steps = pathSteps.get(8);
        assertEquals(3, steps.length);
        assertEquals("a", steps[0]);
        assertEquals("\"b", steps[1]);
        assertEquals("c", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a.\"\\\"b\".c", mergedPath);

        steps = pathSteps.get(9);
        assertEquals(3, steps.length);
        assertEquals("a", steps[0]);
        assertEquals("b", steps[1]);
        assertEquals("c\"", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a.b.\"c\\\"\"", mergedPath);

        steps = pathSteps.get(10);
        assertEquals(3, steps.length);
        assertEquals("\\a", steps[0]);
        assertEquals("b", steps[1]);
        assertEquals("c", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("\"\\\\a\".b.c", mergedPath);

        steps = pathSteps.get(11);
        assertEquals(3, steps.length);
        assertEquals("a", steps[0]);
        assertEquals("\\b", steps[1]);
        assertEquals("c", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a.\"\\\\b\".c", mergedPath);

        steps = pathSteps.get(12);
        assertEquals(3, steps.length);
        assertEquals("a", steps[0]);
        assertEquals("b", steps[1]);
        assertEquals("c\\", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("a.b.\"c\\\\\"", mergedPath);

        /* escape path + quote path verify */
        steps = pathSteps.get(13);
        assertEquals(1, steps.length);
        assertEquals("a.\"b.\"c", steps[0]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("\"a.\\\"b.\\\"c\"", mergedPath);

        steps = pathSteps.get(14);
        assertEquals(3, steps.length);
        assertEquals("\"a", steps[0]);
        assertEquals("b.c", steps[1]);
        assertEquals("d\"e", steps[2]);
        mergedPath = JsonDefImpl.quoteStepIfNeedAndMerge(steps);
        assertEquals("\"\\\"a\".\"b.c\".\"d\\\"e\"", mergedPath);
    }
}
