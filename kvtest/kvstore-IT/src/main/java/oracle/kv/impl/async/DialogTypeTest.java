/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Test the {@link DialogType}, {@link DialogTypeFamily}, and {@link
 * StandardDialogTypeFamily} classes.
 */
public class DialogTypeTest extends TestBase {
    private static class MyDialogTypeFamily implements DialogTypeFamily {
        private final int familyId;
        MyDialogTypeFamily(int familyId) {
            this.familyId = familyId;
        }
        @Override
        public int getFamilyId() { return familyId; }
        @Override
        public String getFamilyName() { return "MyDialogTypeFamily"; }
    }

    private static DialogTypeFamily myTypeFamily = new MyDialogTypeFamily(77);
    static {
        DialogType.registerTypeFamily(myTypeFamily);
    }

    @Test
    @SuppressWarnings({"unused","null"})
    public void testConstructors() {
        try {
            new DialogType(-1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            new DialogType(32);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            new DialogType(null, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            new DialogType(20088);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        DialogType dialogType = new DialogType(myTypeFamily, 1);
        assertEquals(myTypeFamily, dialogType.getDialogTypeFamily());
        int dialogTypeId = dialogType.getDialogTypeId();
        assertEquals(dialogTypeId,
                     new DialogType(dialogTypeId).getDialogTypeId());

        final int max =
            (Integer.MAX_VALUE/DialogType.MAX_TYPE_FAMILIES) - 1;
        dialogTypeId = new DialogType(myTypeFamily, max).getDialogTypeId();
        assertEquals(dialogTypeId,
                     new DialogType(dialogTypeId).getDialogTypeId());
        try {
            new DialogType(myTypeFamily, -1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            new DialogType(myTypeFamily, max + 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        DialogTypeFamily family = new MyDialogTypeFamily(-1);
        try {
            new DialogType(family, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        family = new MyDialogTypeFamily(100);
        try {
            new DialogType(family, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        family = new MyDialogTypeFamily(10000);
        try {
            new DialogType(family, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        family = new MyDialogTypeFamily(88);
        try {
            new DialogType(family, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
    }
}
