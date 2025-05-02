/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.Collections.singletonMap;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.util.TestUtils.checkAll;

import java.util.stream.Stream;

import oracle.kv.TestBase;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.FieldDef.Type;

import org.junit.Test;

/** Test serial version compatibility */
public class JsonDefImplSerialTest extends TestBase {

    @Test
    public void testSerialVersion() {
        checkAll(
            Stream.of(
                serialVersionChecker(
                    new JsonDefImpl(),
                    SerialVersion.MINIMUM, 0xa408321593adc72aL),
                serialVersionChecker(
                    new JsonDefImpl("abc"),
                    SerialVersion.MINIMUM, 0xfc6acd017fe422f2L),
                serialVersionChecker(
                    new JsonDefImpl(singletonMap("name", Type.STRING), null),
                    SerialVersion.MINIMUM, 0x8f6afe1a9ea13291L),
                serialVersionChecker(
                    new JsonDefImpl(singletonMap("name", Type.STRING), "def"),
                    SerialVersion.MINIMUM, 0x22513646e5eddcd5L))
            .map(svc -> svc.reader(FieldDefImpl::readFastExternal))
            .map(svc ->
                 svc.equalsChecker(JsonDefImplSerialTest::checkEquals)));
    }

    private static boolean checkEquals(JsonDefImpl original,
                                       Object deserialized,
                                       short serialVersion) {
        /*
         * The JsonDefImpl.equals method checks the value of the
         * mrcounterFields field, which is only included in the serialized form
         * starting with the MINIMUM version. The equals method makes
         * no other checks, so an instanceof check is sufficient.
         */
        if (serialVersion >= SerialVersion.MINIMUM) {
            return original.equals(deserialized);
        }
        return deserialized instanceof JsonDefImpl;
    }
}
