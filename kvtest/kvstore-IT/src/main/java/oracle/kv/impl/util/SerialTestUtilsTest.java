/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.SerialTestUtils.checkSerialVersion;
import static oracle.kv.impl.util.SerialTestUtils.equalClasses;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.SerialTestUtils.versionHashes;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.TreeMap;

import oracle.kv.Durability;
import oracle.kv.TestBase;
import oracle.kv.impl.util.SerialTestUtils.SerialVersionChecker;

import org.junit.Test;

public class SerialTestUtilsTest extends TestBase {

    @Test
    public void testSerialVersionChecker() {
        {
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(new Object(), (short) 1, 2)
                .reader((in, sv) -> null)
                .writer((obj, out, sv) -> { });
            checkException(checker::check,
                           IllegalArgumentException.class,
                           "Version cannot be less than MINIMUM");
        }
        {
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(new Object(),
                                     (short) (SerialVersion.CURRENT + 1), 2)
                .reader((in, sv) -> null)
                .writer((obj, out, sv) -> { });
            checkException(checker::check,
                           IllegalArgumentException.class,
                           "Version cannot be greater than CURRENT");
        }
        {
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(new Object(),
                                     SerialVersion.MINIMUM, 1,
                                     (short) (SerialVersion.MINIMUM + 1), 1)
                .reader((in, sv) -> null)
                .writer((obj, out, sv) -> { });
            checkException(checker::check,
                           IllegalArgumentException.class,
                           "Hash for version \\d+ duplicates hash for" +
                           " previous version");
        }
        {
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(new Object(),
                                     (short) (SerialVersion.MINIMUM + 1),
                                     1)
                .reader((in, sv) -> null)
                .writer((obj, out, sv) -> { });
            checkException(checker::check,
                           AssertionError.class,
                           "Expected IllegalStateException when using" +
                           " serial version");
        }
        {
            final Object object = new Object();
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(
                    object,
                    SerialVersion.MINIMUM, 0xbf8b4530d8d246ddL,
                    (short) (SerialVersion.MINIMUM + 1), 0xdeadcafeL,
                    SerialVersion.CURRENT, 0x9842926af7ca0a8cL)
                .reader((in, sv) -> {
                        in.readByte();
                        return object;
                    })
                .writer((obj, out, sv) -> {
                        if (sv == SerialVersion.MINIMUM) {
                            out.writeByte(1);
                        } else if (sv == SerialVersion.CURRENT) {
                            out.writeByte(3);
                        } else {
                            out.writeByte(2);
                        }
                    });
            checkException(checker::check,
                           AssertionError.class,
                           "expected: 0xdeadcafe");
        }
        {
            final Object object = new Object();
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(object, 0xbf8b4530d8d246ddL)
                .reader((in, sv) -> object)
                .writer((obj, out, sv) -> out.writeByte(1));
            checkException(checker::check,
                           AssertionError.class,
                           "Expected EOF after reading");
        }
        {
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(new Object(), 0xbf8b4530d8d246ddL)
                .reader((in, sv) -> {
                        in.readByte();
                        return "hi";
                    })
                .writer((obj, out, sv) -> out.writeByte(1));
            checkException(checker::check,
                           AssertionError.class,
                           "expected:<class java.lang.Object>" +
                           " but was:<class java.lang.String>");
        }
        {
            final SerialVersionChecker<Object> checker =
                serialVersionChecker(new Object(), 0xbf8b4530d8d246ddL)
                .reader((in, sv) -> {
                        in.readByte();
                        return new Object();
                    })
                .writer((obj, out, sv) -> out.writeByte(1));
            checkException(checker::check,
                           AssertionError.class,
                           "not equal");
        }

        /*
         * Make sure that the equals checker can handle cases where the types
         * of its arguments are different
         */
        serialVersionChecker(1L, 0xbf8b4530d8d246ddL)
            .reader((in, sv) -> {
                    in.readByte();
                    return new String("one");
                })
            .writer((obj, out, sv) -> out.writeByte(1))
            .equalsChecker((x, y) -> y instanceof String)
            .check();
    }

    @Test
    public void testEqualClasses() {
        assertTrue(equalClasses(null, null));
        assertFalse(equalClasses("a", null));
        assertFalse(equalClasses(null, "b"));
        assertTrue(equalClasses("a", "b"));
        assertFalse(equalClasses("a", 2));
    }

    @Test
    public void testVersionHashes() {
        checkException(() -> versionHashes(SerialVersion.MINIMUM, 2, 3),
                       IllegalArgumentException.class,
                       "even length");
        checkException(() -> versionHashes(SerialVersion.MINIMUM, 1,
                                           SerialVersion.MINIMUM, 2),
                       IllegalArgumentException.class,
                       "Multiple hashes found for serial version");
    }

    @Test
    public void testCheckSerialVersion() {
        checkException(() -> checkSerialVersion(new Object(),
                                                new TreeMap<Short, Long>(),
                                                (x, y, sv) -> true,
                                                (in, sv) -> null,
                                                (obj, out, sv) -> { }),
                       IllegalArgumentException.class,
                       "versionHashes cannot be empty");
    }

    @Test
    public void testEqualsChecker() {
        {
            final SerialVersionChecker<Durability> checker =
                new SerialVersionChecker<>(Durability.COMMIT_SYNC,
                                           0xc7a623fd2bbc05bL);
            checker.check();

            checker.equalsChecker(Object::equals);
            checker.check();

            checker.equalsChecker((x, y, sv) -> x.equals(y));
            checker.check();

            checker.equalsChecker((x, y) -> false);
            checkException(() -> checker.check(),
                           Error.class,
                           "not equal");

            checker.equalsChecker((orig, deser, sv) -> false);
            checkException(() -> checker.check(),
                           Error.class,
                           "not equal");
        }
        {
            final SerialVersionChecker<Durability> checker =
                new SerialVersionChecker<>(Durability.COMMIT_NO_SYNC,
                                           0xc7a623fd2bbc05bL)
                .writer((obj, out, sv) ->
                        Durability.COMMIT_SYNC.writeFastExternal(out, sv));
            checkException(checker::check,
                           Error.class,
                           "not equal");

            checker.equalsChecker(Object::equals);
            checkException(checker::check,
                           Error.class,
                           "not equal");

            checker.equalsChecker((x, y, sv) -> x.equals(y));
            checkException(checker::check,
                           Error.class,
                           "not equal");

            checker.equalsChecker((x, y) ->
                                  Durability.COMMIT_SYNC.equals(y));
            checker.check();

            checker.equalsChecker((orig, deser, sv) ->
                                  Durability.COMMIT_SYNC.equals(deser));
            checker.check();
        }
    }

    @Test
    public void testReader() {
        {
            final SerialVersionChecker<Durability> checker =
                new SerialVersionChecker<>(Durability.COMMIT_SYNC,
                                           0xc7a623fd2bbc05bL);
            checker.check();

            checker.reader((in, sv) -> {
                    @SuppressWarnings("unused")
                    final Durability d = new Durability(in, sv);
                    return Durability.COMMIT_NO_SYNC;
                });
            checkException(checker::check, Error.class, "not equal");
        }

        {
            final SerialVersionChecker<String> checker =
                new SerialVersionChecker<>("abc",
                                           0x44ba8821161c91a7L)
                .writer(WriteFastExternal::writeString);
            checkException(checker::check,
                           Error.class,
                           "Constructor with DataInput and short arguments" +
                           " not found");

            checker.reader(SerializationUtil::readString);
            checker.check();
        }
    }

    @Test
    public void testWriter() {
        {
            final SerialVersionChecker<Durability> checker =
                new SerialVersionChecker<>(Durability.COMMIT_SYNC,
                                           0xc7a623fd2bbc05bL);
            checker.check();

            checker.writer(
                (obj, out, sv) ->
                Durability.COMMIT_NO_SYNC.writeFastExternal(out, sv));
            checkException(checker::check, Error.class, "not equal");
        }

        {
            final SerialVersionChecker<String> checker =
                new SerialVersionChecker<>("abc",
                                           0x44ba8821161c91a7L)
                .reader(SerializationUtil::readString);
            checkException(checker::check,
                           Error.class,
                           "Need to specify writer");

            checker.writer(WriteFastExternal::writeString);
            checker.check();
        }
    }

    @Test
    public void testDeserializedFormOp() {
        {
            final SerialVersionChecker<Durability> checker =
                new SerialVersionChecker<>(Durability.COMMIT_SYNC,
                                           0xc7a623fd2bbc05bL);
            checker.check();

            checker.toDeserializedFormOp(
                (obj, sv) -> Durability.COMMIT_NO_SYNC);
            checkException(checker::check, Error.class, "not equal");
        }

        {
            final SerialVersionChecker<Durability> checker =
                new SerialVersionChecker<>(Durability.COMMIT_NO_SYNC,
                                           0xc7a623fd2bbc05bL)
                .writer((obj, out, sv) ->
                        Durability.COMMIT_SYNC.writeFastExternal(out, sv));
            checkException(checker::check, Error.class, "not equal");

            checker.toDeserializedFormOp((obj, sv) -> Durability.COMMIT_SYNC);
            checker.check();
        }
    }
}
