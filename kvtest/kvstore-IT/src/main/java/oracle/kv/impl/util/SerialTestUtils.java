/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiPredicate;

import oracle.kv.util.TestUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities for testing {@link FastExternalizable} serialization compatibility
 * across multiple serial versions. Every class that implements {@code
 * FastExternalizable} should have a test that uses this class to check that it
 * maintains serialization compatibility.
 *
 * <p>This class provides classes and methods that allow tests to use recorded
 * hash values to check that serialization formats stay the same for ranges of
 * serial versions. Tests create instances of the serializable class, provide
 * information about the expected hashes for particular serial versions, and
 * optionally specify how to serialize, deserialize, and compare instances.
 *
 * <p>For each class that implements {@link FastExternalizable}, tests should
 * check the serialization for instances that represent all of the unique
 * serialization patterns for the class. In particular, if a field is added to
 * a class, testing should include instances where the new field has default
 * (null or 0) and non-default values.
 *
 * <p>Tests should call {@link #serialVersionChecker} method overloadings to
 * define checkers, and can call {@link SerialVersionChecker#check} to perform
 * the check. If a single test method checks multiple instances, it can use
 * {@link TestUtils#checkAll} to perform the checks, so a single test run can
 * print all failure results, making it easier to update hashes. This approach
 * is particularly useful when a new serialization format has been added, and
 * many hashes need to be added.
 *
 * <p>If a class has a single serialized form for all versions, then just a
 * single hash can be specified. If the class has multiple serialized forms for
 * different ranges of serial versions, then a hash should be provided for the
 * first serial version supporting each new version. If serializing a
 * particular object is only supported starting with a particular serial
 * version, then that should be the first version specified, and the
 * serialization should fail with {@link IllegalStateException} if the object
 * is serialized with an earlier serial version. Remember to specify the 'L'
 * character suffix when specifying hash values as long constants (e.g.
 * 0xe6953e736a5251b8L). Only the supported serial versions, from {@link
 * SerialVersion#MINIMUM} through {@link SerialVersion#CURRENT}, are tested.
 */
@NonNullByDefault
public class SerialTestUtils {

    /** Whether to include the deserialized bytes in failure messages. */
    private static final boolean SHOW_BYTES =
        Boolean.getBoolean("oracle.kv.test.serial.showBytes");

    /**
     * Use a SHA message digest to compute an 8 byte hash of the serialized
     * format. Using the standard rule of thumb that random values generated
     * with 2^64 bits will produce 2^32 unique values, we can be confident that
     * the hashes will uniquely identity the serialized forms.
     */
    private static final MessageDigest digest;
    static {
        try {
            digest = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
    }

    /**
     * A checker for serial version compatibility.
     */
    public static class SerialVersionChecker<T> implements Runnable {
        private final T object;
        private final NavigableMap<Short, Long> hashes;
        private EqualsChecker<? super T> equalsChecker =
            EqualsChecker::defaultCheckEquals;
        private @Nullable ReadFastExternal<?> reader;
        private @Nullable WriteFastExternal<T> writer;
        private ToDeserializedFormOp<T> toDeserializedFormOp =
            SerializationUtil::toDeserializedForm;

        /**
         * Creates a checker that confirms that the {@link FastExternalizable}
         * serialized form of the specified object has the specified hash value
         * for all currently supported serial versions.
         *
         * <p>Make sure to specify a writer with the {@link
         * #writer(WriteFastExternal)} method if the object does not implement
         * {@code FastExternalizable}.
         */
        public SerialVersionChecker(T object, long hash) {
            this(object, versionHashes(hash));
        }

        /**
         * Creates a checker that confirms that the {@link FastExternalizable}
         * serialized form of the specified object matches the specified hashes
         * and serial versions. Each version to hash mapping specifies a range
         * of versions for which the hash should match, starting with the
         * specified version and up to the version prior to the next version
         * specified, or up to and including the current version for the last
         * entry. Attempting to serialize the object should fail when using
         * serial versions from {@link SerialVersion#MINIMUM} to one less than
         * the first version specified.
         *
         * <p>Make sure to specify a writer with the {@link
         * #writer(WriteFastExternal)} method if the object does not implement
         * {@code FastExternalizable}.
         */
        public SerialVersionChecker(T object,
                                    NavigableMap<Short, Long> hashes) {
            this.object = object;
            this.hashes = hashes;
        }

        /**
         * Specifies a two argument predicate that will be used to compare the
         * deserialized form of the original object and the deserialized
         * object. The check will fail if the checker returns false or if it
         * throws an exception.
         *
         * <p>If no equals checker is specified, objects are compared using
         * {@link Object#equals}.
         *
         * @see #equalClasses
         */
        public SerialVersionChecker<T>
            equalsChecker(BiPredicate<? super T, Object> newEqualsChecker)
        {
            equalsChecker = (x, y, sv) -> newEqualsChecker.test(x, y);
            return this;
        }

        /**
         * Specifies a predicate that will be used to compare the deserialized
         * form of the original object and the deserialized object, and that
         * will be provided the serial version of serialized form to use when
         * making its comparison. The check will fail if the checker returns
         * false or if it throws an exception.
         *
         * <p>If no equals checker is provided, objects are compared using
         * {@link Object#equals}.
         */
        public SerialVersionChecker<T>
            equalsChecker(EqualsChecker<? super T> newEqualsChecker)
        {
            equalsChecker = newEqualsChecker;
            return this;
        }

        /**
         * Specifies a reader for reading the serialized form of the object.
         *
         * <p>If no reader is provided, the object is read using a constructor
         * for the object class that has {@link DataInput} and {@code short}
         * (serial version) parameters.
         */
        public SerialVersionChecker<T> reader(ReadFastExternal<?> newReader) {
            reader = newReader;
            return this;
        }

        /**
         * Specifies a writer for writing the serialized form of the object.
         *
         * <p>If no writer is provided and the object implements {@link
         * FastExternalizable}, uses the {@link
         * FastExternalizable#writeFastExternal} method. If the object does not
         * implement {@code FastExternalizable}, then a writer must be
         * provided.
         */
        public SerialVersionChecker<T> writer(WriteFastExternal<T> newWriter) {
            writer = newWriter;
            return this;
        }

        /**
         * Specifies the operation to perform to convert the original object to
         * the form expected for an object deserialized for the specified
         * serial version.
         *
         * <p>If no deserialized form operation is provided, uses the {@link
         * SerializationUtil#toDeserializedForm} method.
         */
        public SerialVersionChecker<T>
            toDeserializedFormOp(ToDeserializedFormOp<T> op)
        {
            toDeserializedFormOp = op;
            return this;
        }

        /** Runs the check. */
        @Override
        public void run() {
            check();
        }

        /** Runs the check. */
        public void check() {
            final ReadFastExternal<?> nonNullReader = (reader != null) ?
                reader :
                getConstructorReader(object);
            final WriteFastExternal<T> nonNullWriter;
            if (writer != null) {
                nonNullWriter = writer;
            } else if (object instanceof FastExternalizable) {
                nonNullWriter = (obj, out, sv) ->
                    ((FastExternalizable) obj).writeFastExternal(out, sv);
            } else {
                throw new AssertionError("Need to specify writer for" +
                                         " objects that do not implement" +
                                         " FastExternalizable: " + object);
            }
            checkSerialVersion(object, hashes, equalsChecker,
                               nonNullReader, nonNullWriter,
                               toDeserializedFormOp);
        }
    }

    /**
     * Creates a serial version checker that confirms that the {@link
     * FastExternalizable} serialized form of the specified object has the
     * specified hash value for all currently supported serial versions.
     *
     * <p>Make sure to specify a writer with the {@link
     * SerialVersionChecker#writer(WriteFastExternal)} method if the object
     * does not implement {@code FastExternalizable}.
     */
    public static <T>
        SerialVersionChecker<T> serialVersionChecker(T object, long hash)
    {
        return new SerialVersionChecker<>(object, hash);
    }

    /**
     * Creates a checker that confirms that the {@link FastExternalizable}
     * serialized form of the specified object matches the specified hashes and
     * serial versions. Each version to hash mapping specifies a range of
     * versions for which the hash should match, starting with the specified
     * version and up to the version prior to the next version specified, or up
     * to and including the current version for the last entry. Attempting to
     * serialize the object should fail when using serial versions from {@link
     * SerialVersion#MINIMUM} to one less than the first version specified.
     *
     * <p>Make sure to specify a writer with the {@link
     * SerialVersionChecker#writer(WriteFastExternal)} method if the object
     * does not implement {@code FastExternalizable}.
     */
    public static <T> SerialVersionChecker<T>
        serialVersionChecker(T object,
                             short firstVersion,
                             long firstHash,
                             long... versionsAndHashes)
    {
        return new SerialVersionChecker<>(
            object, versionHashes(firstVersion, firstHash, versionsAndHashes));
    }

    /**
     * Creates a checker that confirms that the {@link FastExternalizable}
     * serialized form of the specified object matches the specified map of
     * hashes and serial versions. Each map entry specifies a range of versions
     * for which the hash should match, starting with the specified version and
     * up to the version prior to the next version specified, or up to and
     * including the current version for the last entry. Attempting to
     * serialize the object should fail when using serial versions from {@link
     * SerialVersion#MINIMUM} to one less than the first version specified.
     *
     * <p>Make sure to specify a writer with the {@link
     * SerialVersionChecker#writer(WriteFastExternal)} method if the object
     * does not implement {@code FastExternalizable}.
     */
    public static <T> SerialVersionChecker<T>
        serialVersionChecker(T object, NavigableMap<Short, Long> hashes)
    {
        return new SerialVersionChecker<>(object, hashes);
    }

    /**
     * A predicate that compares an original object with a version of the
     * object serialized and then deserialized using the specified serial
     * version.
     */
    public interface EqualsChecker<T> {

        /**
         * Returns true if the deserialized object should be considered equal
         * to the original object when deserialized at the specified serial
         * version. Returns false or throws an exception of the objects should
         * not be considered equal.
         */
        boolean checkEquals(T original,
                            Object deserialized,
                            short serialVersion);

        /**
         * A default checkEquals method that first checks that the original and
         * deserialized objects are of the same class, and then calls equals.
         */
        static boolean defaultCheckEquals(Object original,
                                          Object deserialized,
                                          @SuppressWarnings("unused")
                                          short serialVersion) {
            assertEquals(original.getClass(),
                         ((deserialized != null) ?
                          deserialized.getClass() :
                          null));
            return original.equals(deserialized);
        }
    }

    /**
     * Returns true if the two objects are instances of the same class, or
     * both null, else returns false.
     */
    public static boolean equalClasses(@Nullable Object x,
                                       @Nullable Object y) {
        return (x == y) ? true :
            ((x == null) || (y == null)) ? false :
            x.getClass().equals(y.getClass());
    }

    /**
     * Converts an object to the form it is expected to have when serialized
     * and deserialized with the specified serial version.
     */
    public interface ToDeserializedFormOp<T> {
        /**
         * Returns an object of the same type that represents the form that the
         * specified object should take when serialized and deserialized with
         * the specified serial version.
         */
        T toDeserializedForm(T object, short serialVersion);
    }

    /**
     * Returns a map of serial versions and hashes with a single entry that
     * maps all serial versions to the specified hash.
     */
    public static NavigableMap<Short, Long> versionHashes(long hash) {
        final NavigableMap<Short, Long> map = new TreeMap<>();
        map.put(SerialVersion.MINIMUM, hash);
        return map;
    }

    /**
     * Returns a map of the specified serial versions and hashes.
     */
    public static
        NavigableMap<Short, Long> versionHashes(short firstVersion,
                                                long firstHash,
                                                long... versionsAndHashes)
    {
        if (versionsAndHashes.length % 2 != 0) {
            throw new IllegalArgumentException(
                "versionsAndHashes must have even length, was: " +
                versionsAndHashes.length);
        }
        final long[] values = new long[versionsAndHashes.length + 2];
        values[0] = firstVersion;
        values[1] = firstHash;
        System.arraycopy(versionsAndHashes, 0,
                         values, 2, versionsAndHashes.length);
        final NavigableMap<Short, Long> map = new TreeMap<>();
        for (int i = 0; i < values.length; i += 2) {
            final long version = values[i];
            final long hash = values[i+1];
            final Long oldValue = map.put((short) version, hash);
            if (oldValue != null) {
                throw new IllegalArgumentException(
                    "Multiple hashes found for serial version " + version);
            }
        }
        return map;
    }

    /**
     * Checks that the FastExternalizable serialized form of the specified
     * object matches the specified serial versions and hashes, using the
     * specified equals checker, reader, and writer, and calling {@link
     * FastExternalizable#deserializedForm} to obtain the serialized form.
     */
    static <T> void checkSerialVersion(T object,
                                       NavigableMap<Short, Long> versionHashes,
                                       EqualsChecker<? super T> equalsChecker,
                                       ReadFastExternal<?> reader,
                                       WriteFastExternal<T> writer)
    {
        checkSerialVersion(object, versionHashes, equalsChecker, reader,
                           writer, SerializationUtil::toDeserializedForm);
    }

    /**
     * Checks that the FastExternalizable serialized form of the specified
     * object matches the specified serial versions and hashes, using the
     * specified equals checker, reader, writer, and deserialized form
     * operation.
     */
    static <T>
        void checkSerialVersion(T object,
                                NavigableMap<Short, Long> versionHashes,
                                EqualsChecker<? super T> equalsChecker,
                                ReadFastExternal<?> reader,
                                WriteFastExternal<T> writer,
                                ToDeserializedFormOp<T> toDeserializedFormOp)
    {
        if (versionHashes.isEmpty()) {
            throw new IllegalArgumentException(
                "versionHashes cannot be empty");
        }
        final short minVersion = versionHashes.firstEntry().getKey();
        if (minVersion < SerialVersion.MINIMUM) {
            throw new IllegalArgumentException(
                "Version cannot be less than MINIMUM: " + minVersion);
        }
        final short maxVersion = versionHashes.lastEntry().getKey();
        if (maxVersion > SerialVersion.CURRENT) {
            throw new IllegalArgumentException(
                "Version cannot be greater than CURRENT: " + maxVersion);
        }

        /*
         * Check for duplicate hashes. These probably occur if the test was
         * written thinking that a serial version had introduced a format
         * change, but the format is actually the same.
         */
        {
            boolean haveLastHash = false;
            long lastHash = 0;
            for (Entry<Short, Long> entry : versionHashes.entrySet()) {
                final short v = entry.getKey();
                final long h = entry.getValue();
                if (haveLastHash && (lastHash == h)) {
                    throw new IllegalArgumentException(
                        String.format("Hash for version %d duplicates" +
                                      " hash for previous version: %#x",
                                      v,
                                      h));
                }
                haveLastHash = true;
                lastHash = h;
            }
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        /* Test earlier, unsupported versions */
        for (short v = SerialVersion.MINIMUM; v < minVersion; v++) {
            baos.reset();
            final DataOutput out = new DataOutputStream(baos);
            try {
                writer.writeFastExternal(object, out, v);
                fail("Expected IllegalStateException when using serial" +
                     " version " + v + " to serialize object " + object);
            } catch (IOException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            } catch (IllegalStateException e) {
            } catch (IllegalArgumentException e) {
            }
        }

        /* Test supported versions */
        final List<String> wrongHashes = new ArrayList<>();

        for (Entry<Short, Long> hashEntry : versionHashes.entrySet()) {
            final short startVersion = hashEntry.getKey();
            final long expectedHash = hashEntry.getValue();
            final Short nextKey = versionHashes.higherKey(startVersion);
            final short lastVersion = (nextKey == null) ?
                SerialVersion.CURRENT :
                (short) (nextKey - 1);
            long lastHash = 0;
            for (short v = startVersion; v <= lastVersion; v++) {
                baos.reset();
                final DataOutput out = new DataOutputStream(baos);
                try {
                    writer.writeFastExternal(object, out, v);
                } catch (IOException e) {
                    throw new RuntimeException("Unexpected exception: " + e,
                                               e);
                }
                final byte[] bytes = baos.toByteArray();
                final long outputHash = computeHash(bytes);
                if ((expectedHash != outputHash) &&
                    ((v == startVersion) || (outputHash != lastHash))) {
                    wrongHashes.add(
                        String.format("For object: %s serial version: %d" +
                                      "%s" +
                                      " expected: %#x found: %#x",
                                      object,
                                      v,
                                      (SHOW_BYTES ?
                                       " bytes: " + Arrays.toString(bytes) :
                                       ""),
                                      expectedHash,
                                      outputHash));
                    lastHash = outputHash;
                }
                final DataInputStream in =
                    new DataInputStream(new ByteArrayInputStream(bytes));
                final Object deserialized;
                try {
                    deserialized = reader.readFastExternal(in, v);
                    assertEquals(
                        "Expected EOF after reading serialized object data" +
                        " for object=" + object + " serialVersion=" + v,
                        -1, in.read());
                } catch (Exception e) {
                    throw new RuntimeException(
                        "Unexpected exception while deserializing version " +
                        v + ": " +
                        (SHOW_BYTES ?
                         " bytes: " + Arrays.toString(bytes) :
                         "") +
                        e,
                        e);
                }

                /*
                 * Convert the original object to the expected deserialized
                 * form.
                 */
                final T compareTo =
                    toDeserializedFormOp.toDeserializedForm(object, v);
                assertTrue("Object " + compareTo +
                           (compareTo != object ?
                            ", the deserialized form of original object " +
                            object + "," :
                            "") +
                           " written with serial version " + v +
                           " was not equal to deserialized object " +
                           deserialized,
                           equalsChecker.checkEquals(
                               compareTo, deserialized, v));
            }
        }

        if (!wrongHashes.isEmpty()) {
            fail("Found incorrect hashes: " + wrongHashes);
        }
    }

    /**
     * Returns a reader for the serialized form of the specified object using a
     * constructor with DataInputStream and short parameters.
     */
    private static <T> ReadFastExternal<T> getConstructorReader(T object) {

        /* We know a class of an object has the object's type */
        @SuppressWarnings("unchecked")
        final Class<? extends T> cl = (Class<T>) object.getClass();
        final Constructor<? extends T> constructor;
        try {
            constructor =
                cl.getDeclaredConstructor(DataInput.class, Short.TYPE);
            constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(
                "Constructor with DataInput and short arguments not found" +
                " for " + object,
                e);
        }
        return (in, sv) -> {
            try {
                return cl.cast(constructor.newInstance(in, sv));
            } catch (InstantiationException |
                     IllegalAccessException |
                     InvocationTargetException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
        };
    }

    /** Returns a hash of the specified bytes. */
    private static long computeHash(byte[] bytes) {
        digest.reset();
        final byte[] result = digest.digest(bytes);
        return (((long) result[0] & 0xff) << 56) +
            (((long) result[1] & 0xff) << 48) +
            (((long) result[2] & 0xff) << 40) +
            (((long) result[3] & 0xff) << 32) +
            (((long) result[4] & 0xff) << 24) +
            ((result[5] & 0xff) << 16) +
            ((result[6] & 0xff) << 8) +
            (result[7] & 0xff);
    }
}
