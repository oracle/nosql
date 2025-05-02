/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.util;

import java.io.DataOutput;
import java.io.IOException;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Implemented by classes that are serialized using an optimized technique
 * that avoids the Java dependency and the performance pitfalls of the standard
 * Serializable and Externalizable approaches. The serialized form is not Java
 * dependent and can be used for transfer over RMI to Java remote recipients
 * as well as to non-Java recipients using some other (non-RMI) transport. It
 * is mainly intended for API operations, but may also be useful for high
 * frequency operations outside of the API, as well as for use with non-Java
 * applications.
 *
 * <h3>Standard 'Slow' Serialization</h3>
 *
 * The simplest way to serialize objects passed and returned over RMI is to
 * make them Serializable.  The class implements the Serializable tag interface
 * and a serialVersionUID field must be declared as follows:
 * <pre>
 *  private static final long serialVersionUID = 1;
 * </pre>
 * With standard Serialization, for every object serialized in an RMI request
 * or response, metadata with a complete description of its fields is included
 * in the request/response.  The metadata is large and redundant among
 * requests, and it takes time to serialize and deserialize.  The
 * deserialization process is particularly lengthy because classes must be
 * looked up and the class metadata compared to the metadata passed in the
 * request/response.
 * <p>
 * Because of its overhead, Serializable is not appropriate for high frequency
 * operations.  However, it is fine and very practical for low frequency
 * operations.
 * <p>
 * The Externalizable interface is an alternative to Serializable.  Like
 * Serializable, the class implements the Externalizable tag interface and a
 * serialVersionUID field must be declared.  In addition, a public no-args
 * constructor must be declared.  With Externalizable, the class must implement
 * the writeExternal and readExternal methods to explicitly serialize and
 * deserialize each field.  The use of Externalizable is therefore much more
 * complex than with Serializable and requires maintenance.
 * <p>
 * Unlike Serializable, the metadata for an Externalizable object does not
 * include a description of its fields.  Instead, only the class name and some
 * per-class metadata is included.  It is consequently much more efficient than
 * Serializable.  However, like with Serializable, a class lookup is performed
 * during deserialization.  In performance tests, the class lookup was found to
 * be have significant overhead.
 * <p>
 * When using Externalizable, the class description and lookup overhead is
 * multiplied by the number of Externalizable classes for instances embedded in
 * the RMI request/response.  This overhead motivated the implementation of a
 * faster method of serialization.
 *
 * <h3>Optimized or 'Fast' Serialization</h3>
 *
 * To implement fully optimized serialization for an RMI interface, the top
 * level objects (parameters and return value) of the remote methods should be
 * Externalizable.  RMI calls ObjectOutput.writeObject and
 * ObjectInput.readObject on every non-primitive parameter and return value, so
 * the classes must be Serializable or Externalizable, and Externalizable is
 * much preferred.
 * <p>
 * The classes of objects contained by (embedded in) the top level parameter
 * and return value objects may be FastExternalizable rather than
 * Externalizable.  This avoids writing the class name of each object, and more
 * importantly the class lookup during deserialization.
 * <p>
 * Like Externalizable, a FastExternalizable class reads and writes its
 * representation directly, a field at a time.  Unlike Serializable, a complete
 * description of the class is not included in the serialized form.
 * <p>
 * Unlike Externalizable, a FastExternalizable class does not write a
 * full class name, and does not do a class lookup or use reflection to create
 * the instance during deserialization.  Instead, a simple integer constant is
 * typically used to identify the class, or in some cases the class is implied
 * from the context, i.e., the field type.  With FastExternalizable, an
 * instance of the class is created directly (with 'new') rather than using
 * reflection, or a constant instance may be used.
 * <p>
 * Unlike Serializable and Externalizable, a FastExternalizable instance cannot
 * be written using ObjectOutput.writeObject or read using
 * ObjectInput.readObject, since these methods are reserved for standard
 * serialization.
 *
 * <h3>The FastExternalizable serialVersion parameter</h3>
 *
 * Remote method input parameters must include a version identifying their
 * serialized format as well as the required serialized format of the return
 * value.  This allows an older client to interact with a newer service.  When
 * the serialization format changes, the service is responsible for supporting
 * older formats and serializing the return value in the format specified by
 * the client's input parameters.  The client is responsible for using the
 * minimum of the version it supports and the version supported by the service,
 * which allows a newer client to interact with an older service.
 * <p>
 * If a client attempts to serialize an object that contains new format data to
 * an older server, it can follow one of two approaches. One is to skip the new
 * data and serialize the object in the old format. This is the right approach
 * if the new data is in some sense optional and it is OK for it to be ignored.
 * But if the data cannot be ignored, then an attempt to serialize the object
 * in the old format should fail with a {@link IllegalStateException}. The
 * exception means that an object with new format data should not have been
 * created (and an attempt made to serialize it) until all parties were
 * upgraded. This approach makes sense for new features that require all
 * servers to be upgraded before an attempt to use data in the new format.
 *
 * <h3>The FastExternalizable.writeFastExternal method</h3>
 *
 * With FastExternalizable, a member method with the same signature (other than
 * method name) as Externalizable.writeExternal is used to write the object.
 * The enclosing class (whether Externalizable or FastExternalizable itself)
 * should call writeFastExternal directly.
 * <p>
 * If a class identifier field is stored for use by a factory method (more on
 * this below) it is written by the writeFastExternal method.
 *
 * <h3>The FastExternalizable constructor</h3>
 *
 * Unlike Externalizable, a constructor rather than a method is used to read
 * the object from a DataInput.
 * <p>
 * A constructor is used rather than a method so that the fields it initializes
 * can be declared 'final' and a no-args constructor is not needed.
 * <p>
 * A constructor signature similar to the following is required in all
 * FastExternalizable classes, by convention.  When a factory method is used,
 * an additional parameter may be required to initialize a class identifier
 * field (more on this below).
 * <pre>
 *   ExampleClass(DataInput in, short serialVersion) throws IOException {
 *       ...
 *   }
 * </pre>
 * If a field may contain instances of only a single class, not its subclasses,
 * and this won't be changing in the future, the constructor can be used
 * directly by the enclosing class to create the instance (with 'new') and read
 * it from the DataInput.  Otherwise, a factory method is used as described
 * next.
 *
 * <h3>The FastExternalizable factory method</h3>
 *
 * If the class is variable (for example, subtypes of the field type may be
 * used) then a static factory method should be defined to create the instance
 * of the correct class and invoke its constructor, following the constructor
 * convention above.
 * <p>
 * A factory method should also be used when the field may contain constant
 * values.  This allows the factory method to return predefined constants
 * rather than creating a new instance.
 * <p>
 * By convention the signature of the factory method is:
 * <pre>
 *   static ExampleClass readFastExternal(DataInput in, short serialVersion) throws IOException {
 *       ...
 *   }
 * </pre>
 * The {@link ReadFastExternal} interface provides an interface that defines
 * this method for cases where an interface is useful.
 * <p>
 * A factory method must first read a class or constant identifier field of
 * some kind.  After reading the identifier field, it can create a new instance
 * of the appropriate class, or perhaps return a constant instance.
 * <p>
 * A common technique for storing a class or instance identifier is to define
 * an Enum class with one value for each class or constant instance, and store
 * the Enum.ordinal() as the identifier.  When reading the identifier the
 * matching Enum is found, and an Enum method may be used to create an instance
 * of the appropriate class or return a constant instance.  Note that Enum
 * values used in this way may not be reordered, or the change in ordinals will
 * break serialization compatibility.
 * <p>
 * When the factory constructs a new instance, it may pass the class identifier
 * (in addition to the DataInput parameter) to the constructor, if the class
 * identifier will be stored in an instance field.  The constructor cannot read
 * the class identifier from the DataInput, since it has already been read by
 * the factory method.
 *
 * <h3>Null values</h3>
 *
 * When an object may be null, by convention we use one byte to indicate this,
 * where 1 means non-null, and 0 means null.  This logic is implemented in the
 * enclosing class for the object that may be null.
 *
 * <h3>Use with non-Java applications</h3>
 *
 * Because the FastExternalizable class does not depend on Java serialization,
 * data serialized in this format can be read and written by non-Java
 * applications.  To better support this sort of interoperability, please
 * describe the data format in the documentation for each writeExternal method,
 * following the conventions found in the documentation for existing methods.
 *
 * <h3>Detecting sequence length values</h3>
 *
 * Although we do not enforce any limits yet (see [#25828] Data length limits),
 * there are security and resource auditing reasons for us to restrict the
 * lengths of arrays and collections during FastExternalizable serialization
 * and deserialization.  To make this process easier in the future, please make
 * sure to use the methods for serializing arrays, collections, and sequence
 * lengths in the SerializationUtil class.
 *
 * <h3>Combining FastExternalizable with other serialization approaches</h3>
 *
 * FastExternalizable may be combined with Serializable or Externalizable in a
 * single class. So far this has only been done for Serializable, and this is
 * straightforward. It is possible to combine FastExternalizable with
 * Externalizable, but the need for this has not yet arisen.
 * <p>
 * For whatever combination is used, the most restrictive rule must be applied:
 * <ul>
 * <li> A serialVersionUID must be included for Serializable and Externalizable,
 *    but not FastExternalizable.
 *
 * <li> A no-args constructor must be included for
 *    Externalizable, but not Serializable or FastExternalizable.  The no-args
 *    constructor must be public for Externalizable.
 *
 * <li> Serialized fields must not be 'final' for
 *    Externalizable, but may be 'final' for Serializable and
 *    FastExternalizable.
 * </ul>
 * <h3>Testing</h3>
 *
 * It is best to create unit tests that check the compatibility for the fast
 * external format across serial versions using the {@code
 * SerialTestUtils.SerialVersionChecker} class and related facilities. Consider
 * creating either a <code><i>Foo</i>SerialVersionTest</code> class or adding a
 * <code>testSerialVersion</code> method to do this testing.
 *
 * <h3>Examples</h3>
 *
 * <dl>
 * <dt>{@link oracle.kv.impl.api.Request}
 * <dd>
 *   Externalizable class that contains FastExternalizable instances.  Is a
 *   top-level parameter of the RMI remote method
 *   {@link oracle.kv.impl.api.RequestHandler#execute}.
 *
 * <dt>{@link oracle.kv.Value}
 * <dd>
 *   FastExternalizable class that includes a constructor but not a factory
 *   method.  Simplest case.
 *
 * <dt>{@link oracle.kv.Consistency}
 * <dd>
 *   FastExternalizable class that includes a factory method for the purpose
 *   of returning a constant instance in some cases.
 *
 * <dt>{@code oracle.kv.impl.api.ops.Result.ValueVersionResult}
 * <dd>
 *   Example of how null embedded objects are handled.
 *
 * <dt>{@link oracle.kv.Operation}
 * <dd>
 *   FastExternalizable class that includes a constructor and factory method
 *   for creating Operation subclasses.  Uses an enum as the class identifier.
 *   {@link oracle.kv.impl.api.ops.Get} is an example of a Operation subclass.
 *
 * <dt>{@link oracle.kv.impl.topo.ResourceId}
 * <dd>
 *   Similar to Operation but is additionally Serializable.
 * </dl>
 */
public interface FastExternalizable {

    /**
     * Writes the FastExternalizable object to the DataOutput.  To read the
     * object, use a constructor or factory method as described in the class
     * comments.
     *
     * @param out the output stream
     * @param serialVersion the serial version
     * @throws IOException if there is a problem writing to the output stream
     * @throws IllegalStateException if the state of this object makes it
     * illegal to serialize it for the requested serial version
     */
    public void writeFastExternal(@NonNull DataOutput out, short serialVersion)
        throws IOException;

    /**
     * Returns an object that represents the form this instance takes when it
     * is serialized and the result deserialized, both with the specified
     * serial version. The returned object should be of the same type as this
     * instance. Implementations can choose to throw {@link
     * IllegalStateException} if this method is called on an object whose state
     * makes it illegal to serialize it for the requested serial version, but
     * that is not required. Returns this instance by default. Classes should
     * override this method if they change the representation of an object when
     * it is serialized. Used for testing.
     *
     * @param serialVersion the serial version for serialization and
     * deserialization
     * @return an object representing the deserialized form
     */
    default FastExternalizable deserializedForm(short serialVersion) {
        return this;
    }
}
