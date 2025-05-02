/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.jss;

/**
 * This package introduces an API for Json serialization. This serialization
 * format is designed for objects such that it does not cause correctness issue
 * when using default values in the place of missing one. Performance metrics
 * are an example of such objects.
 *
 * <h1>Background</h1>
 * <p>The performance metrics were initially implemented as Java Serializable
 * objects such that they can be passed around for performance collection and
 * monitoring with Java standard facilities such as JMX.  Implementing the Java
 * Serializable interface creates a compatibility issue in that Java
 * serialization have a set of non-trivial rules for mutating fields of a
 * Serializable object. This is extremely inconvenient for performance metric
 * objects because we frequently want to add new fields or filter out
 * unnecessary ones when we understand more about our performance needs. This
 * API is intended to handle this problem.
 *
 * <h1>Overview</h1>
 * <p>The major difficulty with Java Serializable is that new code needs to
 * take care of backward compatibility. That is, the new code must always keep
 * track of the old fields to avoid breaking the old code and over time such
 * tracking efforts accumulate and become increasingly hard to maintain.  This
 * API resolve this by
 * <ul>
 * <li>Describing objects with Json which will reduce the number of cases of
 * incompatible type change since there are far less data types.
 * </li>
 * <li>By using a paradigm that the old code always provide default values if
 * there is a de-serialization error.
 * </li>
 * </ul>
 *
 * <p>Specifically, this package aims to achieve the following goals:
 * <ul>
 * <li>An API and paradigm to serialize and de-serialize objects into Json
 * payload which naturally supports object composition and inheritance.</li>
 * <li>Mutating the fields of serializable objects is naturally be naturally
 * convenient and can avoid compatibility issues in most cases.</li>
 * <li>Compatibility with Java Serialization. This serialization format can be
 * supplimentary to Java Serialization in that the Json payload can be
 * serialized as a String field with Java Serializable objects. Furthermore, we
 * discuss the paradigm to transit from Java Serializable class into this
 * paradigm.</li>
 * </ul>
 *
 * <p>Furthermore, this solution considers the following aspects as side-goals:
 * <ul>
 * <li>Automatic filtering. We find that our implementation with Java
 * Serializable resulted in many insignificant data being printed out (e.g.,
 * zero values for throughput and latency). This paradigm makes it a convention
 * to filter default values.</li>
 * <li>Documentation. This paradigm provides a convention to document the
 * object fields that is easy to maintain.</li>
 * </ul>
 *
 * <p>Aside from serialization, we are also making Json the default interface
 * for interacting with those objects. That is, previously we publish Java
 * interfaces (actually marked hidden for the public, but used by our internal
 * teams) for programmatically query the performance metric objects.  This
 * results in the same compatibility issue as well. Therefore, we want to
 * simply publish Json values.
 *
 * <p>The interface for the Json serializable objects is {@link
 * JsonSerializable}. Classes that can be serialized into {@link ObjectNode}
 * should implement {@link ObjectNodeSerializable}. The {@link
 * JsonSerializationUtils} provides utility methods for implementations.
 *
 * <h1>Compatibility</h1>
 *
 * <h2>Version Upgrade</h2>
 *
 * <p>Json serialization makes the version upgrade issue easier to handle since
 * Json has limited number of data types to consider. Nevertheless, we can not
 * entirely rely on Json. Therefore, an implemenation of the JsonSerializable
 * class must follow two rules that
 * <ul>
 * <li>Each field must have a default value, and</li>
 * <li>Reading a field must provide the default value when the field is missing
 * or of incompatible type.</li>
 * </ul>
 * These two rules solve the backward compatibility issue. In particular,
 * <ul>
 * <li>When adding a field: Old code reading new data does not read the new field;
 * New code reading old data uses the default value.</li>
 * <li>When removing a field: Old code reading new data uses the default value; New
 * code reading old data does not read the removed field.</li>
 * <li>When updating a field:
 * <ul>
 * <li>If the change is a field type change between compatible primitive types,
 * this change is supported by Json.
 * For example, the new code calling {@link JsonSerializationUtils#readLong} on
 * a Json field that was an integer in the old code will not raise any error.
 * The same goes for the opposite direction from the new code to the old code.
 * </li>
 * <li>If the change is a field type change between incompatible types, the
 * default value is used.
 * For example, the new code calling {@link JsonSerializationUtils#readLong} on
 * a Json field that was serialized as a {@code ObjectNode} in the old code
 * will return the default value.
 * The same goes for the opposite direction from the new code to the old code.
 * This type change compatibility is supported for maps or collections of
 * {@code JsonSerializable} by calling {@link JsonSerializationUtils#readList}
 * or {@link JsonSerializationUtils#readMap} or that the compatibility check of
 * the implementation if these convenience methods are not used.</li>
 * <li>For the serialization of Json array or objects of arbitrary types
 * (or similarly collections or maps), the implementation must take extra
 * caution for it to be compatible. For example, the new code of changing an
 * array item from numbers to string must consider the compatibility with old
 * code. Such change is not supported by this interface and paradigm.</li>
 * </ul>
 * In general, updating a field still needs some caution even with Json
 * serialization. On the other hand, if the old code inadventently made a
 * mistake and cannot support forward compatibility, we can always resort to
 * the usual approach of keeping the old fields which we adopt in Java
 * Serialization.
 * </li>
 * </ul>
 *
 * <p>This API introduced a new problem with an incompatible change to the
 * default value. This is sort of the opposite of the field change. If the data
 * types are incompatible, there is no ambiguity when the default value changed
 * between old and new code: they are simply different data. However, if the
 * field types are compatible or the same, changing the default value would
 * make the two side seeing inconsistent data. However, since this API is
 * designed for data-oriented objects such as performance metrics objects and
 * that we only currently use performance data for debugging purposes it seems
 * OK to me. In the future, if the performance data can trigger alerts with
 * default values, we will need to be careful about changing default values. In
 * such cases, we will resort back to the approach of keeping the old fields
 * again.  Most default values are decided in a straightforward manner and are
 * not expected to change, and therefore this is not expected to be a big
 * issue.
 *
 * <h2>Integrate with Java Serialization</h2>
 * <p>We will still need this facility to integrate with Java Serialization for
 * existing mechanisms. The paradigm to do that is for each implementing class
 * to implement an inner static class such as the following.
 * <pre>
 * {@code
 *   class Perf implements JsonSerializable {
 *     ... ...
 *     static class Stats implements Serializable {
 *
 *       private static final long serialVersionUID = 1L;

 *       private transient Perf perf;
 *       private final String serialized;

 *       private Stats(Perf perf) {
 *           this.perf = perf;
 *           this.serialized = perf.toJson().toString();
 *       }

 *       private void readObject(ObjectInputStream in)
 *           throws IOException, ClassNotFoundException {

 *           in.defaultReadObject();
 *           this.perf = new Perf(
 *               JsonUtils.parseJsonObject(serialized));
 *       }

 *       public Perf getPerf() {
 *           return perf;
 *       }
 *     }
 *   }
 * }
 * </pre>
 *
 * <h2>Converting from Java Serialization</h2>
 *
 * <p>Our current paradigm for converting from Java Serialization to Json
 * Serialization is just to implement converting code. In pariticular, every
 * old Java Serializable metrics should have a {@code JsonSerializable}
 * implementation.  The implementations have methods to convert themselves to
 * the Java Serializable counterparts with optional fields that are mostly
 * filtered out to reduce the cost. For the old metrics that the old code is
 * not expected to be missing, both the new {@code JsonSerializable}
 * implementations and the old counterparts needs to be serialized. Otherwise,
 * we can just serialize the new one. For the new code, if we cannot tolerate
 * missing metrics data during upgrade, then we need to convert the old metrics
 * to the new implementation; otherwise, it can be omitted as well.
 *
 * <p>Specifically, in our KV code, the old serialization implementation is
 * ConciseStats and StatsPacket, which I think the old code expects missing
 * entries from StatsPacket. And since only the formatted string of
 * ConciseStats is needed, new entries (from {@code JsonSerializable}) is
 * compatible with the old code as well. For the new code, I do not see a need
 * to convert old metrics into {@code JsonSerializable}. Plus we can tolerate
 * missing performance metrics during upgrade.
 *
 * <p>The same paradigm applies for the API published to upper layers to
 * programmatically interact with the performance metrics. That is, we
 * implement methods to convert {@code JsonSerializable} implementations to old
 * interfaces for backward compatibility but adding new Json representations in
 * the API to move forward.
 *
 * <p>Please see the test code {@code oracle.nosql.common.JsonSerializableTest}
 * and the KV code {@code oracle.kv.impl.async.perf.DialogEndpointPerf} for
 * demonstrations of implementations of this API and paradigm.
 *
 * <h1>Design Notes</h1>
 *
 * <p>One hassle with Using this API/paradigm is the boilerplate code to
 * specify fields (for the types and methods to access the fields). We have
 * provided convenient methods to mitigate the extra efforts, i.e., the
 * ObjectNodeSerializable.Field classes,, but there are still large chunks of
 * code to implement for classes with many fields. Basically we are doing a
 * simplified version of Java Serialization. Or we are doing some of the work
 * that other Json library does (e.g., Gson#fromJson) but in a manual way. We
 * do not want to adopt the third-party approach because we want to limit the
 * surface of our third-party library usage.  Furthermore, there are the
 * following reasons that would argue in favor of the current approach.
 * <ul>
 * <li>Enforcing default values. One of the key aspects of the design is to
 * enforce default values which seems a bit awkward with third-party libraries.
 * </li>
 * <li>Customization for the serialization. Some default values needs to be
 * customized because we associate default values with filtering. Therefore the
 * default value must be exposed through the API and is manually picked during
 * implementation.  Some fields need customized serialization.  For example,
 * even though a metric maybe represented as a Map in the code for quick
 * access. However, we may serialize it into an array when the order of items
 * is significant for the display.
 * </li>
 * </ul>
 *
 * <p>Another caveat of this interface is that the implementation must follows
 * the rules to ensure compatibility. Especially for when the implementations
 * uses customized field types that are not provided with the
 * {@link ObjectNodeSerializable} class.  I think the best way to ensure
 * compatibility is still through testing. For example, we can save serialized
 * performance data in a test file adding more data when we evolve the format
 * and run a unit test to always read the file. The same approach seems to be
 * done by the JE code base.
 *
 * @see JsonSerializable
 * @see ObjectNodeSerializable
 * @see JsonSerializationUtils
 *
 */
