/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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
package com.sleepycat.je.utilint;

/**
 * Tag interface used to mark a Serializable class as being not intended for
 * serialization.
 *
 * <p>Classes with this tag interface are excluded from checks for
 * serialization and de-serialization in the SerializeReadObjectsTest.</p>
 *
 * <p>Used mainly for internal exception classes that are handled internally
 * and never thrown to the app or serialized. A common hallmark of such
 * exceptions is a @SuppressWarnings("serial") annotation, indicating that
 * the class is really not serialized since a serialVersionUID field was not
 * added.</p>
 *
 * <p>May also be used for other internal classes that extend a Serializable
 * class, or implement an interface that extends Serializable, but are never
 * serialized.
 */
public interface NotSerializable {
}
