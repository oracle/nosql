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
 * An abstract base class for {@link JsonSerializable}.
 *
 * <p>
 * This abstract class implements some basic method that are useful for logging
 * and testing.  The methods equals and hashCode are only expected to be used
 * in the tests.  They only compare with the serialized json results and is not
 * very efficient.
 *
 * <p>
 * Design note: the reason we create this abstract class instead of using
 * default methods in the {@link JsonSerializable} is because Java does not
 * allow default implementations of such methods in an interface. <a
 * href="http://mail.openjdk.java.net/pipermail/lambda-dev/2013-March/008435.html">This
 * email exchange</a> might explain why Java is designed this way.
 */
public abstract class AbstractJsonSerializable implements JsonSerializable {

    @Override
    public String toString() {
        return toJson().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!this.getClass().equals(obj.getClass())) {
            return false;
        }
        final JsonSerializable that = (JsonSerializable) obj;
        return this.toJson().equals(that.toJson());
    }

    @Override
    public int hashCode() {
        return toJson().hashCode();
    }
}
