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

package oracle.kv.impl.tif.esclient.esResponse;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SourceField implements Iterable<Object> {

    private String fieldName;
    private List<Object> fieldValues;

    public SourceField(String name, List<Object> values) {
        this.fieldName = Objects.requireNonNull(name,
                                                "field name can not be null");
        this.fieldValues = Objects.requireNonNull(values,
                                                  "values can not be null");
    }

    public String fieldName() {
        return fieldName;
    }

    public Object singleValue() {
        if (fieldValues != null && !fieldValues.isEmpty()) {
            return fieldValues.get(0);
        }
        return null;
    }

    public List<Object> values() {
        return fieldValues;
    }

    @Override
    public Iterator<Object> iterator() {
        return fieldValues.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceField objects = (SourceField) o;
        return Objects.equals(fieldName, objects.fieldName)
                && Objects.equals(fieldValues, objects.fieldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, fieldValues);
    }

    @Override
    public String toString() {
        return "SourceField{" + "name='" + fieldName + '\'' + ", values="
                + fieldValues + '}';
    }

}