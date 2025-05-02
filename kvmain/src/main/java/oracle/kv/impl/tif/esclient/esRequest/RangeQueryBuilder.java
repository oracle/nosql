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

package oracle.kv.impl.tif.esclient.esRequest;

import java.io.IOException;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;

/*
 * This is a limited Range Query only meant from fields having string/keyword
 * type or Numeric.
 * 
 * Provided for FTS Regression Test Suite.
 */
public class RangeQueryBuilder extends QueryBuilder {

    private static String LTE = "lte";
    private static String GTE = "gte";
    private static String FORMAT = "format";
    private static String TIMEZONE = "time_zone";

    private String fieldName;
    private String gte;
    private String lte;
    private Number numGte;
    private Number numLte;
    private String format;
    private String timeZone;

    public RangeQueryBuilder() {
        this.queryName = "range";
    }

    @Override
    public void setQueryName(String queryName) {
        this.queryName = "range";
    }

    public RangeQueryBuilder fieldName(String fieldName1) {
        this.fieldName = fieldName1;
        return this;

    }

    public RangeQueryBuilder gte(String gte1) {
        this.gte = gte1;
        return this;

    }

    public RangeQueryBuilder lte(String lte1) {
        this.lte = lte1;
        return this;

    }

    public RangeQueryBuilder numGte(Number gte1) {
        this.numGte = gte1;
        return this;

    }

    public RangeQueryBuilder numLte(Number lte1) {
        this.numLte = lte1;
        return this;

    }

    public RangeQueryBuilder format(String format1) {
        this.format = format1;
        return this;
    }

    public RangeQueryBuilder timeZone(String timeZone1) {
        this.timeZone = timeZone1;
        return this;
    }

    public String fieldName() {
        return fieldName;
    }

    public String gte() {
        return gte;
    }

    public String lte() {
        return lte;
    }

    public Number numGte() {
        return numGte;
    }

    public Number numLte() {
        return numLte;
    }

    public String timeZone() {
        return timeZone;
    }

    public String format() {
        return format;
    }

    @Override
    public void buildQueryJson(ESJsonBuilder builder) throws IOException {
        builder.startStructure(fieldName);
        if (lte != null && lte.length() > 0)
            builder.field(LTE, lte);
        if (gte != null && gte.length() > 0)
            builder.field(GTE, gte);
        if (numLte != null)
            builder.field(LTE, numLte);
        if (numGte != null)
            builder.field(GTE, numGte);
        if (format != null && format.length() > 0)
            builder.field(FORMAT, format);
        if (timeZone != null && timeZone.length() > 0)
            builder.field(TIMEZONE, timeZone);
        builder.endStructure();
    }

}
