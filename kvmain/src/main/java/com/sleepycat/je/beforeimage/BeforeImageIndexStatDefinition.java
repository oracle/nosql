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

package com.sleepycat.je.beforeimage;

import com.sleepycat.je.utilint.StatDefinition;

public class BeforeImageIndexStatDefinition {
    public static final String GROUP_NAME = "BeforeImageIndex";

    public static final String GROUP_DESC = "BeforeImage Index related stats.";

    public static final String N_BIMG_RECORDS_NAME = "nBImgRecords";
    public static final String N_BIMG_RECORDS_DESC = "Number of records added to before image index";
    public static final StatDefinition N_BIMG_RECORDS = new StatDefinition(
            N_BIMG_RECORDS_NAME, N_BIMG_RECORDS_DESC);

    public static final String S_BIMG_DATA_SIZE_NAME = "StoredBImgData";
    public static final String S_BIMG_DATA_SIZE_DESC = "Amount of data stored in before image index";
    public static final StatDefinition S_BIMG_DATA_SIZE = new StatDefinition(
            S_BIMG_DATA_SIZE_NAME, S_BIMG_DATA_SIZE_DESC);

    public static final String N_BIMG_RECORDS_BY_UPDATES_NAME = "nBImgRecordsByUpds";
    public static final String N_BIMG_RECORDS_BY_UPDATES_DESC = "Number of records added to before image index due to updates";
    public static final StatDefinition N_BIMG_RECORDS_BY_UPDATES = new StatDefinition(
            N_BIMG_RECORDS_BY_UPDATES_NAME, N_BIMG_RECORDS_BY_UPDATES_DESC);

    public static final String N_BIMG_RECORDS_BY_DELETES_NAME = "nBImgRecordsByDels";
    public static final String N_BIMG_RECORDS_BY_DELETES_DESC = "Number of records added to before image index due to deletes";
    public static final StatDefinition N_BIMG_RECORDS_BY_DELETES = new StatDefinition(
            N_BIMG_RECORDS_BY_DELETES_NAME, N_BIMG_RECORDS_BY_DELETES_DESC);

    public static final String N_BIMG_RECORDS_BY_TOMBSTONES_NAME = "nBImgRecordsByTombs";
    public static final String N_BIMG_RECORDS_BY_TOMBSTONES_DESC = "Number of records added to before image index due to tombstones";
    public static final StatDefinition N_BIMG_RECORDS_BY_TOMBSTONES = new StatDefinition(
            N_BIMG_RECORDS_BY_TOMBSTONES_NAME,
            N_BIMG_RECORDS_BY_TOMBSTONES_DESC);
}
