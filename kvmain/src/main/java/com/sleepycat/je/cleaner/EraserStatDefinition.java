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
package com.sleepycat.je.cleaner;

import static com.sleepycat.je.utilint.StatDefinition.StatType.CUMULATIVE;

import com.sleepycat.je.utilint.StatDefinition;

public class EraserStatDefinition {

    public static final String GROUP_NAME = "Eraser";
    public static final String GROUP_DESC =
        "Obsolete data is erased during each erasure cycle.";

    public static final String ERASER_CYCLE_START_NAME =
        "eraserCycleStart";
    public static final String ERASER_CYCLE_START_DESC =
        "Erasure cycle start time (UTC).";
    public static final StatDefinition ERASER_CYCLE_START =
        new StatDefinition(
            ERASER_CYCLE_START_NAME,
            ERASER_CYCLE_START_DESC,
            CUMULATIVE);

    public static final String ERASER_CYCLE_END_NAME =
        "eraserCycleEnd";
    public static final String ERASER_CYCLE_END_DESC =
        "Erasure cycle end time (UTC).";
    public static final StatDefinition ERASER_CYCLE_END =
        new StatDefinition(
            ERASER_CYCLE_END_NAME,
            ERASER_CYCLE_END_DESC,
            CUMULATIVE);

    public static final String ERASER_FILES_REMAINING_NAME =
        "eraserFilesRemaining";
    public static final String ERASER_FILES_REMAINING_DESC =
        "Number of files still to be processed in erasure cycle.";
    public static final StatDefinition ERASER_FILES_REMAINING =
        new StatDefinition(
            ERASER_FILES_REMAINING_NAME,
            ERASER_FILES_REMAINING_DESC,
            CUMULATIVE);

    public static final String ERASER_FILES_ERASED_NAME =
        "eraserFilesErased";
    public static final String ERASER_FILES_ERASED_DESC =
        "Number of files erased by overwriting obsolete entries.";
    public static final StatDefinition ERASER_FILES_ERASED =
        new StatDefinition(
            ERASER_FILES_ERASED_NAME,
            ERASER_FILES_ERASED_DESC);

    public static final String ERASER_FILES_DELETED_NAME =
        "eraserFilesDeleted";
    public static final String ERASER_FILES_DELETED_DESC =
        "Number of reserved files deleted by the eraser.";
    public static final StatDefinition ERASER_FILES_DELETED =
        new StatDefinition(
            ERASER_FILES_DELETED_NAME,
            ERASER_FILES_DELETED_DESC);

    public static final String ERASER_FILES_ALREADY_DELETED_NAME =
        "eraserFilesAlreadyDeleted";
    public static final String ERASER_FILES_ALREADY_DELETED_DESC =
        "Number of reserved files deleted coincidentally by the cleaner.";
    public static final StatDefinition ERASER_FILES_ALREADY_DELETED =
        new StatDefinition(
            ERASER_FILES_ALREADY_DELETED_NAME,
            ERASER_FILES_ALREADY_DELETED_DESC);

    public static final String ERASER_FSYNCS_NAME =
        "eraserFSyncs";
    public static final String ERASER_FSYNCS_DESC =
        "Number of fsyncs performed by the eraser.";
    public static final StatDefinition ERASER_FSYNCS =
        new StatDefinition(
            ERASER_FSYNCS_NAME,
            ERASER_FSYNCS_DESC);

    public static final String ERASER_READS_NAME =
        "eraserReads";
    public static final String ERASER_READS_DESC =
        "Number of file reads performed by the eraser.";
    public static final StatDefinition ERASER_READS =
        new StatDefinition(
            ERASER_READS_NAME,
            ERASER_READS_DESC);

    public static final String ERASER_READ_BYTES_NAME =
        "eraserReadBytes";
    public static final String ERASER_READ_BYTES_DESC =
        "Number of bytes read by the eraser.";
    public static final StatDefinition ERASER_READ_BYTES =
        new StatDefinition(
            ERASER_READ_BYTES_NAME,
            ERASER_READ_BYTES_DESC);

    public static final String ERASER_WRITES_NAME =
        "eraserWrites";
    public static final String ERASER_WRITES_DESC =
        "Number of file writes performed by the eraser.";
    public static final StatDefinition ERASER_WRITES =
        new StatDefinition(
            ERASER_WRITES_NAME,
            ERASER_WRITES_DESC);

    public static final String ERASER_WRITE_BYTES_NAME =
        "eraserWriteBytes";
    public static final String ERASER_WRITE_BYTES_DESC =
        "Number of bytes written by the eraser.";
    public static final StatDefinition ERASER_WRITE_BYTES =
        new StatDefinition(
            ERASER_WRITE_BYTES_NAME,
            ERASER_WRITE_BYTES_DESC);

    public static StatDefinition ALL[] = {
        ERASER_CYCLE_START,
        ERASER_CYCLE_END,
        ERASER_FILES_REMAINING,
        ERASER_FILES_ERASED,
        ERASER_FILES_DELETED,
        ERASER_FILES_ALREADY_DELETED,
        ERASER_FSYNCS,
        ERASER_WRITES,
        ERASER_WRITE_BYTES,
        ERASER_READS,
        ERASER_READ_BYTES,
    };
}
