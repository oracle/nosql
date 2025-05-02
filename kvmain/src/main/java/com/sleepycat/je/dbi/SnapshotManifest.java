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
package com.sleepycat.je.dbi;

import static com.sleepycat.je.dbi.BackupManager.SNAPSHOT_PATTERN;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import com.sleepycat.json_simple.JsonException;
import com.sleepycat.json_simple.JsonKey;
import com.sleepycat.json_simple.JsonObject;
import com.sleepycat.json_simple.Jsoner;
import com.sleepycat.utilint.StringUtils;

/**
 * A manifest that lists the contents and copy status of a snapshot.
 */
public class SnapshotManifest {

    private static final boolean PRETTY_PRINT = true;

    /**
     * The message digest format used to compute the manifest checksum
     * in manifests version 1.
     *
     * TODO : Support for specifying manifest checkSum algorithm from
     * user and add manifest checkSum algorithm in manifest file for
     * use in recovery facilities.
     */
    private static final String MD_FORMAT_V1 = "SHA-256";

    /**
     * The current version of the manifest format.
     *
     * <p>Version history:
     *
     * <dl>
     * <dt>Version 0, JE 18.2.8
     * <dd>No version field, so the 0 version is implicit, no validation,
     * checksum is always "0"
     *
     * <dt>Version 1, JE 18.2.11
     * <dd>Added version field, validation, and checksum
     *
     * <dt>Version 2, JE 19.2.0
     * <dd>Added checksumAlg, encryptionAlg, compressionAlg, and verifyFailed
     * fields to LogFileInfo
     * </dl>
     */
    static final int CURRENT_VERSION = 2;

    /** The version of this manifest. Added in version 1. */
    private final int version;

    /** The sequence number of this snapshot, first value 1, no gaps. */
    private final int sequence;

    /** The name of the snapshot in yymmddhh format. */
    private final String snapshot;

    /**
     * The absolute time in milliseconds that the snapshot was created. Used to
     * determine whether files were erased while the snapshot was being copied.
     * Must be greater than zero.
     */
    private final long startTimeMs;

    /**
     * The absolute time in milliseconds when latest file copy was completed,
     * for either a snapshot file or an erased file, or zero if no files were
     * copied.
     */
    private final long lastFileCopiedTimeMs;

    /**
     * The node name of the environment for which this snapshot was created.
     */
    private final String nodeName;

    /**
     * A checksum of the contents of the manifest, for error checking. The
     * checksum is computed from the field names and values, not from the text
     * of the file. The value is an arbitrary precision integer in hex format.
     *
     * <p>For version 0, the value is always "0".
     *
     * <p>For version 1,2 the value is SHA-256 checksum computed from all
     * field values except for the checksum. When parsing a manifest, if
     * the value is "0", then the checksum will not be checked.
     */
    private final String checksum;

    /**
     * The approximate value of the end of log of the snapshot. This value may
     * be -1 if it was not known, otherwise it will be greater than -1.
     */
    private final long endOfLog;

    /**
     * Approximate information about whether the environment was the master of
     * a replication group at the time the snapshot was created. Always false
     * for a non-replicated environment.
     */
    private final boolean isMaster;

    /**
     * Whether the associated snapshot represents a complete snapshot, where
     * all files in the snapshot have been copied, or one that is still in
     * progress or that was terminated without being completed. Must be false
     * if not all snapshot files are marked as copied.
     */
    private volatile boolean isComplete;

    /** Information about all log files included in this snapshot. */
    private final SortedMap<String, LogFileInfo> snapshotFiles;

    /**
     * Information about log files that were erased while the snapshot was
     * being copied. These versions of the log files cannot be used to restore
     * this snapshot.
     */
    private final SortedMap<String, LogFileInfo> erasedFiles;

    /**
     * Utility class for creating {@link SnapshotManifest} instances.
     */
    public static class Builder {
        private int version = CURRENT_VERSION;
        private int sequence = 1;
        private String snapshot;
        private long startTimeMs;
        private long lastFileCopiedTimeMs;
        private String nodeName;
        private String checksum;
        private long endOfLog;
        private boolean isMaster;
        private boolean isComplete;
        private final SortedMap<String, LogFileInfo> snapshotFiles;
        private final SortedMap<String, LogFileInfo> erasedFiles;
        public Builder() {
            snapshotFiles = new TreeMap<>();
            erasedFiles = new TreeMap<>();
        }
        public Builder(final SnapshotManifest base) {
            sequence = base.getSequence();
            snapshot = base.getSnapshot();
            startTimeMs = base.getStartTimeMs();
            lastFileCopiedTimeMs = base.getLastFileCopiedTimeMs();
            nodeName = base.getNodeName();
            endOfLog = base.getEndOfLog();
            isMaster = base.getIsMaster();
            isComplete = base.getIsComplete();
            snapshotFiles = new TreeMap<>(base.getSnapshotFiles());
            erasedFiles = new TreeMap<>(base.getErasedFiles());
        }
        public SnapshotManifest build() {
            return new SnapshotManifest(version,
                                        sequence,
                                        snapshot,
                                        startTimeMs,
                                        lastFileCopiedTimeMs,
                                        nodeName,
                                        checksum,
                                        endOfLog,
                                        isMaster,
                                        isComplete,
                                        snapshotFiles,
                                        erasedFiles);
        }
        public Builder setVersion(final int version) {
            this.version = version;
            return this;
        }
        public Builder setSequence(final int sequence) {
            this.sequence = sequence;
            return this;
        }
        public Builder setSnapshot(final String snapshot) {
            this.snapshot = snapshot;
            return this;
        }
        public Builder setStartTimeMs(final long startTimeMs) {
            this.startTimeMs = startTimeMs;
            return this;
        }
        public Builder setLastFileCopiedTimeMs(
            final long lastFileCopiedTimeMs) {

            this.lastFileCopiedTimeMs = lastFileCopiedTimeMs;
            return this;
        }
        public Builder setNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }
        public Builder setChecksum(final String checksum) {
            this.checksum = checksum;
            return this;
        }
        public Builder setEndOfLog(final long endOfLog) {
            this.endOfLog = endOfLog;
            return this;
        }
        public Builder setIsMaster(final boolean isMaster) {
            this.isMaster = isMaster;
            return this;
        }
        public Builder setIsComplete(final boolean isComplete) {
            this.isComplete = isComplete;
            return this;
        }
        public SortedMap<String, LogFileInfo> getSnapshotFiles() {
            return snapshotFiles;
        }
        public SortedMap<String, LogFileInfo> getErasedFiles() {
            return erasedFiles;
        }
    }

    private SnapshotManifest(
        final int version,
        final int sequence,
        final String snapshot,
        final long startTimeMs,
        final long lastFileCopiedTimeMs,
        final String nodeName,
        final String checksum,
        final long endOfLog,
        final boolean isMaster,
        final boolean isComplete,
        final SortedMap<String, LogFileInfo> snapshotFiles,
        final SortedMap<String, LogFileInfo> erasedFiles)
    {
        this.version = version;
        this.sequence = sequence;
        this.snapshot = snapshot;
        this.startTimeMs = startTimeMs;
        this.lastFileCopiedTimeMs = lastFileCopiedTimeMs;
        this.nodeName = nodeName;
        this.endOfLog = endOfLog;
        this.isMaster = isMaster;
        this.isComplete = isComplete;
        this.snapshotFiles = snapshotFiles;
        this.erasedFiles = erasedFiles;
        validate();
        this.checksum = (checksum == null) ?
                            computeChecksum(MD_FORMAT_V1) : checksum;
    }

    /**
     * Creates a manifest from a parsed Json object.
     *
     * @throws RuntimeException if there is a problem converting the JSON input
     * to a valid SnapshotManifest instance
     */
    private SnapshotManifest(final JsonObject json) {
        final Integer versionValue = json.getInteger(JsonField.version);
        version = (versionValue != null) ? versionValue : 0;
        sequence = getInteger(json, JsonField.sequence);
        snapshot = json.getString(JsonField.snapshot);
        startTimeMs = getLong(json, JsonField.startTimeMs);
        lastFileCopiedTimeMs = getLong(json, JsonField.lastFileCopiedTimeMs);
        nodeName = json.getString(JsonField.nodeName);
        checksum = json.getString(JsonField.checksum);
        endOfLog = getLong(json, JsonField.endOfLog);
        isMaster = getBoolean(json, JsonField.isMaster);
        isComplete = getBoolean(json, JsonField.isComplete);
        snapshotFiles = getLogFileMap(json, JsonField.snapshotFiles, version);
        erasedFiles = getLogFileMap(json, JsonField.erasedFiles, version);
        validate();

        if (!"0".equals(checksum)) {
            final String expectedChecksum =
                computeChecksum(MD_FORMAT_V1);
            if (!expectedChecksum.equals(checksum)) {
                throw new IllegalArgumentException(
                    "Incorrect checksum: expected " + expectedChecksum +
                    ", found " + checksum);
            }
        }
    }

    private static int getInteger(final JsonObject json, final JsonKey field) {
        final Integer value = json.getInteger(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        return value;
    }

    private static long getLong(final JsonObject json, final JsonKey field) {
        final Long value = json.getLong(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        return value;
    }

    private static boolean getBoolean(final JsonObject json,
                                      final JsonKey field) {
        final Boolean value = json.getBoolean(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        return value;
    }

    private static SortedMap<String, LogFileInfo> getLogFileMap(
        final JsonObject json, final JsonField field, final int version) {

        final Map<String, JsonObject> jsonMap = json.getMap(field);
        if (jsonMap == null) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        final SortedMap<String, LogFileInfo> logFileMap = new TreeMap<>();
        for (final Map.Entry<String, JsonObject> entry : jsonMap.entrySet()) {
            final String key = entry.getKey();
            final JsonObject value = entry.getValue();
            if (value == null) {
                throw new IllegalArgumentException(
                    "Key " + key + " missing for field " + field);
            }
            logFileMap.put(key, new LogFileInfo(value, version));
        }
        return logFileMap;
    }

    /**
     * Returns a map that can be used for Json serialization.
     * Returns a sorted map for predictable output order.
     */
    SortedMap<String, Object> toJsonMap() {

        final SortedMap<String, Object> map = new TreeMap<>();

        map.put(JsonField.version.name(), version);
        map.put(JsonField.sequence.name(), sequence);
        map.put(JsonField.snapshot.name(), snapshot);
        map.put(JsonField.startTimeMs.name(), startTimeMs);
        map.put(JsonField.lastFileCopiedTimeMs.name(), lastFileCopiedTimeMs);
        map.put(JsonField.nodeName.name(), nodeName);
        map.put(JsonField.checksum.name(), checksum);
        map.put(JsonField.endOfLog.name(), endOfLog);
        map.put(JsonField.isMaster.name(), isMaster);
        map.put(JsonField.isComplete.name(), isComplete);
        map.put(JsonField.snapshotFiles.name(), getJsonMap(snapshotFiles));
        map.put(JsonField.erasedFiles.name(), getJsonMap(erasedFiles));
        return map;
    }

    private SortedMap<String, Object> getJsonMap(
        final SortedMap<String, LogFileInfo> logFileMap) {

        final SortedMap<String, Object> jsonMap = new TreeMap<>();
        for (final Map.Entry<String, LogFileInfo> entry :
                 logFileMap.entrySet()) {
            jsonMap.put(entry.getKey(), entry.getValue().toJsonMap());
        }
        return jsonMap;
    }

    /** For use with the JsonObject API. */
    private enum JsonField implements JsonKey {
        version,
        sequence,
        snapshot,
        startTimeMs,
        lastFileCopiedTimeMs,
        nodeName,
        checksum,
        endOfLog,
        isMaster,
        isComplete,
        snapshotFiles,
        erasedFiles;

        @Override
        public String getKey() {
            return name();
        }

        @Override
        public Object getValue() {
            return null;
        }
    }

    public int getVersion() {
        return version;
    }

    public int getSequence() {
        return sequence;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getLastFileCopiedTimeMs() {
        return lastFileCopiedTimeMs;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getEndOfLog() {
        return endOfLog;
    }

    public boolean getIsMaster() {
        return isMaster;
    }

    public boolean getIsComplete() {
        return isComplete;
    }

    public void setIsComplete(boolean isComplete) {
        this.isComplete = isComplete;
    }

    public SortedMap<String, LogFileInfo> getSnapshotFiles() {
        return snapshotFiles;
    }

    public SortedMap<String, LogFileInfo> getErasedFiles() {
        return erasedFiles;
    }

    /**
     * Create a serialized form of this instance.
     *
     * @return the serialized form
     * @throws IOException if a problem occurs during serialization
     */
    public byte[] serialize()
        throws IOException {

        return serialize(toJsonMap());
    }

    static byte[] serialize(final SortedMap<String, Object> map)
        throws IOException {

        if (PRETTY_PRINT) {
            final Writer writer = new StringWriter(512);
            Jsoner.serialize(map, writer);
            final String result = Jsoner.prettyPrint(writer.toString());
            return result.getBytes("UTF-8");
        } else {
            final ByteArrayOutputStream out = new ByteArrayOutputStream(512);
            final Writer writer = new OutputStreamWriter(out, "UTF-8");
            Jsoner.serialize(map, writer);
            writer.flush();
            return out.toByteArray();
        }
    }

    /**
     * Create a SnapshotManifest instance from bytes in serialized form.
     *
     * @return the instance
     * @throws IOException if a problem occurs during deserialization, in
     * particular if the data format is invalid
     */
    public static SnapshotManifest deserialize(byte[] bytes)
        throws IOException {

        try {
            return new SnapshotManifest(deserializeToJson(bytes));
        } catch (JsonException|RuntimeException e) {
            final String msg = e.getMessage();
            throw new IOException(msg != null ? msg : e.toString(), e);
        }
    }

    static JsonObject deserializeToJson(byte[] bytes)
        throws JsonException, IOException {

        final ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        final InputStreamReader reader = new InputStreamReader(in, "UTF-8");
        return (JsonObject) Jsoner.deserialize(reader);
    }

    /**
     * Checks if the format of the instance is valid, but does no validation
     * if the version is 0.
     *
     * @throws IllegalArgumentException if the format of the instance is
     * not valid
     */
    public void validate() {
        if (version == 0) {
            return;
        }
        if (version < 0) {
            throw new IllegalArgumentException(
                "version must not be less than 0: " + version);
        }
        if (sequence < 1) {
            throw new IllegalArgumentException(
                "sequence must not be less than 1: " + sequence);
        }
        checkNull("snapshot", snapshot);
        if (!SNAPSHOT_PATTERN.matcher(snapshot).matches()) {
            throw new IllegalArgumentException(
                "snapshot name is invalid: " + snapshot);
        }
        if (startTimeMs <= 0) {
            throw new IllegalArgumentException(
                "startTimeMs must be greater than 0: " + startTimeMs);
        }
        if (lastFileCopiedTimeMs < 0) {
            throw new IllegalArgumentException(
                "lastFileCopiedTimeMs must not be less than 0: " +
                lastFileCopiedTimeMs);
        }
        checkNull("nodeName", nodeName);
        /* Checksum will be checked after validation */
        if (endOfLog < -1) {
            throw new IllegalArgumentException(
                "endOfLog must not be less than -1");
        }
        checkNull("snapshotFiles", snapshotFiles);
        snapshotFiles.forEach((logFile, info) -> {
                checkNull("snapshotFile info for " + logFile, info);
                if (!info.getIsCopied()) {
                    if (isComplete) {
                        throw new IllegalArgumentException(
                            "snapshot cannot be complete when a log file was" +
                            " not copied: " + logFile);
                    }
                } else if (isComplete && info.getVerifyFailed()) {
                    throw new IllegalArgumentException(
                        "snapshot cannot be complete when a log file failed" +
                        " verification: " + logFile);
                } else if (snapshot.equals(info.getSnapshot())) {
                    validateCopiedFile(logFile, info);
                }
            });
        checkNull("erasedFiles", erasedFiles);
        erasedFiles.forEach((logFile, info) -> {
            checkNull("erasedFile info for " + logFile, info);
            if (info.getIsCopied()) {
                if (!snapshot.equals(info.getSnapshot())) {
                    throw new IllegalArgumentException(
                        "Snapshot " + snapshot + " does not match" +
                        " snapshot for copied erased file " + logFile +
                        ": " + info.getSnapshot());
                }
                validateCopiedFile(logFile, info);
            }
        });
    }

    private void validateCopiedFile(final String logFile,
                                    final LogFileInfo info) {
        assert info.getIsCopied();
        assert snapshot.equals(info.getSnapshot());
        final long copyStartTimeMs = info.getCopyStartTimeMs();
        if (copyStartTimeMs < startTimeMs) {
            throw new IllegalArgumentException(
                "copyStartTimeMs " + copyStartTimeMs +
                " for file " + logFile +
                " must not be less than startTimeMs " +
                startTimeMs);
        }
        if (copyStartTimeMs > lastFileCopiedTimeMs) {
            throw new IllegalArgumentException(
                "lastFileCopiedTimeMs " + lastFileCopiedTimeMs +
                " must not be less than copyStartTimeMs " + copyStartTimeMs +
                " for file " + logFile);
        }
    }

    private static void checkNull(final String fieldName, final Object value) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName +
                                               " must not be null");
        }
    }

    private String computeChecksum(final String computeChecksumAlg) {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(computeChecksumAlg);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unexpected failure: " + e, e);
        }
        tallyChecksum(md);
        return BackupManager.checksumToHex(md.digest());
    }

    private void tallyChecksum(final MessageDigest md) {
        tallyChecksumInt(md, version);
        tallyChecksumInt(md, sequence);
        tallyChecksumString(md, snapshot);
        tallyChecksumLong(md, startTimeMs);
        tallyChecksumLong(md, lastFileCopiedTimeMs);
        tallyChecksumString(md, nodeName);
        tallyChecksumLong(md, endOfLog);
        tallyChecksumBoolean(md, isMaster);
        tallyChecksumBoolean(md, isComplete);
        tallyChecksum(md, snapshotFiles);
        tallyChecksum(md, erasedFiles);
    }

    private static void tallyChecksumString(final MessageDigest md,
                                            final String value) {
        md.update(StringUtils.toUTF8(value));
    }

    private static void tallyChecksumBoolean(final MessageDigest md,
                                             final boolean value) {
        md.update((byte) (value ? 1 : 0));
    }

    private static void tallyChecksumInt(final MessageDigest md,
                                         final int value) {
        final byte[] bytes = new byte[4];
        bytes[0] = (byte) (value >>> 24);
        bytes[1] = (byte) (value >>> 16);
        bytes[2] = (byte) (value >>> 8);
        bytes[3] = (byte) value;
        md.update(bytes);
    }

    private static void tallyChecksumLong(final MessageDigest md,
                                          final long value) {
        final byte[] bytes = new byte[8];
        bytes[0] = (byte) (value >>> 56);
        bytes[1] = (byte) (value >>> 48);
        bytes[2] = (byte) (value >>> 40);
        bytes[3] = (byte) (value >>> 32);
        bytes[4] = (byte) (value >>> 24);
        bytes[5] = (byte) (value >>> 16);
        bytes[6] = (byte) (value >>> 8);
        bytes[7] = (byte) value;
        md.update(bytes);
    }

    private void tallyChecksum(
        final MessageDigest md,
        final SortedMap<String, LogFileInfo> logFileMap) {

        tallyChecksumInt(md, logFileMap.size());
        logFileMap.forEach(
            (k, v) -> {
                tallyChecksumString(md, k);
                v.tallyChecksum(md, version);
            });
    }

    @Override
    public String toString() {
        return "SnapshotManifest[" +
            "version:" + version +
            " sequence:" + sequence +
            " snapshot:" + snapshot +
            " startTimeMs:" + startTimeMs +
            " lastFileCopiedTimeMs:" + lastFileCopiedTimeMs +
            " nodeName:" + nodeName +
            " checksum:" + checksum +
            " endOfLog:" + endOfLog +
            " isMaster:" + isMaster +
            " isComplete:" + isComplete +
            " snapshotFiles:" + snapshotFiles +
            " erasedFiles:" + erasedFiles +
            "]";
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SnapshotManifest)) {
            return false;
        }
        final SnapshotManifest other = (SnapshotManifest) object;
        return (version == other.version) &&
            (sequence == other.sequence) &&
            Objects.equals(snapshot, other.snapshot) &&
            (startTimeMs == other.startTimeMs) &&
            (lastFileCopiedTimeMs == other.lastFileCopiedTimeMs) &&
            Objects.equals(nodeName, other.nodeName) &&
            Objects.equals(checksum, other.checksum) &&
            (endOfLog == other.endOfLog) &&
            (isMaster == other.isMaster) &&
            (isComplete == other.isComplete) &&
            Objects.equals(snapshotFiles, other.snapshotFiles) &&
            Objects.equals(erasedFiles, other.erasedFiles);
    }

    @Override
    public int hashCode() {
        int hash = 41;
        hash = (43 * hash) + version;
        hash = (43 * hash) + sequence;
        hash = (43 * hash) + Objects.hashCode(snapshot);
        hash = (43 * hash) + Long.hashCode(startTimeMs);
        hash = (43 * hash) + Long.hashCode(lastFileCopiedTimeMs);
        hash = (43 * hash) + Objects.hashCode(nodeName);
        hash = (43 * hash) + Objects.hashCode(checksum);
        hash = (43 * hash) + Long.hashCode(endOfLog);
        hash = (43 * hash) + Boolean.hashCode(isMaster);
        hash = (43 * hash) + Boolean.hashCode(isComplete);
        hash = (43 * hash) + Objects.hashCode(snapshotFiles);
        hash = (43 * hash) + Objects.hashCode(erasedFiles);
        return hash;
    }

    /**
     * Information associated with a single log file in the snapshot.
     */
    public static class LogFileInfo {

        /**
         * A checksum of the contents of the log file, for error checking, or
         * "0" if not yet computed. The value is an arbitrary precision integer
         * in hex format. For erased files, the value represents the checksum
         * of the data stored in the archive, but that value may not match the
         * checksum for the local file if the file was erased while it was
         * being copied.
         */
        private final String checksum;

        /**
         * Node configured properties, parsed from initProperties in
         * BackupFileCopy for files copy operation. These configured
         * properties are part of manifest file so that correct checksum,
         * encryption and compression algorithm can be used in copy out of
         * jdb files from archive during recovery of node as part of disaster
         * recovery procedure.
         */
        private final String checksumAlg;
        private final String encryptionAlg;
        private final String compressionAlg;

        /**
         * Whether this file has been copied to the archive.
         */
        private final boolean isCopied;

        /**
         * The time the file copy started or 0 if not copied.
         */
        private final long copyStartTimeMs;

        /**
         * The name of the snapshot containing the copied file in the archive.
         */
        private final String snapshot;

        /**
         * The node name of the environment from which the log file was
         * obtained.
         */
        private final String nodeName;

        /**
         * Whether log verification was performed on the log file and the
         * verification failed. If true, then a snapshot that includes this log
         * file is not considered complete, and recovery should not use this
         * log file. The idea is that we might want the corrupted file for
         * by-hand recovery of some sort, but it would not be safe to use it
         * automatically. Added in version 2.
         */
        private volatile boolean verifyFailed;

        public void setVerifyFailed(boolean failed) {
            this.verifyFailed = failed;
        }

        /**
         * Utility class for creating {@link LogFileInfo} instances.
         */
        public static class Builder {
            private String checksum = "0";
            private String checksumAlg = BackupManager.NODE_CHECKSUM_ALG;
            private String encryptionAlg = "NONE";
            private String compressionAlg = "NONE";
            private boolean isCopied;
            private long copyStartTimeMs;
            private String snapshot;
            private String nodeName;
            private boolean verifyFailed;
            public Builder() { }
            public LogFileInfo build() {
                return new LogFileInfo(checksum,
                                       checksumAlg,
                                       encryptionAlg,
                                       compressionAlg,
                                       isCopied,
                                       copyStartTimeMs,
                                       snapshot,
                                       nodeName,
                                       verifyFailed);
            }
            public Builder setChecksum(final String checksum) {
                this.checksum = checksum;
                return this;
            }
            public Builder setChecksumAlg(final String checksumAlg) {
                this.checksumAlg = checksumAlg;
                return this;
            }
            public Builder setEncryptionAlg(final String encryptionAlg) {
                this.encryptionAlg = encryptionAlg;
                return this;
            }
            public Builder setCompressionAlg(final String compressionAlg) {
                this.compressionAlg = compressionAlg;
                return this;
            }
            public Builder setIsCopied(final boolean isCopied) {
                this.isCopied = isCopied;
                return this;
            }
            public Builder setCopyStartTimeMs(final long copyStartTimeMs) {
                this.copyStartTimeMs = copyStartTimeMs;
                return this;
            }
            public Builder setSnapshot(final String snapshot) {
                this.snapshot = snapshot;
                return this;
            }
            public Builder setNodeName(final String nodeName) {
                this.nodeName = nodeName;
                return this;
            }
            public Builder setVerifyFailed(final boolean verifyFailed) {
                this.verifyFailed = verifyFailed;
                return this;
            }
            /**
             * Provides updates for a newly copied file by marking the file as
             * copied, specifying the checksum and copy start time, and getting
             * the snapshot and node name from the manifest.
             */
            public Builder setCopied(final String checksum,
                                     final long copyStartTimeMs,
                                     final SnapshotManifest manifest) {
                this.checksum = checksum;
                this.isCopied = true;
                this.copyStartTimeMs = copyStartTimeMs;
                snapshot = manifest.getSnapshot();
                nodeName = manifest.getNodeName();
                return this;
            }
        }

        private LogFileInfo(final String checksum,
                            final String checksumAlg,
                            final String encryptionAlg,
                            final String compressionAlg,
                            final boolean isCopied,
                            final long copyStartTimeMs,
                            final String snapshot,
                            final String nodeName,
                            final boolean verifyFailed) {
            this.checksum = checksum;
            this.checksumAlg = checksumAlg;
            this.encryptionAlg = encryptionAlg;
            this.compressionAlg = compressionAlg;
            this.isCopied = isCopied;
            this.copyStartTimeMs = copyStartTimeMs;
            this.snapshot = snapshot;
            this.nodeName = nodeName;
            this.verifyFailed = verifyFailed;
            validate();
        }

        /**
         * Creates a LogFileInfo from a parsed Json object.
         *
         * @throws RuntimeException if there is a problem converting the JSON
         * input to a valid LogFileInfo instance
         */
        LogFileInfo(final JsonObject json, final int version) {
            checksum = json.getString(JsonField.checksum);

            /*
             * Support for older version of the manifest file
             */
            if (json.getString(JsonField.checksumAlg) != null) {
                checksumAlg = json.getString(JsonField.checksumAlg);
            } else {
                checksumAlg = BackupManager.NODE_CHECKSUM_ALG;
            }
            if (json.getString(JsonField.encryptionAlg) != null) {
                encryptionAlg = json.getString(JsonField.encryptionAlg);
            } else {
                /*
                 * Matching with new version of manifest file.
                 */
                encryptionAlg = "NONE";
            }
            if (json.getString(JsonField.compressionAlg) != null) {
                compressionAlg = json.getString(JsonField.compressionAlg);
            } else {
                /*
                 * Matching with new version of manifest file.
                 */
                compressionAlg = "NONE";
            }
            isCopied = getBoolean(json, JsonField.isCopied);
            copyStartTimeMs = getLong(json, JsonField.copyStartTimeMs);
            snapshot = json.getString(JsonField.snapshot);
            nodeName = json.getString(JsonField.nodeName);
            verifyFailed = (version >= 2) &&
                getBoolean(json, JsonField.verifyFailed);
            validate();
        }

        /**
         * Returns a map that can be used for Json serialization.
         * Returns a sorted map for predictable output order.
         */
        SortedMap<String, Object> toJsonMap() {

            final SortedMap<String, Object> map = new TreeMap<>();

            map.put(JsonField.checksum.name(), checksum);
            map.put(JsonField.checksumAlg.name(), checksumAlg);
            map.put(JsonField.encryptionAlg.name(), encryptionAlg);
            map.put(JsonField.compressionAlg.name(), compressionAlg);
            map.put(JsonField.isCopied.name(), isCopied);
            map.put(JsonField.copyStartTimeMs.name(), copyStartTimeMs);
            map.put(JsonField.snapshot.name(), snapshot);
            map.put(JsonField.nodeName.name(), nodeName);
            map.put(JsonField.verifyFailed.name(), verifyFailed);

            return map;
        }

        /** For use with the JsonObject API. */
        private enum JsonField implements JsonKey {
            checksum,
            checksumAlg,
            compressionAlg,
            encryptionAlg,
            isCopied,
            copyStartTimeMs,
            snapshot,
            nodeName,
            verifyFailed;

            @Override
            public String getKey() {
                return name();
            }

            @Override
            public Object getValue() {
                return null;
            }
        }

        public String getChecksum() {
            return checksum;
        }

        public String getChecksumAlg() {
            return checksumAlg;
        }

        public String getEncryptionAlg() {
            return encryptionAlg;
        }

        public String getCompressionAlg() {
            return compressionAlg;
        }

        public boolean getIsCopied() {
            return isCopied;
        }

        public long getCopyStartTimeMs() {
            return copyStartTimeMs;
        }

        public String getSnapshot() {
            return snapshot;
        }

        public String getNodeName() {
            return nodeName;
        }

        public boolean getVerifyFailed() {
            return verifyFailed;
        }

        /**
         * Checks if the format of the instance is valid.
         *
         * @throws IllegalArgumentException if the format of the instance is
         * not valid
         */
        public void validate() {
            checkNull("checksum", checksum);
            if (isCopied) {
                if ("0".equals(checksum)) {
                    throw new IllegalArgumentException(
                        "checksum for copied entry must not be \"0\"");
                }
            }
            if (copyStartTimeMs < 0) {
                throw new IllegalArgumentException(
                    "copyStartTimeMs must not be negative: " +
                    copyStartTimeMs);
            }
            if (isCopied) {
                if (copyStartTimeMs == 0) {
                    throw new IllegalArgumentException(
                        "copyStartTimeMs for copied entry must not be 0");
                }
            }
            checkNull("snapshot", snapshot);
            if (!SNAPSHOT_PATTERN.matcher(snapshot).matches()) {
                throw new IllegalArgumentException(
                    "snapshot name is invalid: " + snapshot);
            }
            checkNull("nodeName", nodeName);
            checkNull("checksumAlg", checksumAlg);
            checkNull("encryptionAlg", encryptionAlg);
            checkNull("compressionAlg", compressionAlg);
        }

        void tallyChecksum(final MessageDigest md, final int version) {
            tallyChecksumString(md, checksum);
            if (version >= 2) {
                tallyChecksumString(md, checksumAlg);
                tallyChecksumString(md, encryptionAlg);
                tallyChecksumString(md, compressionAlg);
            }
            tallyChecksumBoolean(md, isCopied);
            tallyChecksumLong(md, copyStartTimeMs);
            tallyChecksumString(md, snapshot);
            tallyChecksumString(md, nodeName);
            if (version >= 2) {
                tallyChecksumBoolean(md, verifyFailed);
            }
        }

        @Override
        public String toString() {
            return "LogFileInfo[" +
                "checksum:" + checksum +
                " checksumAlg:" + checksumAlg +
                " encryptionAlg:" + encryptionAlg +
                " compressionAlg:" + compressionAlg +
                " isCopied:" + isCopied +
                " copyStartTimeMs:" + copyStartTimeMs +
                " snapshot:" + snapshot +
                " nodeName:" + nodeName +
                " verifyFailed:" + verifyFailed +
                "]";
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof LogFileInfo)) {
                return false;
            }
            final LogFileInfo other = (LogFileInfo) object;
            return Objects.equals(checksum, other.checksum) &&
                Objects.equals(checksumAlg, other.checksumAlg) &&
                Objects.equals(encryptionAlg, other.encryptionAlg) &&
                Objects.equals(compressionAlg, other.compressionAlg) &&
                (isCopied == other.isCopied) &&
                (copyStartTimeMs == other.copyStartTimeMs) &&
                Objects.equals(snapshot, other.snapshot) &&
                Objects.equals(nodeName, other.nodeName) &&
                (verifyFailed == other.verifyFailed);
        }

        @Override
        public int hashCode() {
            int hash = 71;
            hash = (73 * hash) + Objects.hashCode(checksum);
            hash = (73 * hash) + Objects.hashCode(checksumAlg);
            hash = (73 * hash) + Objects.hashCode(compressionAlg);
            hash = (73 * hash) + Objects.hashCode(encryptionAlg);
            hash = (73 * hash) + Boolean.hashCode(isCopied);
            hash = (73 * hash) + Long.hashCode(copyStartTimeMs);
            hash = (73 * hash) + Objects.hashCode(snapshot);
            hash = (73 * hash) + Objects.hashCode(nodeName);
            hash = (73 * hash) + Boolean.hashCode(verifyFailed);
            return hash;
        }
    }
}
