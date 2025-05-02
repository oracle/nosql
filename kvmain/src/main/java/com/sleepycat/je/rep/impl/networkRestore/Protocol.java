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

package com.sleepycat.je.rep.impl.networkRestore;

import java.nio.ByteBuffer;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.BinaryProtocol;

/**
 * The protocol used to obtain backup files from a LF Feeder. The message
 * exchange is always initiated by the client.
 *
 * The following describes the request/response messages exchanged between the
 * two nodes:
 *
 * {@literal
 *      FeederInfoReq -> FeederInfoResp
 *
 *      FileListReq -> FileListResp
 *
 *      FileInfoReq -> FileInfoResp
 *
 *      FileReq -> FileStart <byte stream> FileEnd
 *
 *      Done
 * }
 *
 * So a complete sequence of successful request messages looks like:
 *
 * FeederInfoReq FileListReq [[FileInfoReq] [FileReq] ]+ Done
 *
 * A response sequence would look like:
 *
 * FeederInfoResp FileListResp [[FileInfoResp] [FileStart <i>byte stream</i> FileEnd] ]+
 *
 * The client may abandon its interaction with the server if it decides the
 * server is overloaded.
 *
 * The client tries to minimize the number of files it actually requests based
 * upon its current state.
 *
 * When a FileReq is received by the server, other files previously requested
 * (using FileReq) may be deleted by the server. These previously requested
 * files must not be requested again using FileReq or FileReqInfo.
 */
public class Protocol extends BinaryProtocol {

    /**
     * Initial version, or at least the first used in KVS
     */
    public static final int VERSION_2 = 2;

    /**
     * JE 20.2
     * - add version negotiation
     * - add FeederInfoResp.logFormat and jeVersion
     * - check that supplier log format is compatible with client
     * - changed hash algorithm to SHA-256
     */
    public static final int VERSION_3 = 3;

    /**
     * JE 24.3.5
     * - change network restore to multi-threaded
     * - add ThreadCount
     */
    public static final int VERSION_4 = 4;

    /* The minimum version we're willing to interact with. */
    static public final int MIN_VERSION = VERSION_2;

    /* The default (highest) version supported by the Protocol code. */
    public static final int MAX_VERSION = VERSION_4;

    /* The messages defined by this class. */
    public final MessageOp FEEDER_INFO_REQ =
        new MessageOp((short)1, FeederInfoReq.class, (ByteBuffer buffer) -> {
            return new FeederInfoReq(buffer);
        });

    public final MessageOp FEEDER_INFO_RESP =
        new MessageOp((short)2, FeederInfoResp.class, (ByteBuffer buffer) -> {
            return new FeederInfoResp(buffer);
        });

    public final MessageOp FILE_LIST_REQ =
        new MessageOp((short)3, FileListReq.class, (ByteBuffer buffer) -> {
            return new FileListReq(buffer);
        });

    public final MessageOp FILE_LIST_RESP =
        new MessageOp((short)4, FileListResp.class, (ByteBuffer buffer) -> {
            return new FileListResp(buffer);
        });

    public final MessageOp FILE_REQ =
        new MessageOp((short)5, FileReq.class, (ByteBuffer buffer) -> {
            return new FileReq(buffer);
        });

    public final MessageOp FILE_START =
        new MessageOp((short)6, FileStart.class, (ByteBuffer buffer) -> {
            return new FileStart(buffer);
        });

    public final MessageOp FILE_END =
        new MessageOp((short)7, FileEnd.class, (ByteBuffer buffer) -> {
            return new FileEnd(buffer);
        });

    public final MessageOp FILE_INFO_REQ =
        new MessageOp((short)8, FileInfoReq.class, (ByteBuffer buffer) -> {
            return new FileInfoReq(buffer);
        });

    public final MessageOp FILE_INFO_RESP =
        new MessageOp((short)9, FileInfoResp.class, (ByteBuffer buffer) -> {
            return new FileInfoResp(buffer);
        });

    public final MessageOp DONE =
        new MessageOp((short)10, Done.class, (ByteBuffer buffer) -> {
            return new Done(buffer);
        });

    public final MessageOp THREAD_COUNT =
            new MessageOp((short)11, ThreadCount.class, (ByteBuffer buffer) -> {
                return new ThreadCount(buffer);
            });

    public Protocol(NameIdPair nameIdPair,
                    NameIdPair remoteNameIdPair,
                    int configuredVersion,
                    EnvironmentImpl envImpl) {

        super(nameIdPair, remoteNameIdPair, configuredVersion, envImpl);

        initializeMessageOps(new MessageOp[]
                             {FEEDER_INFO_REQ,
                              FEEDER_INFO_RESP,
                              FILE_LIST_REQ,
                              FILE_LIST_RESP,
                              FILE_INFO_REQ,
                              FILE_INFO_RESP,
                              FILE_REQ,
                              FILE_START,
                              FILE_END,
                              DONE,
                              THREAD_COUNT});
    }

    /*
     * Returns the hash algorithm used by this version of the protocol.
     */
    String getHashAlgorithm() {
        if (configuredVersion >= VERSION_3) {
            return "SHA-256";
        }
        return "SHA1";
    }

    /* Requests the list of log files that need to be backed up. */
    public class FeederInfoReq extends SimpleMessage {

        public FeederInfoReq() {
            super();
        }

        @SuppressWarnings("unused")
        public FeederInfoReq(ByteBuffer buffer) {
            super();
        }

        @Override
        public MessageOp getOp() {
            return FEEDER_INFO_REQ;
        }
    }

    public class FeederInfoResp extends SimpleMessage {
        /*
         * The number of feeders that are currently busy at this server.
         * Used to prioritize servers.
         */
        private final int activeFeeders;

        /*
         * The vlsn range covered by this server if it's a rep node.
         * Used to prioritize servers.
         */
        private final long rangeFirst;
        private final long rangeLast;

        /*
         * The file format, or -1 if unknown.
         * Used to reject incompatible servers.
         */
        private final int logVersion;

        /*
         * Server JE version, or null if unknown. May be used to evolve the
         * implementation without changing the protocol version.
         */
        private final JEVersion jeVersion;

        public FeederInfoResp(int activeFeeders,
                              long rangeFirst,
                              long rangeLast,
                              int logVersion) {
            super();
            this.activeFeeders = activeFeeders;
            this.rangeFirst = rangeFirst;
            this.rangeLast = rangeLast;
            this.logVersion = logVersion;
            this.jeVersion = JEVersion.CURRENT_VERSION;
        }

        /** Called using reflection. */
        @SuppressWarnings("unused")
        public FeederInfoResp(ByteBuffer buffer) {
            super();
            activeFeeders = LogUtils.readInt(buffer);
            rangeFirst = getVLSN(buffer);
            rangeLast = getVLSN(buffer);
            if (configuredVersion >= VERSION_3) {
                logVersion = LogUtils.readInt(buffer);
                jeVersion = new JEVersion(getString(buffer));
            } else {
                logVersion = -1;
                jeVersion = null;
            }
        }

        @Override
        public MessageOp getOp() {
            return FEEDER_INFO_RESP;
        }

        @Override
        public ByteBuffer wireFormat() {
            return (configuredVersion >= VERSION_3) ?
                wireFormat(activeFeeders, rangeFirst, rangeLast,
                    logVersion, jeVersion.getVersionString()) :
                wireFormat(activeFeeders, rangeFirst, rangeLast);
        }

        public int getActiveFeeders() {
            return activeFeeders;
        }

        public long getRangeFirst() {
            return rangeFirst;
        }

        public long getRangeLast() {
            return rangeLast;
        }

        public int getLogVersion() {
            return logVersion;
        }

        public JEVersion getJEVersion() {
            return jeVersion;
        }
    }

    /* Requests the list of log files that need to be backed up. */
    public class FileListReq extends SimpleMessage {

        public FileListReq() {
            super();
        }

        @SuppressWarnings("unused")
        public FileListReq(ByteBuffer buffer) {
            super();
        }

        @Override
        public MessageOp getOp() {
            return FILE_LIST_REQ;
        }
    }

    /* Response to the above containing the list of files. */
    public class FileListResp extends SimpleMessage {
        private final String[] fileNames;

        public FileListResp(String[] fileNames) {
            super();
            this.fileNames = fileNames;
        }

        /** Called using reflection. */
        @SuppressWarnings("unused")
        public FileListResp(ByteBuffer buffer) {
            fileNames = getStringArray(buffer);
        }

        @Override
        public MessageOp getOp() {
            return FILE_LIST_RESP;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat((Object)fileNames);
        }

        public String[] getFileNames() {
            return fileNames;
        }
    }

    /**
     * Requests that a specific file be sent to the client.
     */
    public class FileReq extends SimpleMessage {

        protected final String fileName;

        public FileReq(String fileName) {
            super();
            this.fileName = fileName;
        }

        public FileReq(ByteBuffer buffer) {
            fileName = getString(buffer);
        }

        @Override
        public MessageOp getOp() {
            return FILE_REQ;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(fileName);
        }

        public String getFileName() {
            return fileName;
        }
    }

    /**
     * Requests information about a specific log file.
     */
    public class FileInfoReq extends FileReq {
        private final boolean needSHA256;

        public FileInfoReq(String fileName, boolean needSHA256) {
            super(fileName);
            this.needSHA256 = needSHA256;
        }

        /** Called using reflection. */
        @SuppressWarnings("unused")
        public FileInfoReq(ByteBuffer buffer) {
            super(buffer);
            needSHA256 = getBoolean(buffer);
        }

        @Override
        public MessageOp getOp() {
            return FILE_INFO_REQ;
        }

        @Override
        public ByteBuffer wireFormat() {
            return super.wireFormat(fileName, needSHA256);
        }

        public boolean getNeedSHA256() {
            return needSHA256;
        }
    }

    /*
     * The Response for information about a specific log file.
     */
    public class FileInfoResp extends FileStart {
        private final byte[] digestSHA256;

        public FileInfoResp(String fileName,
                            long fileLength,
                            long lastModifiedTime,
                            byte[] digestSHA256) {
            super(fileName, fileLength, lastModifiedTime);
            this.digestSHA256 = digestSHA256;
        }

        public FileInfoResp(ByteBuffer buffer) {
            super(buffer);
            this.digestSHA256 = getByteArray(buffer);
        }

        @Override
        public MessageOp getOp() {
            return FILE_INFO_RESP;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(fileName,
                              fileLength,
                              lastModifiedTime,
                              digestSHA256);
        }

        /**
         * Returns the SHA256 value if it was requested, or a zero length byte
         * array if it was not requested.
         */
        public byte[] getDigestSHA256() {
            return digestSHA256;
        }
    }

    /**
     * The message starting the response triple:
     *
     * FileStart <i>byte stream</i> FileEnd
     */
    public class FileStart extends SimpleMessage {
        /* Must match the request name. */
        protected final String fileName;

        /* The actual file length in bytes on disk */
        protected final long fileLength;
        protected final long lastModifiedTime;

        public FileStart(String fileName,
                         long fileLength,
                         long lastModifiedTime) {
            super();
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.lastModifiedTime = lastModifiedTime;
        }

        public FileStart(ByteBuffer buffer) {
            fileName = getString(buffer);
            fileLength = LogUtils.readLong(buffer);
            lastModifiedTime = LogUtils.readLong(buffer);
        }

        @Override
        public MessageOp getOp() {
            return FILE_START;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(fileName, fileLength, lastModifiedTime);
        }

        public long getFileLength() {
            return fileLength;
        }

        public long getLastModifiedTime() {
            return lastModifiedTime;
        }
    }

    /**
     * The message ending the response triple:
     *
     * FileStart <i>byte stream</i> FileEnd
     */
    public class FileEnd extends FileInfoResp {

        public FileEnd(String fileName,
                       long fileLength,
                       long lastModifiedTime,
                       byte[] digestSHA256) {
            super(fileName, fileLength, lastModifiedTime, digestSHA256);
        }

        /** Called using reflection. */
        @SuppressWarnings("unused")
        public FileEnd(ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        public MessageOp getOp() {
            return FILE_END;
        }

        @Override
        public ByteBuffer wireFormat() {
            return super.wireFormat();
        }
    }

    /**
     * Message from client indicating it's done with all the files it needs and
     * that the connection can be terminated.
     */
    public class Done extends SimpleMessage {

        public Done() {
            super();
        }

        @SuppressWarnings("unused")
        public Done(ByteBuffer buffer) {
            super();
        }

        @Override
        public MessageOp getOp() {
            return DONE;
        }
    }


    /**
     * Message from client requesting a certain number of LogFileFeeders
     * to copy files in parallel.
     */
    public class ThreadCount extends SimpleMessage {

        protected final int threadCount;
        public ThreadCount(int threadCount) {
            super();
            this.threadCount = threadCount;
        }

        public ThreadCount(ByteBuffer buffer) {
            threadCount = LogUtils.readInt(buffer);
        }

        @Override
        public MessageOp getOp() {
            return THREAD_COUNT;
        }

        @Override
        public ByteBuffer wireFormat() {
            return wireFormat(threadCount);
        }

        public int getThreadCount() {
            return threadCount;
        }
    }
}
