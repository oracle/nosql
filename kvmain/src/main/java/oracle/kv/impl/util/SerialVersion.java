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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import oracle.kv.KVVersion;

/**
 * Defines the previous and current serialization version for services and
 * clients.
 *
 * <p>As features that affect serialized formats are introduced constants
 * representing those features should be added here, associated with the
 * versions in which they are introduced. This creates a centralized location
 * for finding associations of features, serial versions, and release
 * versions. Existing constants (as of release 4.0) are spread throughout the
 * source and can be moved here as time permits.
 *
 * <p>Oracle NoSQL versions have what's called a "prerequisite" version, which
 * is the oldest version supported by a given release using compatibility
 * interfaces. Normally this is a release 2 years in the past.  This version is
 * also the oldest one that can be upgraded in one step to the current
 * versions. As mentioned above new features that affect protocol/serial
 * version should have constants in this file for use by conditional code.
 *
 * At some point a version becomes an old version, and one that is older than
 * the prerequisite. At that point the conditional code can be removed because
 * the older version is simply not supported. An example is code like this.
 * Note that QUERY_VERSION_9 is a constant in this file.
 * <code>
 *   if (serialVersion >= QUERY_VERSION_9) {
 *       theTableAliases = PlanIter.deserializeStringArray(in, serialVersion);
 *   } else {
 *       theTableAliases = null;
 *   }
 * </code>
 * Once the prerequisite version is QUERY_VERSION_9 or higher the "if"
 * condition is always true so the conditional can be removed.
 *
 * There are other ways to "notify" a developer that code will expire with a
 * new prerequisite version. That's using a static declaration in the affected
 * files that looks like this:
 * <code>
 *    static {
 *        assert KVVersion.PREREQUISITE_VERSION.
 *                compareTo(KVVersion.R22_2) <= 0 :
 *                "Code to convert table MD can be removed";
 *    }
 * </code>
 * This type of code will fail at runtime when the prerequisite version
 * exceeds R22_2. When dealing with code that will "age out" leaving this
 * sort of construct is very helpful for whoever is updating the prerequisite
 * version. One issue with this construct is that it's not 100% clear what
 * code is involved. When creating this sort of reminder please provide enough
 * information so that the maintainer can easily find the dead code. Adding
 * to the message, comments in both this construct and the affected code are
 * all ways to do this.
 *
 * When adding a new feature that affects serial version do these things
 * <ol>
 * <li>create a constant in this file for the feature, with a comment</li>
 * <li>add required conditional code to handle compatibility issues</li>
 * <li>leave a comment or better yet, a static block indicating when
 * the conditional code can be removed (see below for updating prequisite
 * version)</li>
 * <li>in many cases there are serialization tests that will need to be
 * updated to handle the new version. Most of these have "SerialTest" in
 * their names. These tests can be run in this manner:
 * <code>
 * mvn -Pit.kvstore -pl kvtest/kvstore-IT -am verify -Dit.test="**SerialTest**"
 * </code>
 * </li>
 * </ol>
 *
 * When updating the prerequisite version do these things:
 * <ol>
 * <li>Update the prerequisite version in KVVersion.java and this file,
 * SerialVersion</li>
 * <li>In KVVsersion move the "Supported versions" comment and mark all
 * older versions as deprecated (the pattern there is clear)</li>
 * <li>Manually search the code for SerialVersion constants that are
 * older than the new prerequisite (e.g. QUERY_VERSION_10) and remove
 * conditionals related to them. One way to do this is to comment out the
 * not-supported constants and look for compilation issues</li>
 * <li>Manually search the code (incl tests) for KVVersion constants older than
 * the new preequisite (e.g. KVVersion.R21_3 and adjust code as needed. This
 * will catch some of the static blocks mentioned above</li>
 * <li>run a warning check -- this finds issues as well</li>
 * <li>run tests. This will catch any static blocks missed</li>
 * <li>serialization tests will need to be updated because they will fail.
 * Most (not all) of these have "SerialTest" in their names and will import
 * SerialTestUtils. These tests can be run in this manner:
 * <code>
 * mvn -Pit.kvstore -pl kvtest/kvstore-IT -am verify -Dit.test="**SerialTest**"
 * </code>
 * Note that when a hash mismatch is found it's not usually a problem. The
 * hashes for specific versions should generally remain the same. When the
 * minimum (prereq) version passes certain features the hash associated with
 * SerialVersion.MINIMUM will change and needs to be updated in the test.
 * HINT -- the hash test failure only catches one at a time. It can be more
 * efficient to temporarily replace the assertion in SerialTestUtils with a
 * println to handle multiple changes in one run.
 * </li>
 * </ol>
 *
 * @see oracle.kv.impl.util.registry.VersionedRemote
 */
/*
 * Suppress deprecation warnings for references to unsupported (and deprecated)
 * KVVersion fields
 */
@SuppressWarnings("deprecation")
public class SerialVersion {

    public static final short UNKNOWN = -1;

    private static final Map<Short, KVVersion> kvVersions = new HashMap<>();

    private static final TreeMap<KVVersion, Short> versionMap = new TreeMap<>();

    /* Unsupported versions */

    /* R1 version */
    private static final short V1 = 1;
    static { init(V1, KVVersion.R1_2_123); }

    /* Introduced at R2 (2.0.23) */
    private static final short V2 = 2;
    static { init(V2, KVVersion.R2_0_23); }

    /* Introduced at R2.1 (2.1.8) */
    private static final short V3 = 3;
    static { init(V3, KVVersion.R2_1); }

    /*
     * Introduced at R3.0 (3.0.5)
     *  - secondary datacenters
     *  - table API
     */
    private static final short V4 = 4;
    static { init(V4, KVVersion.R3_0); }

    /* Introduced at R3.1 (3.1.0) for role-based authorization */
    private static final short V5 = 5;
    static { init(V5, KVVersion.R3_1); }

    /*
     * Introduced at R3.2 (3.2.0):
     * - real-time session update
     * - index key iteration
     */
    private static final short V6 = 6;
    static { init(V6, KVVersion.R3_2); }

    /*
     * Introduced at R3.3 (3.3.0) for secondary Admin type and JSON flag to
     * verifyConfiguration, and password expiration.
     */
    private static final short V7 = 7;
    static { init(V7, KVVersion.R3_2); }

    /*
     * Introduced at R3.4 (3.4.0) for the added replica threshold parameter on
     * plan methods, and the CommandService.getAdminStatus,
     * repairAdminQuorum, and createFailoverPlan methods.
     * Also added MetadataNotFoundException.
     *
     * Added bulk get APIs to Key/Value and Table interface.
     */
    private static final short V8 = 8;
    static { init(V8, KVVersion.R3_4); }

    /*
     * Introduced at R3.5 (3.5.0) for Admin automation V1 features, including
     * json format output, error code, and Kerberos authentication.
     *
     * Added bulk put APIs to Key/Value and Table interface.
     */
    private static final short V9 = 9;
    static { init(V9, KVVersion.R3_5); }

    /*
     * The first version that support admin CLI output in JSON string. Keep
     * this field public because this value is used for JSON entrypoints
     * separate from use as a serial version.
     */
    public static final short ADMIN_CLI_JSON_V1_VERSION = V9;

    /*
     * Introduced at R4.0/V10:
     * - new query protocol operations. These were added in V10, but because
     *   they were "preview" there is no attempt to handle V10 queries in
     *   releases > V10. Because the serialization format of queries has changed
     *   such operations will fail with an appropriate message.
     * - time to live
     * - Arbiters
     * - Full text search
     */
    private static final short V10 = 10;
    static { init(V10, KVVersion.R4_0); }

    /*
     * Introduced at R4.1/V11
     * - SN/topology contraction
     * - query protocol change (not compatible with V10)
     * - new SNA API for mount point sizes
     */
    private static final short V11 = 11;
    static { init(V11, KVVersion.R4_1); }

    /*
     * Introduced at R4.2/V12
     * - query protocol change (compatible with V11)
     * - getStorageNodeInfo added to SNA
     * - indicator bytes in indexes
     */
    private static final short V12 = 12;
    static { init(V12, KVVersion.R4_2); }

    /*
     * Introduced at R4.3/V13
     * - new SNI API for checking parameters
     */
    private static final short V13 = 13;
    static { init(V13, KVVersion.R4_3); }

    /*
     * Introduced at R4.4/V14
     * - Standard UTF-8 encoding
     * - typed indexes for JSON, affecting index plans
     * - SFWIter.theDoNullOnEmpty field
     * - Snapshot command is executed on the server side, using the admin to
     *   coordinate operations and locking.
     * - add TableQuery.mathContext field
     * - changed the value of the NULL indicator in index keys and
     *   added IndexImpl.serialVersion.
     */
    private static final short V14 = 14;
    static { init(V14, KVVersion.R4_4); }


    /* Introduced at R4.5/V15
     * - Switched query and DDL statements to char[] from String.
     * - Added currentIndexRange field in TableQuery op
     * - BaseTableIter may carry more than 1 keys and ranges
     * - Add TABLE_V1 to Value.Format
     * - added theIsUpdate field in BaseTableIter and ReceiveIter
     */
    private static final short V15 = 15;

    static { init(V15, KVVersion.R4_5); }

    /*
     * - Added members theIndexTupleRegs and theIndexResultReg in BaseTableIter
     * - Master affinity zone feature.
     * - Added failed shard removal.
     * - Added verify data feature.
     * - Added table limits
     * - Added LogContext string in Request.
     * - Check running subscription feeder before running elasticity operation
     * - Add maxReadKB to Table/Index iterate operation
     */
    private static final short V16 = 16;
    static { init(V16, KVVersion.R18_1); }

    /*
     * Support all admin CLI for JSON output v2. Keep this field public because
     * this value is used for JSON entrypoints separate from use as a serial
     * version.
     */
    public static final short ADMIN_CLI_JSON_V2_VERSION = V16;

    /*
     * Introduced at R18.2/V17
     * - Add getTable with optional cost
     */
    private static final short V17 = 17;
    static { init(V17, KVVersion.R18_2); }

    /*
     * Introduced at R18.3/V17
     * - Enable extended table namespace support
     */
    private static final short V18 = 18;
    static { init(V18, KVVersion.R18_3); }

    /*
     * Introduced at R19.1/V19
     * - New RepNodeAdmin shutdown API
     * - Update the verify data API
     */
    private static final short V19 = 19;
    static { init(V19, KVVersion.R19_1); }

    /*
     * Introduced at R19.2/V20
     * - Added IndexImpl.skipNulls field
     */
    private static final short V20 = 20;
    static { init(V20, KVVersion.R19_2); }

    /*
     * Introduced at R19.3/V21
     * - Renamed IndexImpl.notIndexNulls to IndexImpl.skipNulls
     */
    private static final short V21 = 21;
    static { init(V21, KVVersion.R19_3); }

    /*
     * Introduced at R19.5/V22
     * - Changed the plan field of WriteNewAdminParams from
     *   ChangeAdminParamsPlan to AbstractPlan, making it more general
     * - Add support for multi-region tables
     * - Enable the async request handler on the server
     * - Added stacktrace to async exceptions
     */
    private static final short V22 = 22;
    static { init(V22, KVVersion.R19_5); }

    /*
     * Introduced at R20.1/V23
     * - Changed BaseTableIter
     * - Alter multi-region table
     * - Add new table metadata info keys
     */
    public static final short V23 = 23;
    static { init(V23, KVVersion.R20_1); }

    /* public static final short QUERY_VERSION_9 = V23; */

    /* public static final short ROW_MODIFICATION_TIME_VERSION = V23; */

    /* public static final short ALTER_MULTI_REGION_TABLE = V23; */

    /*
     * Introduced at R20.2/V24
     * - PutResolve with expiration time
     */
    public static final short V24 = 24;
    static { init(V24, KVVersion.R20_2); }

    /* public static final short PUT_RESOLVE_EXPIRATION_TIME = V24; */

    /* public static final short QUERY_VERSION_10 = V24; */

    /*
     * Introduced at R20.3/V25
     * - PutResult and ValueVersionResult with row storage size
     * - Add isUUID to StringDefImpl
     * - Add ServiceLogStats to monitoring stats
     */
    public static final short V25 = 25;
    static { init(V25, KVVersion.R20_3); }

    /* public static final short QUERY_VERSION_11 = V25; */

    /* UUID data type */
    public static final short UUID_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 = V25;

    /*
     * Store seq # on a per table bases and changed semantics of the
     * sequence number returned in the Result object
     */
    public static final short
        TABLE_SEQ_NUM_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 = V25;

    /**
     * Support for {@link oracle.kv.impl.measurement.LoggingStats}.
     */
    /* public static final short LOGGING_STATS_VERSION = V25; */

    /*
     * Introduced at R21.1/V26
     * - MRCounter columns.
     */
    public static final short V26 = 26;
    static { init(V26, KVVersion.R21_1); }

    /*
     * Allow MRCounter columns.
     */
    public static final short
        COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 = V26;

    /*
     * Introduced at R21.2/V27
     * - Added isUnique field in IndexImpl
     * - New ClientRepNodeAdmin service
     * - Async versions of miscellaneous services used by clients
     * - Remove RMI callback objects
     */
    public static final short V27 = 27;
    static { init(V27, KVVersion.R21_2); }

    public static final short
        QUERY_VERSION_12_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 = V27;

    /**
     * Added async versions of miscellaneous services used by clients:
     * ClientAdminService, RepNodeAdmin, and UserLogin.
     */
    /* public static final short MISC_ASYNC_CLIENT_SERVICES_VERSION = V27; */

    /** Remove RMI callback objects */
    /* public static final short REMOVE_RMI_CALLBACKS_VERSION = V27; */

    /* Admin command to move a single partition. */
    /* public static final short MOVE_SINGLE_PARTITION = V27; */

    /*
     * Introduced at R21.3/V28
     * - Add API to get multi-region tables
     * - Allow JSON MRCounter.
     * - Multi-region child tables
     * - Added version checking for MRT agent
     * - Java serialization format for async calls
     * - Async versions of miscellaneous services used by all callers
     * - Add resource ID to FaultException
     * - Add no charge field to Request
     */
    public static final short V28 = 28;
    static { init(V28, KVVersion.R21_3); }

    /* Add API to get multi-region tables */
    public static final short
        MRT_INFO_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 = V28;

    /*
     * Allow JSON MRCounter.
     */
    public static final short
        JSON_COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 = V28;

    /* Multi-region child tables. */
    public static final short
        MULTI_REGION_CHILD_TABLE_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 =
        V28;

    /* Added version checking for MRT agent */
    /* public static final short MRT_AGENT_VERSION_VERSION = V28; */

    /**
     * Java serialization format for async versioned remote calls
     */
    /* public static final short JAVA_SERIAL_ASYNC_VERSION = V28; */

    /**
     * Added async versions of all remaining miscellaneous services for use by
     * all callers.
     */
    /* public static final short MISC_ASYNC_SERVICES_VERSION = V28; */

    /**
     * Added version checking due to modification of FaultException
     * class with the addition of the resourceId instance variable
     */
    /* public static final short FAULT_EXCEPTION_RESOURCE_ID_VERSION = V28; */

    /**
     * Introduced at R22.1/V29
     * - Change drop request of multi-region table
     * - NetworkAddress subclasses
     */
    public static final short V29 = 29;
    static { init(V29, KVVersion.R22_1); }

    /** Added NetworkAddress subclasses. */
    /* public static final short NETWORK_ADDRESS_SUBCLASS_VERSION = V29; */

    /**
     * Introduced at R22.2/V30
     * - Change table metadata serialization in RN
     * - Add getVlsn API to clientRepNodeAdmin
     * - Add noCharge option to Request
     */
    public static final short V30 = 30;
    static { init(V30, KVVersion.R22_2); }

    /* public static final short CLIENT_RN_ADMIN_GET_VLSN_VERSION = V30; */

    /* Supported versions */

    /**
     * Introduced at R22.3/V31
     * - Add reason parameters to service shutdown methods
     */
    public static final short V31 = 31;
    static { init(V31, KVVersion.R22_3); }

    /** Add reason parameters to service shutdown methods */
    public static final short SHUTDOWN_REASON_VERSION = V31;

    /**
     * Introduced at R22.4/V32
     * - Added doTombstone to Delete, MultiDeleteTable,
     * - Added localRegionId to PutResolve
     * - Added localRegionId, doTombstone to TableQuery
     * - Added schemaless table boolean (protocol only)
     * - Added theTopFieldNames to InsertRowInter
     */
    public static final short V32 = 32;
    static { init(V32, KVVersion.R22_4); }

    public static final short CLOUD_MR_TABLE = V32;
    public static final short SCHEMALESS_TABLE_VERSION = V32;
    public static final short QUERY_VERSION_13 = V32;

    /**
     * Introduced at R23.3/V33
     * - Changes in ResumeInfo to support queries during elasticity
     * - Changes in QueryResult to carry the query trace
     * - Support maxServerMemoryConsumption field in TableQuery
     * - Support multiple mr agents
     * - Put table metadata in a system table
     * - Added RequestTimeoutException.dispatchEventTrace
     * - MR counter in TableImpl serialization, affects table
     *   creation and TableImpl itself
     * - Improvements to RemoteTestInterface
     */
    public static final short V33 = 33;
    static { init(V33, KVVersion.R23_3); }

    /** Support maxServerMemoryConsumption field in TableQuery. */
    public static final short QUERY_VERSION_14 = V33;

    /** Added version to support multiple mr agents */
    public static final short MULTI_MRT_AGENT_VERSION = V33;

    /** Put table metadata in a system table. */
    public static final short TABLE_MD_IN_STORE_VERSION = V33;

    /** Added RequestTimeoutException.dispatchEventTrace. */
    public static final short REQUEST_TIMEOUT_EXCEPTION_TRACE = V33;

    /* JSON collections support -- added MR counters */
    public static final short JSON_COLLECTION_VERSION = V33;

    /**
     * Modify RemoteTestInterface to change the status parameter for
     * processExit to a restart parameter and to remove the status parameter
     * from processHalt.
     */
    public static final short REMOTE_TEST_INTERFACE_IMPROVEMENTS = V33;

    /**
     * Introduced at R24.1/V34
     * - BulkPut enhanced to allow PutResolve
     */
    public static final short V34 = 34;
    static { init(V34, KVVersion.R24_1); }

    /**
     * Add information to BulkPut to allow it to do PutResolve
     */
    public static final short BULK_PUT_RESOLVE = V34;

    /**
     * Introduced at R24.2/V35
     * - Enhance table iteration to optionally include tombstones
     * - New SQL functions for TIMESTAMP type
     * - Added methods in ClientAdminServiceAPI to support various admin
     *   related operations required for getStore
     */
    public static final short V35 = 35;
    static { init(V35, KVVersion.R24_2); }

    /**
     * Extends {@link oracle.kv.table.TableIterator} to optionally include
     * tombstones
     */
    public static final short TABLE_ITERATOR_TOMBSTONES_VER = V35;

    /**
     * New SQL functions for TIMESTAMP type
     */
    public static final short QUERY_VERSION_15 = V35;

    /**
     * New ClientAdminServiceAPI methods to support getStore using admin service
     */
    public static final short CLIENT_ADMIN_SERVICE_GET_STORE_VERSION = V35;

    /**
     * Introduced at R24.4/V36
     * - Query: Update query supports updating multiple rows with same shard key
     * - Commands to update and show TLS credentials
     * - Allocate and assign index id to the metadata of each newly created index
     * - Table id, table schema version, and index id stored in query execution plan
     */
    public static final short V36 = 36;
    static { init(V36, KVVersion.R24_4); }

    public static final short QUERY_VERSION_16 = V36;

    /**
     * Add commands to update and show TLS credentials.
     */
    public static final short TLS_CREDENTIALS_VERSION = V36;

    /**
     * Introduced at R25.1/V37
     * - New fields in UpdateRowIter, to specify what indexes need to be updated
     * - New field (thePosInJoin) in BaseTableIter
     */
    public static final short V37 = 37;
    static { init(V37, KVVersion.R25_1); }

    public static final short QUERY_VERSION_17 = V37;

    /**
     * When adding a new version and updating DEFAULT_CURRENT, be sure to make
     * corresponding changes in KVVersion as well as the files referenced from
     * there to add a new release version. See {@link KVVersion#CURRENT_VERSION}
     */
    private static final short DEFAULT_CURRENT = V37;

    /*
     * The default earliest supported serial version.
     */
    private static final short DEFAULT_MINIMUM = V31;

    /*
     * Check that the default minimum version matches the KVVersion
     * prerequisite version, since that is the first supported KV version.
     */
    static {
        assert KVVersion.PREREQUISITE_VERSION == getKVVersion(DEFAULT_MINIMUM);
    }

    /*
     * The earliest supported serial version.  Clients and servers should both
     * reject connections from earlier versions.
     */
    public static final short MINIMUM = Integer.getInteger(
        "oracle.kv.minimum.serial.version", DEFAULT_MINIMUM).shortValue();

    static {
        if (MINIMUM != DEFAULT_MINIMUM) {
            System.err.println("Setting SerialVersion.MINIMUM=" + MINIMUM);
        }
    }

    /**
     * The current serial version, with a system property override for use in
     * testing.
     */
    public static final short CURRENT = Integer.getInteger(
        "oracle.kv.test.currentserialversion", DEFAULT_CURRENT).shortValue();

    static {
        if (CURRENT != DEFAULT_CURRENT) {
            System.err.println("Setting SerialVersion.CURRENT=" + CURRENT);
        }
    }

    private static short init(int version, KVVersion kvVersion) {
        if (version > Short.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Version needs to be less than Short.MAX_VALUE, found: " +
                version);
        }
        final short shortVersion = (short) version;
        kvVersions.put(shortVersion, kvVersion);
        if (shortVersion > MINIMUM) {
            versionMap.put(kvVersion, shortVersion);
        }
        return shortVersion;
    }

    public static KVVersion getKVVersion(short serialVersion) {
        return kvVersions.get(serialVersion);
    }

    /**
     * Returns the maximum serial version supported by the specified KV version.
     * Returns MINIMUM if the KV version is less than the supported versions.
     */
    public static short getMaxSerialVersion(KVVersion kvVersion) {
        final Entry<KVVersion, Short> e = versionMap.floorEntry(kvVersion);
        return e == null ? MINIMUM : e.getValue();
    }

    /**
     * Creates an appropriate exception for a client that does not meet the
     * minimum required version.
     *
     * @param clientSerialVersion the serial version of the client
     * @param requiredSerialVersion the minimum required version
     * @return an appropriate exception
     */
    public static UnsupportedOperationException clientUnsupportedException(
        short clientSerialVersion, short requiredSerialVersion) {

        return new UnsupportedOperationException(
            "The client is incompatible with this service. " +
            "Client version is " +
            getKVVersion(clientSerialVersion).getNumericVersionString() +
            ", but the minimum required version is " +
            getKVVersion(requiredSerialVersion).getNumericVersionString());
    }

    /**
     * Creates an appropriate exception for a server that does not meet the
     * minimum required version.
     *
     * @param serverSerialVersion the serial version of the server
     * @param requiredSerialVersion the minimum required version
     * @return an appropriate exception
     */
    public static UnsupportedOperationException serverUnsupportedException(
        short serverSerialVersion, short requiredSerialVersion) {

        return new UnsupportedOperationException(
            "The server is incompatible with this client.  " +
            "Server version is " +
            getKVVersion(serverSerialVersion).getNumericVersionString() +
            ", but the minimum required version is " +
            getKVVersion(requiredSerialVersion).getNumericVersionString());
    }
}
