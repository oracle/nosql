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
package oracle.kv.impl.security;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of system privileges within KVStore security system.
 */
public class SystemPrivilege extends KVStorePrivilege {

    private static final long serialVersionUID = 1L;

    /*
     * A map to store system privilege instances used to define system
     * predefined roles.
     */
    private static final EnumMap<KVStorePrivilegeLabel, SystemPrivilege>
        sysPrivilegeMap = new EnumMap<>(KVStorePrivilegeLabel.class);

    /*
     * A lists of canonical system privilege instances corresponding to
     * system privilege labels.
     */
    public static final SystemPrivilege READ_ANY =
        new SystemPrivilege(KVStorePrivilegeLabel.READ_ANY);

    public static final SystemPrivilege WRITE_ANY =
        new SystemPrivilege(KVStorePrivilegeLabel.WRITE_ANY);

    public static final SystemPrivilege SYSVIEW =
        new SystemPrivilege(KVStorePrivilegeLabel.SYSVIEW);

    public static final SystemPrivilege USRVIEW =
        new SystemPrivilege(KVStorePrivilegeLabel.USRVIEW);

    public static final SystemPrivilege SYSOPER =
        new SystemPrivilege(KVStorePrivilegeLabel.SYSOPER);

    public static final SystemPrivilege INTLOPER =
        new SystemPrivilege(KVStorePrivilegeLabel.INTLOPER);

    public static final SystemPrivilege DBVIEW =
        new SystemPrivilege(KVStorePrivilegeLabel.DBVIEW);

    public static final SystemPrivilege SYSDBA =
        new SystemPrivilege(KVStorePrivilegeLabel.SYSDBA);

    public static final SystemPrivilege WRITE_SYSTEM_TABLE =
        new SystemPrivilege(KVStorePrivilegeLabel.WRITE_SYSTEM_TABLE);

    /*
     * A series of useful privilege arrays
     */
    private static final KVStorePrivilege[] emptyPriv =
        new KVStorePrivilege[0];

    private static final KVStorePrivilege[] sysdba =
        new KVStorePrivilege[] { SYSDBA };

    private static final KVStorePrivilege[] dbviewAndSysdba =
        new KVStorePrivilege[] { DBVIEW, SYSDBA };

    private static final KVStorePrivilege[] readany =
        new KVStorePrivilege[] { READ_ANY };

    private static final KVStorePrivilege[] writeany =
        new KVStorePrivilege[] { WRITE_ANY };

    /*
     * Internal used only privilege to allow the access and operation on Avro
     * schemas.
     */
    public static final SystemPrivilege READ_ANY_SCHEMA =
        new SystemPrivilege(KVStorePrivilegeLabel.READ_ANY_SCHEMA) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return dbviewAndSysdba;
        }
    };

    public static final SystemPrivilege WRITE_ANY_SCHEMA =
        new SystemPrivilege(KVStorePrivilegeLabel.WRITE_ANY_SCHEMA) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return sysdba;
        }
    };

    /*
     * System privileges for table operations
     */
    public static final SystemPrivilege READ_ANY_TABLE =
        new SystemPrivilege(KVStorePrivilegeLabel.READ_ANY_TABLE) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return readany;
        }
    };

    public static final SystemPrivilege DELETE_ANY_TABLE =
        new SystemPrivilege(KVStorePrivilegeLabel.DELETE_ANY_TABLE) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return writeany;
        }
    };

    public static final SystemPrivilege INSERT_ANY_TABLE =
        new SystemPrivilege(KVStorePrivilegeLabel.INSERT_ANY_TABLE) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return writeany;
        }
    };

    public static final SystemPrivilege CREATE_ANY_TABLE =
        new SystemPrivilege(KVStorePrivilegeLabel.CREATE_ANY_TABLE) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return sysdba;
        }
    };

    public static final SystemPrivilege DROP_ANY_TABLE =
        new SystemPrivilege(KVStorePrivilegeLabel.DROP_ANY_TABLE) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return sysdba;
        }
    };

    public static final SystemPrivilege EVOLVE_ANY_TABLE =
        new SystemPrivilege(KVStorePrivilegeLabel.EVOLVE_ANY_TABLE) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return sysdba;
        }
    };

    public static final SystemPrivilege CREATE_ANY_INDEX =
        new SystemPrivilege(KVStorePrivilegeLabel.CREATE_ANY_INDEX) {

        private static final long serialVersionUID = 1L;

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return sysdba;
        }
    };

    public static final SystemPrivilege DROP_ANY_INDEX =
        new SystemPrivilege(KVStorePrivilegeLabel.DROP_ANY_INDEX) {

            private static final long serialVersionUID = 1L;

            @Override
            public KVStorePrivilege[] implyingPrivileges() {
                return sysdba;
            }
        };

    public static final SystemPrivilege CREATE_ANY_NAMESPACE =
        new SystemPrivilege(KVStorePrivilegeLabel.CREATE_ANY_NAMESPACE) {

            private static final long serialVersionUID = 1L;

            @Override
            public KVStorePrivilege[] implyingPrivileges() {
                return sysdba;
            }
        };

    public static final SystemPrivilege DROP_ANY_NAMESPACE =
        new SystemPrivilege(KVStorePrivilegeLabel.DROP_ANY_NAMESPACE) {

            private static final long serialVersionUID = 1L;

            @Override
            public KVStorePrivilege[] implyingPrivileges() {
                return sysdba;
            }
        };

    public static final SystemPrivilege CREATE_ANY_REGION =
        new SystemPrivilege(KVStorePrivilegeLabel.CREATE_ANY_REGION) {

            private static final long serialVersionUID = 1L;

            @Override
            public KVStorePrivilege[] implyingPrivileges() {
                return sysdba;
            }
        };

    public static final SystemPrivilege DROP_ANY_REGION =
        new SystemPrivilege(KVStorePrivilegeLabel.DROP_ANY_REGION) {

            private static final long serialVersionUID = 1L;

            @Override
            public KVStorePrivilege[] implyingPrivileges() {
                return sysdba;
            }
        };

     public static final SystemPrivilege SET_LOCAL_REGION =
        new SystemPrivilege(KVStorePrivilegeLabel.SET_LOCAL_REGION) {

            private static final long serialVersionUID = 1L;

            @Override
            public KVStorePrivilege[] implyingPrivileges() {
                return sysdba;
            }
        };

    private static final Set<SystemPrivilege> allSysPrivs =
        new HashSet<SystemPrivilege>(sysPrivilegeMap.values());

    /*
     * A couple of useful privilege lists.
     */
    public static final List<SystemPrivilege> dbviewPrivList =
        Collections.singletonList(SystemPrivilege.DBVIEW);

    public static final List<SystemPrivilege> sysdbaPrivList =
        Collections.singletonList(SystemPrivilege.SYSDBA);

    public static final List<SystemPrivilege> internalPrivList =
        Collections.singletonList(SystemPrivilege.INTLOPER);

    public static final List<SystemPrivilege> sysviewPrivList =
        Collections.singletonList(SystemPrivilege.SYSVIEW);

    public static final List<SystemPrivilege> usrviewPrivList =
        Collections.singletonList(SystemPrivilege.USRVIEW);

    public static final List<SystemPrivilege> sysoperPrivList =
        Collections.singletonList(SystemPrivilege.SYSOPER);

    public static final List<SystemPrivilege> writeOnlyPrivList =
        Collections.singletonList(SystemPrivilege.WRITE_ANY);

    public static final List<SystemPrivilege> readOnlyPrivList =
        Collections.singletonList(SystemPrivilege.READ_ANY);

    public static final List<SystemPrivilege> schemaReadPrivList =
        Collections.singletonList(SystemPrivilege.READ_ANY_SCHEMA);

    public static final List<SystemPrivilege> schemaWritePrivList =
        Collections.singletonList( SystemPrivilege.WRITE_ANY_SCHEMA );

    public static final List<SystemPrivilege> tableCreatePrivList =
        Collections.singletonList( SystemPrivilege.CREATE_ANY_TABLE );

    public static final List<SystemPrivilege> tableDropPrivList =
        Collections.singletonList( SystemPrivilege.DROP_ANY_TABLE );

    public static final List<SystemPrivilege> namespaceCreatePrivList =
        Collections.singletonList( SystemPrivilege.CREATE_ANY_NAMESPACE );

    public static final List<SystemPrivilege> namespaceDropPrivList =
        Collections.singletonList( SystemPrivilege.DROP_ANY_NAMESPACE );

    public static final List<SystemPrivilege> regionCreatePrivList =
        Collections.singletonList( SystemPrivilege.CREATE_ANY_REGION );

    public static final List<SystemPrivilege> regionDropPrivList =
        Collections.singletonList( SystemPrivilege.DROP_ANY_REGION );

    public static final List<SystemPrivilege> regionSetLocalPrivList =
        Collections.singletonList( SystemPrivilege.SET_LOCAL_REGION );


    /**
     * Constructs a system privilege using the specified label.
     */
    private SystemPrivilege(KVStorePrivilegeLabel privLabel) {
        super(privLabel);

        if (privLabel.getType() != PrivilegeType.SYSTEM) {
            throw new IllegalArgumentException(
                "Could not create a system privilege using a non-system " +
                "privilege label " + privLabel);
        }
        sysPrivilegeMap.put(privLabel, this);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
    }

    /**
     * Gets a canonical predefined system privilege instance according to the
     * specified label.
     *
     * @param privLabel KVStore privilege label
     * @return KVStorePrivilege instance
     */
    public static SystemPrivilege get(KVStorePrivilegeLabel privLabel) {

        if (privLabel.getType() != PrivilegeType.SYSTEM) {
            throw new IllegalArgumentException(
                "Could not obtain a system privilege with a non-system " +
                "privilege label " + privLabel);
        }
        return sysPrivilegeMap.get(privLabel);
    }

    /**
     * Return all system privileges.
     */
    public static Set<SystemPrivilege> getAllSystemPrivileges() {
        return allSysPrivs;
    }

    /**
     * Return an empty array as the set of privileges implying this privilege
     * by default. This can be overrided by subclasses so that they can
     * implement new semantics.
     */
    @Override
    public KVStorePrivilege[] implyingPrivileges() {
        return emptyPriv;
    }
}
