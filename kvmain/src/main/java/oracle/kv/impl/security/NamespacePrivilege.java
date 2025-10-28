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

import static java.util.Locale.ENGLISH;

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.api.table.NameUtils;

public abstract class NamespacePrivilege extends KVStorePrivilege {

    private static final long serialVersionUID = 1L;

    /**
     * Name of the specified namespace. Null means the INITIAL namespace.
     */
    private final String namespace;

    /**
     * Namespace privilege creator.
     */
    interface CreateNsPrivilege {
        NamespacePrivilege createPrivilege(String namespaceName);
    }

    /*
     * A convenient map of namespace privilege creators.
     */
    private static final EnumMap<KVStorePrivilegeLabel, CreateNsPrivilege>
        namespacePrivCreateMap =
            new EnumMap<KVStorePrivilegeLabel, CreateNsPrivilege>(
                KVStorePrivilegeLabel.class);

    static {
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.READ_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new ReadInNamespace(name);
                }
            });
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.INSERT_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new InsertInNamespace(name);
                }
            });
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.DELETE_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new DeleteInNamespace(name);
                }
            });
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.CREATE_TABLE_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new CreateTableInNamespace(name);
                }
            });
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.DROP_TABLE_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new DropTableInNamespace(name);
                }
            });
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.EVOLVE_TABLE_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new EvolveTableInNamespace(name);
                }
            });
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.CREATE_INDEX_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new CreateIndexInNamespace(name);
                }
            });
        namespacePrivCreateMap.put(
            KVStorePrivilegeLabel.DROP_INDEX_IN_NAMESPACE,
            new CreateNsPrivilege() {
                @Override
                public NamespacePrivilege createPrivilege(String name) {
                    return new DropIndexInNamespace(name);
                }
            });
    }

    private NamespacePrivilege(KVStorePrivilegeLabel privLabel,
        String namespaceName) {
        super(privLabel);

        this.namespace = namespaceName;
    }

    /** Reads a NamespacePrivilege subclass. */
    static NamespacePrivilege
        readNamespacePrivilege(DataInput in,
                               short sv,
                               KVStorePrivilegeLabel privLabel)
        throws IOException
    {
        final String namespaceName = readString(in, sv);
        return get(privLabel, namespaceName);
    }

    @Override
    public void writeFastExternal(DataOutput out, short sv)
        throws IOException
    {
        writeString(out, sv, namespace);
    }

    public String getNamespace() {
        return namespace;
    }

    /**
     * Gets a specific namespace privilege instance according to the specific
     * label and namespace information.  It is used in the case that builds a
     * namespace privilege instance according to user-input privilege name. In
     * other cases, it is recommended to directly get the instances via
     * constructors for efficiency.
     *
     * @param privLabel label of the privilege
     * @param namespaceName namespace name
     * @return namespace privilege instance specified by the label
     */
    public static NamespacePrivilege get(KVStorePrivilegeLabel privLabel,
        String namespaceName) {
        if (privLabel.getType() != PrivilegeType.NAMESPACE) {
            throw new IllegalArgumentException(
                "Could not obtain a namespace privilege with a non-namespace " +
                    "privilege label " + privLabel);
        }

        final CreateNsPrivilege creator = namespacePrivCreateMap.get(privLabel);
        if (creator == null) {
            throw new IllegalArgumentException(
                "Could not find a namespace privilege with label of " +
                    privLabel);
        }
        return creator.createPrivilege(namespaceName);
    }

    /**
     * Return all namespace privileges on namespace with given name.
     */
    public static Set<KVStorePrivilege> getAllNamespacePrivileges(
        String namespaceName) {

        final Set<KVStorePrivilege> namespacePrivs =
            new HashSet<KVStorePrivilege>();
        for (KVStorePrivilegeLabel privLabel : namespacePrivCreateMap.keySet())
        {
            namespacePrivs.add(get(privLabel, namespaceName));
        }
        return namespacePrivs;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        final NamespacePrivilege otherNamespacePriv = (NamespacePrivilege)other;
        return NameUtils.namespaceEquals(namespace,
            otherNamespacePriv.namespace);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 17 * prime + super.hashCode();
        result = result * prime +
            (namespace == null ? 0 : namespace.toUpperCase(ENGLISH).hashCode());
        return result;
    }

    /**
     * A namespace privilege appears in the form of PRIV_NAME(NAMESPACE_NAME)
     */
    @Override
    public String toString() {

        return String.format("%s(%s)", getLabel(),
            NameUtils.switchToExternalUse(namespace));
    }

    public final static class ReadInNamespace extends NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.READ_ANY,
                                     SystemPrivilege.READ_ANY_TABLE };

        public ReadInNamespace(String namespaceName) {
            super(KVStorePrivilegeLabel.READ_IN_NAMESPACE, namespaceName);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class InsertInNamespace extends NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY,
                                     SystemPrivilege.INSERT_ANY_TABLE };

        public InsertInNamespace(String namespace) {
            super(KVStorePrivilegeLabel.INSERT_IN_NAMESPACE, namespace);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class DeleteInNamespace extends NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.WRITE_ANY,
                                     SystemPrivilege.DELETE_ANY_TABLE };

        public DeleteInNamespace(String namespaceName) {
            super(KVStorePrivilegeLabel.DELETE_IN_NAMESPACE, namespaceName);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class CreateTableInNamespace extends
        NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.CREATE_ANY_TABLE };

        public CreateTableInNamespace(String namespaceName) {
            super(KVStorePrivilegeLabel.CREATE_TABLE_IN_NAMESPACE,
                namespaceName);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class DropTableInNamespace extends
        NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.DROP_ANY_TABLE };

        public DropTableInNamespace(String namespaceName) {
            super(KVStorePrivilegeLabel.DROP_TABLE_IN_NAMESPACE, namespaceName);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class EvolveTableInNamespace extends
        NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.EVOLVE_ANY_TABLE };

        public EvolveTableInNamespace(String namespaceName) {
            super(KVStorePrivilegeLabel.EVOLVE_TABLE_IN_NAMESPACE,
                namespaceName);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class CreateIndexInNamespace extends
        NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.CREATE_ANY_INDEX };

        public CreateIndexInNamespace(String namespaceName) {
            super(KVStorePrivilegeLabel.CREATE_INDEX_IN_NAMESPACE,
                namespaceName);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }

    public final static class DropIndexInNamespace extends
        NamespacePrivilege {

        private static final long serialVersionUID = 1L;

        private static final KVStorePrivilege[] implyingPrivs =
            new KVStorePrivilege[] { SystemPrivilege.SYSDBA,
                                     SystemPrivilege.DROP_ANY_INDEX };

        public DropIndexInNamespace(String namespaceName) {
            super(KVStorePrivilegeLabel.DROP_INDEX_IN_NAMESPACE, namespaceName);
        }

        @Override
        public KVStorePrivilege[] implyingPrivileges() {
            return implyingPrivs;
        }
    }
}
