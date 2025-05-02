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

package oracle.kv.impl.security.metadata;

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readMap;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeMap;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.RoleInstance.RoleDescription;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.security.metadata.SecurityMDChange.SecurityMDChangeType;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;

/**
 * Class for security metadata. The class contains all elements of security
 * metadata, e.g., user data, role data, audit policy and other information
 * needed in future. Currently, the KVStoreUser and RoleInstance information
 * are included.
 * <p>
 * The security metadata can be fully updated as whole. And it also supports
 * delta-update mechanism. A history change list is used to keep all changes
 * happened to the security metadata copy. All the changes can also be applied
 * to another copy to make it updated to the targeted status.
 * <p>
 * The SecurityMetadata is NOT thread safe. The concurrency protocol for it is
 * to always give out copies. Updates will be made on a new copy, and the old
 * one will be replaced as a whole. This can save the complex concurrency
 * control and corruption recovery mechanism.
 * <p>
 * On Admin side, the SecurityMetadata copy will be fetched from EntityStore
 * on each get. And the update will be performed in plans, which will be
 * executed serially. On the RepNode side, the access to SecurityMetadata is
 * coordinated by SecurityMetadataManager, in which concurrent updates and read
 * will be protected by "synchronized".
 */
public class SecurityMetadata implements Metadata<SecurityMetadataInfo>,
                                         Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The types of elements stored in security metadata.
     */
    public enum SecurityElementType
            /*
             * A Java 8 compiler bug (in JDK 1.8.0_281 at least) seems to
             * require a fully qualified class name for FastExternalizable here
             */
            implements oracle.kv.impl.util.FastExternalizable {
        KVSTOREUSER(0, KVStoreUser::readUser,
                    (elem, outp, sv) ->
                    ((KVStoreUser) elem).writeUser(outp, sv)),
        KVSTOREROLE(1, RoleInstance::new),
        KRBPRINCIPAL(2, KerberosInstance::new);

        private static final SecurityElementType[] VALUES = values();
        private final ReadFastExternal<SecurityElement> reader;
        private final WriteFastExternal<SecurityElement> writer;

        SecurityElementType(final int ordinal,
                            final ReadFastExternal<SecurityElement> reader) {
            this(ordinal, reader, FastExternalizable::writeFastExternal);
        }

        SecurityElementType(final int ordinal,
                            final ReadFastExternal<SecurityElement> reader,
                            final WriteFastExternal<SecurityElement> writer) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
            this.writer = writer;
        }

        /** Reads a security element type. */
        static
        SecurityElementType readFastExternal(DataInput in,
                                             @SuppressWarnings("unused")
                                             short serialVersion)
            throws IOException
        {
            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unknown SecurityElementType: " + ordinal);
            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        /** Reads a security element type and the  associated element. */
        static SecurityElement readSecurityElement(DataInput in,
                                                   short serialVersion)
            throws IOException
        {
            final SecurityElementType elementType =
                readFastExternal(in, serialVersion);
            return elementType.reader.readFastExternal(in, serialVersion);
        }

        /** Write the security element associated with this type. */
        void writeSecurityElement(DataOutput out,
                                  short serialVersion,
                                  SecurityElement element)
            throws IOException
        {
            writeFastExternal(out, serialVersion);
            writer.writeFastExternal(element, out, serialVersion);
        }
    }

    private final String id;

    private final KVStoreUserMap kvstoreUserMap;

    private KVStoreRoleMap kvstoreRoleMap;

    private KerberosInstanceMap krbInstanceMap;

    /* TODO: need to include AuditPolicy in future */

    /* Used to identify which store the metadata belongs to */
    private final String kvstoreName;

    private final LinkedList<SecurityMDChange> changeList;

    private int sequenceNumber = Metadata.EMPTY_SEQUENCE_NUMBER;

    /* Descriptions of system build-in assignable roles */
    private static final TreeMap<String, RoleDescription> builtInRoleInfoMap =
        new TreeMap<>();

    static {
        builtInRoleInfoMap.put(RoleInstance.DBADMIN_NAME,
                               RoleInstance.DBADMIN.getDescription());
        builtInRoleInfoMap.put(RoleInstance.READONLY_NAME,
                               RoleInstance.READONLY.getDescription());
        builtInRoleInfoMap.put(RoleInstance.READWRITE_NAME,
                               RoleInstance.READWRITE.getDescription());
        builtInRoleInfoMap.put(RoleInstance.SYSADMIN_NAME,
                               RoleInstance.SYSADMIN.getDescription());
        builtInRoleInfoMap.put(RoleInstance.WRITEONLY_NAME,
                               RoleInstance.WRITEONLY.getDescription());
        builtInRoleInfoMap.put(RoleInstance.PUBLIC_NAME,
                               RoleInstance.PUBLIC.getDescription());
        builtInRoleInfoMap.put(RoleInstance.WRITESYSTABLE_NAME,
                               RoleInstance.WRITESYSTABLE.getDescription());
    }

    /**
     * Create a SecurityMetadata instance with KVStore name. The ID is
     * "SecurityMD-" + currentTimeMillis by default.
     */
    public SecurityMetadata(final String storeName) {
        this(storeName, "SecurityMD-" + System.currentTimeMillis());
    }

    /**
     * Create a SecurityMetadata instance with specified KVStore name and ID.
     */
    public SecurityMetadata(final String storeName, final String id) {
        this.id = id;
        kvstoreUserMap = new KVStoreUserMap(this);
        kvstoreRoleMap = new KVStoreRoleMap(this);
        krbInstanceMap = new KerberosInstanceMap(this);
        changeList = new LinkedList<>();
        kvstoreName = storeName;
    }

    public SecurityMetadata(DataInput in, short serialVersion)
        throws IOException
    {
        id = readString(in, serialVersion);
        kvstoreUserMap = new KVStoreUserMap(in, serialVersion, this);
        kvstoreRoleMap = new KVStoreRoleMap(in, serialVersion, this);
        krbInstanceMap = new KerberosInstanceMap(in, serialVersion, this);
        kvstoreName = readString(in, serialVersion);
        changeList = readCollection(in, serialVersion, LinkedList::new,
                                    SecurityMDChange::readSecurityMDChange);
        sequenceNumber = readPackedInt(in);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writeString(out, serialVersion, id);
        kvstoreUserMap.writeFastExternal(out, serialVersion);
        kvstoreRoleMap.writeFastExternal(out, serialVersion);
        krbInstanceMap.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, kvstoreName);
        writeCollection(out, serialVersion, changeList,
                        SecurityMDChange::writeSecurityMDChange);
        writePackedInt(out, sequenceNumber);
    }

    public String getKVStoreName() {
        return kvstoreName;
    }

    public String getId() {
        return id;
    }

    /**
     * Returns a KVStore user with the specified name.
     *
     * @param name user name
     * @return the user, null if no user with the specified name is found.
     */
    public KVStoreUser getUser(final String name) {
        if ((name == null) || name.isEmpty()) {
            return null;
        }

        final Collection<KVStoreUser> users = kvstoreUserMap.getAll();
        for (final KVStoreUser user : users) {
            if (name.equals(user.getName())) {
                return user;
            }
        }
        return null;
    }

    /**
     * Returns a KVStore user-defined role with the specified name.
     *
     * @param name role name
     * @return the role, null if no role with the specified name is found.
     */
    public RoleInstance getRole(final String name) {
        if ((name == null) || name.isEmpty()) {
            return null;
        }

        final Collection<RoleInstance> roles = kvstoreRoleMap.getAll();
        for (final RoleInstance role : roles) {
            if (name.equalsIgnoreCase(role.name())) {
                return role;
            }
        }
        return null;
    }

    /**
     * Returns Kerberos principal instance name of given storage node.
     *
     * @param snId storage node Id
     * @return principal instance name, null if no one with the specified
     * storage node id is found.
     */
    public KerberosInstance getKrbInstance(final StorageNodeId snId) {
        if ((snId == null) || snId.getStorageNodeId() == 0) {
            return null;
        }

        final Collection<KerberosInstance> instances = krbInstanceMap.getAll();
        for (final KerberosInstance instance : instances) {
            if (snId.equals(instance.getStorageNodeId())) {
                return instance;
            }
        }
        return null;
    }

    /**
     * Returns a KVStore user with the specified ID.
     *
     * @param uid user id
     * @return the user, null if no user with the specified ID is found.
     */
    public KVStoreUser getUserById(final String uid) {
        return kvstoreUserMap.get(uid);
    }

    /**
     * Returns a KVStore user-defined role with the specified ID.
     *
     * @param rid role id
     * @return the role, null if no role with the specified ID is found.
     */
    public RoleInstance getRoleById(final String rid) {
        return kvstoreRoleMap.get(rid);
    }

    /**
     * Returns a Kerberos principal instance name with the specified ID.
     *
     * @param kid kerberos instance name id
     * @return instance name, null if no one with the specified ID is found.
     */
    public KerberosInstance getKrbInstanceById(final String kid) {
        return krbInstanceMap.get(kid);
    }

    /**
     * Returns all users.
     */
    public Collection<KVStoreUser> getAllUsers() {
        return kvstoreUserMap.getAll();
    }

    /**
     * Returns all Kerberos principal instance names.
     */
    public Collection<KerberosInstance> getAllKrbInstanceNames() {
        return krbInstanceMap.getAll();
    }

    /**
     * Returns all user-defined roles.
     */
    public Collection<RoleInstance> getAllRoles() {
        return kvstoreRoleMap.getAll();
    }

    /**
     * Returns the internal KVStoreUser map.
     *
     * @return KVStoreUserMap
     */
    public KVStoreUserMap getKVStoreUserMap() {
        return kvstoreUserMap;
    }

    /**
     * Returns the internal RoleInstance map.
     *
     * @return RoleInstanceMap
     */
    public KVStoreRoleMap getRoleInstanceMap() {
        return kvstoreRoleMap;
    }

    /**
     * Returns the internal Kerberos principal instance map.
     *
     * @return KerberosInstanceMap
     */
    public KerberosInstanceMap getKerberosInstanceMap() {
        return krbInstanceMap;
    }

    /**
     * Adds a new user into the map.
     *
     * @param user user instance
     * @return the newly added user
     */
    public KVStoreUser addUser(final KVStoreUser user) {
        return kvstoreUserMap.add(user);
    }

    /**
     * Adds a new role into the map.
     *
     * @param role user-defined role
     * @return the newly added role
     */
    public RoleInstance addRole(final RoleInstance role) {
        return kvstoreRoleMap.add(role);
    }

    /**
     * Adds service principal instance name of given storage node into the map.
     *
     * @param instanceName Kerberos principal instance name
     * @param snId storage node Id
     * @return KerberosInstance of this storage node if adding succeed or null
     *         if given storage node id is invalid or its instance name exists
     *         in metadata already.
     */
    public KerberosInstance addKerberosInstanceName(final String instanceName,
                                                    final StorageNodeId snId) {
        return krbInstanceMap.addInstanceName(instanceName, snId);
    }

    /**
     * Removes a user with the specified ID. If the user does not exist, an
     * IllegalArgumentException will be thrown.
     *
     * @param uid user ID
     * @return the removed user if successful
     * @throws IllegalArgumentException if no user with specified ID is found
     */
    public KVStoreUser removeUser(final String uid) {
        return kvstoreUserMap.remove(uid);
    }

    /**
     * Removes a role with the specified ID. If the role does not exist, an
     * IllegalArgumentException will be thrown.
     *
     * @param rid role ID
     * @return the removed role if successful
     * @throws IllegalArgumentException if no role with specified ID is found
     */
    public RoleInstance removeRole(final String rid) {
        return kvstoreRoleMap.remove(rid);
    }

    /**
     * Removes service principal instance name from the map with the specified
     * StorageNodeId.
     *
     * @param snId storage node Id
     * @return the removed instance or null if not found
     */
    public KerberosInstance removeKrbInstanceName(final StorageNodeId snId) {
        return krbInstanceMap.removeInstanceName(snId);
    }

    /**
     * Updates an existing user of the specified ID with a new user data. If
     * the user does not exist, an IllegalArgumentException will be thrown.
     *
     * @param uid user ID
     * @param user user
     * @return the new user if successful
     * @throws IllegalArgumentException if no user with specified ID is found
     */
    public KVStoreUser updateUser(final String uid, final KVStoreUser user) {
        return kvstoreUserMap.update(uid, user);
    }

    /**
     * Updates an existing role of the specified ID with a new role data. If
     * the role does not exist, an IllegalArgumentException will be thrown.
     *
     * @param rid role ID
     * @param role user-defined role
     * @return the new role if successful
     * @throws IllegalArgumentException if no role with specified ID is found
     */
    public RoleInstance updateRole(final String rid,
                                      final RoleInstance role) {
        return kvstoreRoleMap.update(rid, role);
    }

    /**
     * Verifies if the plain password works for the given user.
     *
     * @param userName user name
     * @param password the plain password to verify
     * @return true iff. the specified password works
     */
    public boolean verifyUserPassword(final String userName,
                                      final char[] password) {
        final KVStoreUser user = getUser(userName);
        if (user == null) {
            return false;
        }
        return user.verifyPassword(password);
    }

    /**
     * Returns the brief and detailed description of all users for showing. The
     * descriptions in the returned Map are ordered by user name.
     *
     * @return a SortedMap of user descriptions which are sorted by user name
     */
    public SortedMap<String, UserDescription> getUsersDescription() {
        final Collection<KVStoreUser> users = getAllUsers();

        /* Use TreeMap to order the description by users' names */
        final TreeMap<String, UserDescription> userInfoMap = new TreeMap<>();
        for (final KVStoreUser user : users) {
            userInfoMap.put(user.getName(), user.getDescription());
        }
        return userInfoMap;
    }

    /**
     * Returns the brief and detailed description of all roles for showing. The
     * descriptions in the returned Map are ordered by role name.
     *
     * @return a SortedMap of role descriptions which are sorted by role name
     */
    public SortedMap<String, RoleDescription> getRolesDescription() {
        final TreeMap<String, RoleDescription> roleDescMap =
            new TreeMap<String, RoleDescription>(builtInRoleInfoMap);

        for (final RoleInstance role : kvstoreRoleMap.getAll()) {
            roleDescMap.put(role.name(), role.getDescription());
        }
        return roleDescMap;
    }

    /**
     * Returns the description of the current session user for showing. If
     * could not identify current user, null will be returned.
     *
     * @return Current user's description, null will be returned if current
     * user could not be identified or does not exist in metadata.
     */
    public Map<String, UserDescription> getCurrentUserDescription() {

        final KVStoreUserPrincipal currentUserPrinc =
            KVStoreUserPrincipal.getCurrentUser();
        if (currentUserPrinc == null) {
            return null;
        }

        final KVStoreUser currentUser =
            kvstoreUserMap.get(currentUserPrinc.getUserId());
        if (currentUser == null) {
            return null;
        }

        final Map<String, UserDescription> userInfoMap = new HashMap<>();
        userInfoMap.put(currentUser.getName(), currentUser.getDescription());
        return userInfoMap;
    }

    /**
     * Check if given user is the last enabled user having sysadmin role
     * in the system.
     */
    public boolean isLastSysadminUser(String userName) {
        final KVStoreUser existUser = getUser(userName);
        if (existUser != null &&
            existUser.isAdmin()) {

            for (final KVStoreUser user : getAllUsers()) {
                if (!user.getName().equals(userName) &&
                    user.isEnabled() &&
                    user.isAdmin()) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Applies a series of change on the security metadata. The changes applied
     * should be in increasing sequence number order.
     * <p>
     * Note that the object may be corrupted because of unexpected exceptions
     * while applying changes. For simplicity we do not implement recovering
     * codes for such failure. So a safe way to call the operation is to get
     * a deep copy of the object first, and then apply the changes on the copy.
     * If the applying failed, the corrupted copy could be simply discarded.
     *
     * @param changes list of SecurityMDChange
     * @return true if any update is made
     */
    public boolean apply(final List<SecurityMDChange> changes) {
        if (changes == null || changes.isEmpty()) {
            return false;
        }

        if (changes.get(0).getSeqNum() > (getSequenceNumber() + 1)) {
            throw new IllegalStateException(
                "Unexpected gap in security metadata sequence. Current " +
                "sequence = " + getSequenceNumber() + ", first change =" +
                changes.get(0).getSeqNum());
        }

        int changedCount = 0;
        for (final SecurityMDChange change : changes) {
            /* Skip the change already existed */
            if (change.getSeqNum() <= getSequenceNumber()) {
                continue;
            }

            if (change.getElementType() == SecurityElementType.KVSTOREUSER) {
                kvstoreUserMap.apply(change);
                changedCount++;
            } else if (change.getElementType() ==
                       SecurityElementType.KVSTOREROLE) {
                kvstoreRoleMap.apply(change);
                changedCount++;
            } else if (change.getElementType() ==
                       SecurityElementType.KRBPRINCIPAL) {
                krbInstanceMap.apply(change);
                changedCount++;
            }  else {
                throw new IllegalArgumentException(
                    "Unknown security element type: " +
                    change.getElementType());
            }
        }
        return changedCount > 0;
    }

    @Override
    public MetadataType getType() {
        return MetadataType.SECURITY;
    }

    /**
     * Returns the current change sequence number associated with security
     * metadata. If no change has been logged, the
     * {@code Metadata.EMPTY_SEQUENCE_NUMBER} will be return.
     */
    @Override
    public int getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Returns a list of security metadata changes starting with
     * <code>startSeqNum</code>. The list of changes will be wrapped into the
     * SecurityMetadataInfo object and will be used in the metadata framework.
     *
     * @param startSeqNum the inclusive start of the sequence of
     * changes to be returned
     *
     * @return the list of changes starting with startSeqNum and ending with
     * getSequenceNumber(). Return null if startSequenceNumber &gt;
     * getSequenceNumber() or if the security metadata does not contain changes
     * at startSeqNum because they have been discarded.
     */
    @Override
    public SecurityMetadataInfo getChangeInfo(int startSeqNum) {
        return new SecurityMetadataInfo(this, getChanges(startSeqNum));
    }

    @Override
    public SecurityMetadata pruneChanges(final int minRetainSeqNum,
                                         final int maxChange) {
        final int firstChangeSeqNum = getFirstChangeSeqNum();
        if (firstChangeSeqNum < 0) {
            /* No change to prune */
            return this;
        }

        final int newStartSeqNum = Math.min(getSequenceNumber() - maxChange + 1,
                                            minRetainSeqNum);

        if (newStartSeqNum <= firstChangeSeqNum) {
            /* Nothing to prune. */
            return this;
        }

        final Iterator<SecurityMDChange> itr = changeList.iterator();
        while (itr.hasNext() && (itr.next().getSeqNum() < newStartSeqNum)) {
            itr.remove();
        }
        return this;
    }

    /**
     * Gets the first sequence number of the changes stored in the tracker.
     *
     * @return the first sequence number of the changes, or -1 if no change
     * exists yet.
     */
    public int getFirstChangeSeqNum() {
        return changeList.isEmpty() ? -1 : changeList.getFirst().getSeqNum();
    }

    /**
     * Obtain a list of all changes that have a seqNum of startSeqNum or higher.
     * Returns null if a change with seqNum == startSeqNum is not present in
     * our change list.
     *
     * @param startSeqNum starting sequence number
     * @return a list containing all qualified changes, or null if a change
     * with seqNum == startSeqNum is not present.
     */
    public List<SecurityMDChange> getChanges(final int startSeqNum) {
        /*
         * Return null if there's no change currently, or the starting seqNum
         * is out of the boundary of stored changes.
         */
        if (changeList.isEmpty()) {
            return null;
        }

        if (startSeqNum < getFirstChangeSeqNum() ||
            startSeqNum > changeList.getLast().getSeqNum()) {
            return null;
        }

        final List<SecurityMDChange> retChanges = new LinkedList<>();
        for (final SecurityMDChange change : changeList) {
            if (change.getSeqNum() >= startSeqNum) {
                retChanges.add(change.clone());
            }
        }
        return retChanges;
    }

    /**
     * Get all changes.
     */
    public List<SecurityMDChange> getChanges() {
        return getChanges((changeList.isEmpty()) ?
                0 : changeList.getFirst().seqNum);
    }

    /**
     * Get latest change.
     */
    public SecurityMDChange getLatestChange() {
        return changeList.getLast();
    }

    /**
     * Create and return a deep copy of this SecurityMetadata object.
     *
     * @return the new SecurityMetadata instance
     */
    @Override
    public SecurityMetadata getCopy() {
        return (SecurityMetadata)Metadata.super.getCopy();
    }

    @Override
    public String toString() {
        return String.format("SecurityMetadata id=%s seq# %d", id,
                             sequenceNumber);
    }

    /**
     * Bump the sequence number, and log the change into the list.
     */
    public void logChange(final SecurityMDChange change) {
        sequenceNumber++;
        change.setSeqNum(sequenceNumber);
        changeList.add(change);
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();

        /*
         * Initialize user-defined role map if read the security metadata
         * object created in previous version.
         */
        if (kvstoreRoleMap == null) {
            kvstoreRoleMap = new KVStoreRoleMap(this);
        }

        /*
         * Initialize Kerberos service principal map if read the security
         * metadata object created in previous version.
         */
        if (krbInstanceMap == null) {
            krbInstanceMap = new KerberosInstanceMap(this);
        }
    }

    public static Map<String, RoleDescription> getBuiltInRoleInfo() {
        return Collections.unmodifiableMap(builtInRoleInfoMap);
    }

    /*
     * For testing
     */
    public void setRoleMapId(int id) {
        this.kvstoreRoleMap.setId(id);
    }

    /**
     * Map for storing elements of KVStoreUser.
     */
    public static class KVStoreUserMap extends ElementMap<KVStoreUser> {

        private static final long serialVersionUID = 1L;

        public KVStoreUserMap(final SecurityMetadata securityMD) {
            super(securityMD);
        }

        KVStoreUserMap(DataInput in,
                       short serialVersion,
                       SecurityMetadata securityMD)
            throws IOException
        {
            super(in, serialVersion, securityMD);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            super.writeFastExternal(out, serialVersion);
        }

        @Override
        void writeElement(KVStoreUser element,
                          DataOutput out,
                          short serialVersion)
            throws IOException
        {
            element.writeUser(out, serialVersion);
        }

        @Override
        KVStoreUser readElement(DataInput in, short serialVersion)
            throws IOException
        {
            return KVStoreUser.readUser(in, serialVersion);
        }

        @Override
        String nextId() {
            /* A user id is a digital number with prefix of "u". */
            return "u" + (++idSequence);
        }

        @Override
        SecurityElementType getElementType() {
            return SecurityElementType.KVSTOREUSER;
        }
    }

    /**
     * Map for storing elements of RoleInstance.
     */
    public static class KVStoreRoleMap extends ElementMap<RoleInstance> {

        private static final long serialVersionUID = 1L;

        private int currentId;

        public KVStoreRoleMap(final SecurityMetadata securityMD) {
            super(securityMD);
        }

        KVStoreRoleMap(DataInput in,
                       short serialVersion,
                       SecurityMetadata securityMD)
            throws IOException
        {
            super(in, serialVersion, securityMD);
            currentId = in.readInt();
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            super.writeFastExternal(out, serialVersion);
            out.writeInt(currentId);
        }

        @Override
        RoleInstance readElement(DataInput in, short serialVersion)
            throws IOException
        {
            return new RoleInstance(in, serialVersion);
        }

        @Override
        String nextId() {
            /* A role id is a digital number with prefix of "r". */
            return "r" + getNextId();
        }

        /*
         * For testing
         */
        void setId(int id) {
            idSequence = id;
        }

        private int getNextId() {
            if (idSequence < Integer.MAX_VALUE) {
                currentId = idSequence++;
                return idSequence;
            }

            for (currentId = 1; currentId < idSequence; currentId++) {
                if (this.get("r" + currentId) == null) {
                    return currentId;
                }
            }
            throw new IllegalStateException("Could not add role, " +
                "the number of roles exceeds the limit");
        }

        @Override
        SecurityElementType getElementType() {
            return SecurityElementType.KVSTOREROLE;
        }
    }

    public static class KerberosInstanceMap
        extends ElementMap<KerberosInstance> {

        private static final long serialVersionUID = 1L;

        public KerberosInstanceMap(final SecurityMetadata securityMD) {
            super(securityMD);
        }

        KerberosInstanceMap(DataInput in,
                            short serialVersion,
                            SecurityMetadata securityMD)
            throws IOException
        {
            super(in, serialVersion, securityMD);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            super.writeFastExternal(out, serialVersion);
        }

        @Override
        KerberosInstance readElement(DataInput in, short serialVersion)
            throws IOException
        {
            return new KerberosInstance(in, serialVersion);
        }

        /**
         * Add principal instance name for given storage node id.
         *
         * @param instanceName principal instance name
         * @param snId storage node id
         * @return KerberosInstance if adding succeed or null if given storage
         *         node id is invalid
         * @throws IllegalStateException if an instance with the same name
         *         already exist in the Kerberos principal map.
         */
        public KerberosInstance addInstanceName(final String instanceName,
                                                final StorageNodeId snId)
            throws IllegalStateException {

            if (snId.getStorageNodeId() == 0) {
                return null;
            }

            for (KerberosInstance instance : getAll()) {
                if (instance.getStorageNodeId().equals(snId)) {
                    return null;
                }
            }
            return add(new KerberosInstance(instanceName, snId));
        }

        /**
         * Remove storage node principal instance name from security metadata.
         *
         * @param snId
         * @return KerberosInstance of this storage node if removing succeed or
         *         null if it does not exists.
         */
        public KerberosInstance removeInstanceName(final StorageNodeId snId) {
            for (KerberosInstance instance : getAll()) {
                if (instance.getStorageNodeId().equals(snId)) {
                    return remove(instance.getElementId());
                }
            }
            return null;
        }

        @Override
        SecurityElementType getElementType() {
            return SecurityElementType.KRBPRINCIPAL;
        }

        @Override
        String nextId() {
            /* A Kerberos instance id is a digital number with prefix of "k". */
            return "k" + (++idSequence);
        }
    }

    /**
     * Abstract class for composed element of security metadata.
     */
    public abstract static class SecurityElement
        /*
         * A Java 8 compiler bug (in JDK 1.8.0_281 at least) seems to require a
         * fully qualified class name for FastExternalizable here
         */
        implements oracle.kv.impl.util.FastExternalizable,
                   Serializable, Cloneable {

        private static final long serialVersionUID = 1L;

        /*
         * The unique element id. It's null if the element has not been
         * "added" to the ElementMap.
         */
        private String elementId;

        /**
         * Ctor with un-initialized elementId. The initialization of the
         * elementId will be deferred to, for example, when the element is
         * added in the storage container.
         */
        protected SecurityElement() {
        }

        protected SecurityElement(final SecurityElement other) {
            this.elementId = other.elementId;
        }

        protected SecurityElement(DataInput in, short serialVersion)
            throws IOException
        {
            elementId = readString(in, serialVersion);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeString(out, serialVersion, elementId);
        }

        public void setElementId(final String eId) {
            this.elementId = eId;
        }

        public String getElementId() {
            return elementId;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SecurityElement)) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            final SecurityElement other = (SecurityElement) obj;
            return (elementId == other.elementId ||
                    (elementId != null && elementId.equals(other.elementId)));
        }

        /**
         * Derived classes are required to calculate the hash code using their
         * own properties.
         */
        @Override
        public abstract int hashCode();

        @Override
        public abstract SecurityElement clone();

        public abstract SecurityElementType getElementType();

        public static SecurityElement readSecurityElement(DataInput in,
                                                          short sv)
            throws IOException
        {
            return SecurityElementType.readSecurityElement(in, sv);
        }

        public void writeSecurityElement(DataOutput out, short serialVersion)
            throws IOException
        {
            getElementType().writeSecurityElement(out, serialVersion, this);
        }
    }

    /**
     * Class holding a collection of SecurityElement. Use Map as the collection
     * to build a mapping between the element Ids and the elements.
     * <p>
     * Note that any operation modifying the elements may encounter unexpected
     * exceptions, and thus lead to corrupted data. The caller should handle
     * such cases.
     */
    protected abstract static class ElementMap<T extends SecurityElement>
        implements FastExternalizable, Serializable {

        private static final long serialVersionUID = 1L;

        private final SecurityMetadata securityMD;

        private final HashMap<String, T> elementMap;

        int idSequence;

        protected ElementMap(final SecurityMetadata securityMD) {
            this.securityMD = securityMD;
            elementMap = new HashMap<>();
        }

        protected ElementMap(DataInput in,
                             short serialVersion,
                             SecurityMetadata securityMD)
            throws IOException
        {
            this.securityMD = securityMD;
            elementMap = readMap(in, serialVersion, HashMap::new,
                                 SerializationUtil::readString,
                                 this::readElement);
            idSequence = readPackedInt(in);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            writeMap(out, serialVersion, elementMap,
                     WriteFastExternal::writeString, this::writeElement);
            writePackedInt(out, idSequence);
        }

        /** Writes a value of the element map. */
        void writeElement(T element, DataOutput out, short serialVersion)
            throws IOException
        {
            element.writeFastExternal(out, serialVersion);
        }

        /** Reads a value of the element map. */
        abstract T readElement(DataInput in, short serialVersion)
            throws IOException;

        T get(final String elementId) {
            return elementMap.get(elementId);
        }

        Collection<T> getAll() {
            return elementMap.values();
        }

        /**
         * Add a new element into the ElementMap. A new internal ElementID will
         * be generated and assigned to the new element.
         *
         * @param element the new element
         * @return the new element
         * @throws IllegalStateException if an existing element is found with
         * the same ID
         */
        T add(final T element) {
            return add(nextId(), element);
        }

        /**
         * Add a new element with its ElementID into the ElementMap. If the add
         * operation fails, i.e., an existing element with the same ID is found,
         * an IllegalStateException will be thrown. The caller should catch and
         * handle the exception.
         *
         * @param elementId ID of the element
         * @return the new element
         * @throws IllegalStateException if an existing element is found with
         * the same ID
         */
        T add(final String elementId, final T element) {
            element.setElementId(elementId);
            final T prev = elementMap.put(elementId, element);
            securityMD.logChange(new SecurityMDChange.Add(element));
            if (prev != null) {
                throw new IllegalStateException("Element " + prev +
                                                " was been overwritten by " +
                                                element);
            }
            return element;
        }

        T update(final String elementId, final T element) {
            if (elementMap.get(elementId) == null) {
                throw new IllegalArgumentException(
                    "Element " + elementId + " absent from security metadata.");
            }
            element.setElementId(elementId);
            final T prev = elementMap.put(elementId, element);
            assert prev != null;
            securityMD.logChange(new SecurityMDChange.Update(element));
            return element;
        }

        T remove(final String elementId) {
            final T prev = elementMap.remove(elementId);
            if (prev == null) {
                throw new IllegalArgumentException("Element " + elementId +
                                                   "was not found.");
            }
            securityMD.logChange(
                new SecurityMDChange.Remove(elementId, getElementType(), prev));
            return prev;
        }

        /**
         * Apply a change to the current ElementMap object
         *
         * @param change the change of element
         */
        @SuppressWarnings("unchecked")
        void apply(final SecurityMDChange change) {
            final SecurityMDChangeType changeType = change.getChangeType();
            final String changeElementId = change.getElementId();

            switch (changeType) {
            case REMOVE:
                remove(changeElementId);
                break;

            case ADD:
                final String newId = nextId();
                if (!newId.equals(changeElementId)) {
                    throw new IllegalStateException(
                        "Element sequence out of sync; expected: " +
                         newId + " replayId: " + changeElementId);
                }
                add(newId, (T) change.getElement());
                break;

            case UPDATE:
                update(changeElementId, (T) change.getElement());
                break;

            default:
                throw new IllegalStateException(
                    "Unknown change type: " + changeType);
            }

            /*
             * SecurityMetadata should have the same sequence with the change
             * after applying it.
             */
            if (securityMD.getSequenceNumber() != change.getSeqNum()) {
                throw new IllegalStateException(
                    "Mismatched security metadata change sequence: log# " +
                    change.getSeqNum() + ", replay# " +
                    securityMD.getSequenceNumber());
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            final int result = 17 * prime + elementMap.hashCode();
            return result * prime + idSequence;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ElementMap<?> other = (ElementMap<?>) obj;
            return (idSequence == other.idSequence) &&
                   elementMap.equals(other.elementMap);
        }

        @Override
        public String toString() {
            return super.toString() +
                "[idSequence=" + idSequence +
                " elementMap=" + elementMap + "]";
        }

        abstract String nextId();
        abstract SecurityElementType getElementType();
    }

    /**
     * Kerberos principal instance name of storage node.
     */
    public static class KerberosInstance extends SecurityElement {

        private static final long serialVersionUID = 1L;

        private final String instanceName;

        private final StorageNodeId snId;

        public KerberosInstance(String instanceName, StorageNodeId snId) {
            this.instanceName = instanceName;
            this.snId = snId;
        }

        protected KerberosInstance(KerberosInstance other) {
            super(other);
            this.snId = other.snId;
            this.instanceName = other.instanceName;
        }

        KerberosInstance(DataInput in, short serialVersion)
            throws IOException
        {
            super(in, serialVersion);
            instanceName = readString(in, serialVersion);
            snId = (StorageNodeId)
                ResourceId.readFastExternal(in, serialVersion);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            super.writeFastExternal(out, serialVersion);
            writeString(out, serialVersion, instanceName);
            snId.writeFastExternal(out, serialVersion);
        }

        public StorageNodeId getStorageNodeId() {
            return this.snId;
        }

        public String getInstanceName() {
            return this.instanceName;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            if (!super.equals(obj)) {
                return false;
            }
            final KerberosInstance other = (KerberosInstance) obj;
            return snId.equals(other.snId);
        }

        @Override
        public int hashCode() {
            return snId.hashCode();
        }

        @Override
        public SecurityElement clone() {
            return new KerberosInstance(this);
        }

        @Override
        public SecurityElementType getElementType() {
            return SecurityElementType.KRBPRINCIPAL;
        }
    }
}
