/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.SecurityStore;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.KVStoreUser.UserType;
import oracle.kv.impl.security.metadata.PasswordHashDigest;
import oracle.kv.impl.security.metadata.SecurityMDChange;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.metadata.SecurityMetadata.KerberosInstance;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElement;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;

import org.junit.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class SecurityMetadataTest extends TestBase {

    private static final String SEC_ID = "md-" + System.currentTimeMillis();
    private static final String STORE_NAME = "foo";
    private static final File TEST_DIR = TestUtils.getTestDir();

    private static final SecureRandom random = new SecureRandom();

    private final SecurityMetadata md =
        new SecurityMetadata(STORE_NAME /* kvstorename */, SEC_ID /* id */);

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testBasic() {
        /* Test id, name and type */
        assertEquals(SEC_ID, md.getId());
        assertEquals(STORE_NAME, md.getKVStoreName());
        assertEquals(MetadataType.SECURITY, md.getType());

        /* Still empty now */
        assertEquals(SecurityMetadata.EMPTY_SEQUENCE_NUMBER,
                     md.getSequenceNumber());
        int expectedSeqNum = 3;

        for (SecurityElementType type : SecurityElementType.values()) {
            if (type.equals(SecurityElementType.KRBPRINCIPAL)) {
                continue;
            }

            final SecurityElement e1 = addSecurityElement(type, "first");
            final SecurityElement e2 = addSecurityElement(type, "second");

            assertEquals(e1, getSecurityElement(type, "first"));
            assertEquals(e2, getSecurityElement(type, "second"));

            final Collection<? extends SecurityElement> elements =
                getAllElements(type);
            assertEquals(2, elements.size() /* should have two elements now */);
            assertTrue(elements.contains(e1));
            assertTrue(elements.contains(e2));

            /* Add nobody */
            assertNull(getSecurityElement(type, "nobody"));

            /* Test element id related operations */
            assertEquals(getSecurityElementId(type, 2), e2.getElementId());
            assertEquals(e1, (getSecurityElementById(type, 1)));

            /* Security elements with same names should have different ids. */
            final SecurityElement repeatedE1 = addSecurityElement(type, "first");
            assertFalse(e1.getElementId().equals(repeatedE1.getElementId()));

            /* The sequence should have increased to 3 by now. */
            assertEquals(expectedSeqNum, md.getSequenceNumber());
            expectedSeqNum += 3;
        }
    }

    @Test
    public void testRemove() {
        int initSeqNum = md.getSequenceNumber();

        for (SecurityElementType type : SecurityElementType.values()) {
            if (type.equals(SecurityElementType.KRBPRINCIPAL)) {
                continue;
            }
            final int initElementNum = getAllElements(type).size();
            final SecurityElement shortLived =
                addSecurityElement(type, "shortLived");
            assertEquals(shortLived, getSecurityElement(type, "shortLived"));

            removeSecurityElement(type, shortLived.getElementId());
            assertNull(getSecurityElement(type, "shortLived"));
            assertNull(getSecurityElementById(type, 3));
            assertFalse(getAllElements(type).contains(shortLived));
            assertEquals(initElementNum, getAllElements(type).size());

            /* Test removing a non-existing element */
            try {
                removeSecurityElement(type, "fooId");
                fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException iae) {
                assertTrue(true); /* ignore */
            }

            /* The sequence should have increased by 2 now. */
            assertEquals(initSeqNum += 2, md.getSequenceNumber());
        }
    }

    @Test
    public void testUpdate() {
        int initSeqNum = md.getSequenceNumber();

        for (SecurityElementType type : SecurityElementType.values()) {
            if (type.equals(SecurityElementType.KRBPRINCIPAL)) {
                continue;
            }

            /* Test updating an existing element */
            final SecurityElement oldElement = addSecurityElement(type, "old");
            final SecurityElement newElement =
                updateSecurityElement(type, oldElement.getElementId(), "new");
            assertEquals(newElement, getSecurityElement(type, "new"));
            assertNull(getSecurityElement(type, "old"));

            /* Test updating a non-existing element */
            try {
                updateSecurityElement(type, "fooId", null);
                fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException iae) {
                assertTrue(true); /* ignore */
            }

            /* The sequence should have increased by 2 now. */
            assertEquals(initSeqNum += 2, md.getSequenceNumber());
        }
    }

    @Test
    public void testChangeApply() {

        for (SecurityElementType type : SecurityElementType.values()) {
            if (type.equals(SecurityElementType.KRBPRINCIPAL)) {
                continue;
            }

            /* Load some change data */
            final SecurityElement toUpdate = addSecurityElement(type, "update");
            final SecurityElement toRemove = addSecurityElement(type, "remove");
            addSecurityElement(type, "remove");
            updateSecurityElement(type, toUpdate.getElementId(), "newUpdate");
            removeSecurityElement(type, toRemove.getElementId());

            final SecurityMetadata newMd =
                new SecurityMetadata(md.getKVStoreName(), md.getId());

            /* Test applying null or empty changes */
            assertFalse(newMd.apply(null));
            assertFalse(newMd.apply(new ArrayList<SecurityMDChange>()));

            /* Test applying non-continuous changes */
            final List<SecurityMDChange> gappedChanges = md.getChanges(3);

            try {
                newMd.apply(gappedChanges);
                fail("Expected IllegalStateException");
            } catch (IllegalStateException ise) {
                assertTrue(true); /* ignore */
            }

            /* Test applying non-overlapped and continuous changes */
            final List<SecurityMDChange> appliableChanges = md.getChanges();
            assertTrue(newMd.apply(appliableChanges));
            assertSecurityMDEquals(newMd, md);

            /* Test applying overlapped changes */
            addSecurityElement(type, "oneMore");
            /* overlapped SN: 3-5 */
            final List<SecurityMDChange> overlappedChanges = md.getChanges(3);
            assertTrue(newMd.apply(overlappedChanges));
            assertSecurityMDEquals(newMd, md);
        }
    }

    @Test
    public void testBasicLogChanges() {

        /* Empty tracker should return -1 as its first sequence number */
        assertEquals(-1, md.getFirstChangeSeqNum());
        int initialSeqNum = Metadata.EMPTY_SEQUENCE_NUMBER;
        int totalChangesNum = 0;

        for (SecurityElementType type : SecurityElementType.values()) {
            if (type.equals(SecurityElementType.KRBPRINCIPAL)) {
                continue;
            }

            /* Initial sequence number should be EMPTY_SEQUENCE_NUMBER */
            assertEquals(initialSeqNum, md.getSequenceNumber());

            /* No changes is saved */
            if (initialSeqNum == Metadata.EMPTY_SEQUENCE_NUMBER) {
                assertNull(md.getChanges(1));
            } else {
                assertNotNull(md.getChanges(1));
            }

            /* Test sequencing logged changes (3 changes will be logged)*/
            md.logChange(new SecurityMDChange.Add(
                newSecurityElement(type, "first")));
            md.logChange(new SecurityMDChange.Remove("fooId", type,
                         newSecurityElement(type, "foo")));
            md.logChange(new SecurityMDChange.Update(
                newSecurityElement(type, "second")));
            totalChangesNum += 3;

            final int firstSeqNum = md.getFirstChangeSeqNum();
            assertEquals(1, firstSeqNum);
            assertEquals(initialSeqNum + 3, md.getSequenceNumber());
            assertEquals(totalChangesNum, md.getChanges().size());

            /* Test getting partial changes */
            assertEquals(2, md.getChanges(initialSeqNum + 2).size());
            assertNull(md.getChanges(firstSeqNum + totalChangesNum + 1));
            initialSeqNum += 3;
        }
    }

    @Test
    public void testDiscardChanges() {
        final int expectedSN = 2;
        final int expectedSize = 5;
        final int gap = 10;

        testBasicLogChanges();

        /* Test discarding changes in legal range */
        md.pruneChanges(2, 0);
        final int firstSeqNum = md.getFirstChangeSeqNum();
        final int newSize = md.getChanges().size();
        assertEquals(expectedSN, firstSeqNum);
        assertEquals(expectedSize, newSize);

        /* Test discarding changes with start SeqNum before first SeqNum */
        md.pruneChanges(firstSeqNum - 1, Integer.MAX_VALUE);
        assertEquals(expectedSN, firstSeqNum);
        assertEquals(expectedSize, newSize);

        /* Test discarding changes with start SeqNum before first SeqNum */
        md.pruneChanges(md.getSequenceNumber() + gap, Integer.MAX_VALUE);
        assertEquals(expectedSN, firstSeqNum);
        assertEquals(expectedSize, newSize);
    }

    @Test
    public void testSerialization() throws Exception {
        testBasic();

        /* Serialize the md */
        final byte[] secMdByteArray = SerializationUtil.getBytes(md);

        /* De-serialize the md */
        final SecurityMetadata restoredMd =
            SerializationUtil.getObject(secMdByteArray, md.getClass());
        assertSecurityMDEquals(restoredMd, md);

        /* Same check with FastExternalizable serialization */
        assertSecurityMDEquals(
            md, TestUtils.fastSerialize(md, SecurityMetadata::new));
    }

    @Test
    public void testPersistence() {
        testBasic();

        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        final Environment env = new Environment(TEST_DIR, envConfig);
        final TransactionConfig tconfig = new TransactionConfig();
        final SecurityStore store = SecurityStore.getTestStore(env);

        /* Persist the data */
        Transaction txn = env.beginTransaction(null, tconfig);
        store.putSecurityMetadata(txn, md, false);
        txn.commit();

        /* Retrieve the data */
        txn = env.beginTransaction(null, tconfig);
        final SecurityMetadata restoredMd = store.getSecurityMetadata(txn);
        txn.commit();
        assertSecurityMDEquals(restoredMd, md);

        /* Test update and reload the persisted copy */
        for (SecurityElementType type : SecurityElementType.values()) {
            if (type.equals(SecurityElementType.KRBPRINCIPAL)) {
                md.addKerberosInstanceName("instance", new StorageNodeId(1));
            } else {
                addSecurityElement(type, "newOne");
            }

            txn = env.beginTransaction(null, tconfig);
            store.putSecurityMetadata(txn, md, false);
            txn.commit();

            txn = env.beginTransaction(null, tconfig);
            final SecurityMetadata newReloadMd = store.getSecurityMetadata(txn);
            txn.commit();
            assertSecurityMDEquals(newReloadMd, md);
        }
        store.close();
        env.close();
    }

    @Test
    public void testPasswordVerification() {
        final String userName = "test";
        final KVStoreUser user = KVStoreUser.newInstance(userName);
        user.setEnabled(true);
        final char[] rightPass = "NoSql00__rightPass".toCharArray();
        final char[] wrongPass = "wrongPass".toCharArray();
        final char[] newPass = "NoSql00__newPass".toCharArray();

        md.addUser(user);

        user.setPassword(makeDefaultHashDigest(rightPass));
        user.setPasswordLifetime(TimeUnit.DAYS.toMillis(1));
        assertFalse(md.verifyUserPassword(userName, wrongPass));
        assertTrue(md.verifyUserPassword(userName, rightPass));

        /* Test retained password */
        user.retainPassword();
        user.getRetainedPassword().setLifetime(
            TimeUnit.SECONDS.toMillis(1));
        user.setPassword(makeDefaultHashDigest(newPass));

        /* Both current and retained password work */
        assertTrue(md.verifyUserPassword(userName, rightPass));
        assertTrue(md.verifyUserPassword(userName, newPass));
        assertTrue(user.retainedPasswordValid());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            /* Ignore */
            assertTrue(true); /* ignore */
        }

        /* Time expired, current password works, retained password is gone */
        assertTrue(md.verifyUserPassword(userName, newPass));
        assertFalse(md.verifyUserPassword(userName, rightPass));
        assertFalse(user.retainedPasswordValid());

        /* Disabled user cannot login */
        user.setEnabled(false);
        assertFalse(md.verifyUserPassword(userName, newPass));
    }

    @Test
    public void testPreviousPasswords() {
        final String userName = "PrevPassUser";
        KVStoreUser user = KVStoreUser.newInstance(userName);
        user.setEnabled(true);
        final char[] firstPass = "NoSql00__Pass1".toCharArray();
        final char[] secondPass = "NoSql00__Pass2".toCharArray();
        final char[] thirdPass = "NoSql00__Pass3".toCharArray();
        final PasswordHashDigest firstPassPHD =
            makeDefaultHashDigest(firstPass);
        final PasswordHashDigest secondPassPHD =
            makeDefaultHashDigest(secondPass);
        final PasswordHashDigest thirdPassPHD =
            makeDefaultHashDigest(thirdPass);
        md.addUser(user);
        user.setPassword(firstPassPHD);
        user.setPassword(secondPassPHD);
        user.setPassword(thirdPassPHD);
        assertTrue(md.verifyUserPassword(userName, thirdPass));
        SecurityMetadata secMd = persistAndRetrieve(md);
        user = secMd.getUser(userName);
        PasswordHashDigest[] previousPasswords = user.getRememberedPasswords(5);
        assertEquals(previousPasswords.length, 3);
        assertEquals(previousPasswords[0], thirdPassPHD);
        assertEquals(previousPasswords[1], secondPassPHD);
        assertEquals(previousPasswords[2], firstPassPHD);
        previousPasswords = user.getRememberedPasswords(2);
        assertEquals(previousPasswords.length, 2);
        assertEquals(previousPasswords[0], thirdPassPHD);
        assertEquals(previousPasswords[1], secondPassPHD);
    }

    @Test
    public void testGrantRevokeRoles() {
        final KVStoreUser user = KVStoreUser.newInstance("user1");
        final Set<String> defaultRoles = buildRoleSet(RoleInstance.PUBLIC_NAME);
        final Set<String> adminDefaultRoles =
            buildRoleSet(RoleInstance.SYSADMIN_NAME, RoleInstance.PUBLIC_NAME);
        final Set<String> grantedRoles = buildRoleSet(RoleInstance.READWRITE_NAME,
                                                      RoleInstance.SYSADMIN_NAME,
                                                      RoleInstance.PUBLIC_NAME);

        /* Verify default roles of user and Admin user */
        assertEquals(defaultRoles, user.getGrantedRoles());
        user.setAdmin(true);
        assertEquals(adminDefaultRoles, user.getGrantedRoles());

        /* Grant READWRITE role to user and verify */
        Set<String> roles = buildRoleSet(RoleInstance.READWRITE_NAME);
        user.grantRoles(roles);
        assertEquals(grantedRoles, user.getGrantedRoles());

        /* Revoke READWRITE role */
        user.revokeRoles(roles);
        assertEquals(adminDefaultRoles, user.getGrantedRoles());

        /* Revoke SYSADMIN role */
        roles.remove(RoleInstance.READWRITE_NAME);
        roles.add(RoleInstance.SYSADMIN_NAME);
        user.revokeRoles(roles);
        assertEquals(defaultRoles, user.getGrantedRoles());
    }

    @Test
    public void testExternalUserType() {
        final String externalUserName = "external/machine@external.com";
        KVStoreUser user = KVStoreUser.newV1Instance(externalUserName);
        user.setUserType(UserType.EXTERNAL);
        md.addUser(user);
        assertUserEquals(user, md.getUser(externalUserName));
        assertEquals(
            md.getUser(externalUserName).getUserType(), UserType.EXTERNAL);
        final Collection<KVStoreUser> users = md.getAllUsers();
        assertEquals(1, users.size());
        assertEquals(1, md.getSequenceNumber());
    }

    @Test
    public void testUserDescription() {
        final char[] pwd = "NoSql00__Pass1".toCharArray();
        final PasswordHashDigest pwdHash = makeDefaultHashDigest(pwd);
        final char[] newPwd = "NoSql00__newPass".toCharArray();
        final PasswordHashDigest newPwdHash = makeDefaultHashDigest(newPwd);
        final KVStoreUser user = KVStoreUser.newInstance("user")
            .setEnabled(true).setPassword(pwdHash);
        JsonNode desc = JsonUtils.parseJsonNode(
            user.getDescription().detailsAsJSON());
        assertEquals(user.getName(), desc.get("name").asText());
        assertEquals(user.isEnabled(), desc.get("enabled").asBoolean());
        assertEquals(user.getUserType().toString(), desc.get("type").asText());
        assertEquals("inactive", desc.get("retain-passwd").asText());

        /* Test retain password expiration output */
        user.retainPassword();
        user.setPassword(newPwdHash);
        desc = JsonUtils.parseJsonNode(user.getDescription().detailsAsJSON());
        String expiryInfo = user.getRetainedPassword().getExpirationInfo();
        assertEquals(
            "active [expiration: " + expiryInfo +"]",
            desc.get("retain-passwd").asText());

        /* Test password expiration output */
        expiryInfo = user.getPassword().getExpirationInfo();
        desc = JsonUtils.parseJsonNode(user.getDescription().detailsAsJSON());
        assertEquals(expiryInfo, desc.get("current-passwd-expiration").asText());

        long lifetime = 360_000_000;
        newPwdHash.setLifetime(lifetime);
        user.setPassword(newPwdHash);
        desc = JsonUtils.parseJsonNode(user.getDescription().detailsAsJSON());
        final String expiry = desc.get("current-passwd-expiration").asText();
        assertEquals(newPwdHash.getExpirationInfo(), expiry);
        final String format = "yyyy-MM-dd HH:mm:ss z";
        final SimpleDateFormat df = new SimpleDateFormat(format);
        try {
            df.parse(expiry);
        } catch (ParseException e) {
            fail("Expiration not in correct format, expect in format " + format +
                 ", actual value " + expiry);
        }

        newPwdHash.setLifetime(-1);
        user.setPassword(newPwdHash);
        desc = JsonUtils.parseJsonNode(user.getDescription().detailsAsJSON());
        assertEquals(newPwdHash.getExpirationInfo(),
                     desc.get("current-passwd-expiration").asText());

        /* Test external user description output */
        final KVStoreUser extUser = KVStoreUser.newInstance("user")
            .setEnabled(true).setUserType(UserType.EXTERNAL);
        desc = JsonUtils.parseJsonNode(extUser.getDescription().detailsAsJSON());
        assertEquals(extUser.getName(), desc.get("name").asText());
        assertEquals(extUser.isEnabled(), desc.get("enabled").asBoolean());
        assertEquals(extUser.getUserType().toString(), desc.get("type").asText());
        assertNull(desc.get("retain-passwd"));
        assertNull(desc.get("current-passwd-expiration"));
    }

    @Test
    public void testMDUpgrade() {

        /* Test id, name and type */
        assertEquals(SEC_ID, md.getId());
        assertEquals(STORE_NAME, md.getKVStoreName());
        assertEquals(MetadataType.SECURITY, md.getType());

        /* Still empty now */
        assertEquals(SecurityMetadata.EMPTY_SEQUENCE_NUMBER,
                     md.getSequenceNumber());

        final String userName = "user";
        final String adminUserName = "admin";
        KVStoreUser user = KVStoreUser.newV1Instance(userName);
        KVStoreUser admin =
            KVStoreUser.newV1Instance(adminUserName).setAdmin(true);
        md.addUser(user);
        md.addUser(admin);
        assertUserEquals(user, md.getUser(userName));
        assertUserEquals(admin, md.getUser(adminUserName));
        final Collection<KVStoreUser> users = md.getAllUsers();
        assertEquals(2, users.size());
        assertEquals(2, md.getSequenceNumber());

        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        final Environment env = new Environment(TEST_DIR, envConfig);
        final TransactionConfig tconfig = new TransactionConfig();
        final SecurityStore store = SecurityStore.getTestStore(env);

        /* Persist the data */
        Transaction txn = env.beginTransaction(null, tconfig);
        store.putSecurityMetadata(txn, md, false);
        txn.commit();

        /* Retrieve the data */
        txn = env.beginTransaction(null, tconfig);
        final SecurityMetadata md1 = store.getSecurityMetadata(txn);
        txn.commit();
        assertSecurityMDEquals(md1, md);
        assertUserEquals(md.getUser(userName), md1.getUser(userName));
        assertUserEquals(md.getUser(adminUserName), md1.getUser(adminUserName));

        /* Check default roles of user */
        assertEquals(user.getGrantedRoles(),
                     buildRoleSet(RoleInstance.PUBLIC_NAME,
                                  RoleInstance.READWRITE_NAME));

        /* Check default roles of admin user */
        assertEquals(admin.getGrantedRoles(),
                     buildRoleSet(RoleInstance.PUBLIC_NAME,
                                  RoleInstance.READWRITE_NAME,
                                  RoleInstance.SYSADMIN_NAME));

        /* Grant DBAADMIN role to user */
        Set<String> dba = buildRoleSet(RoleInstance.DBADMIN_NAME);
        user = user.grantRoles(dba);

        /* Grant READONLY role to admin user */
        admin = admin.grantRoles(
            Collections.singleton(RoleInstance.READONLY_NAME));
        md1.updateUser(user.getElementId(), user);
        md1.updateUser(admin.getElementId(), admin);
        txn = env.beginTransaction(null, tconfig);
        store.putSecurityMetadata(txn, md1, false);
        txn.commit();

        txn = env.beginTransaction(null, tconfig);
        final SecurityMetadata md2 = store.getSecurityMetadata(txn);
        txn.commit();
        assertSecurityMDEquals(md1, md2);
        assertUserEquals(user, md2.getUser(userName));
        assertUserEquals(admin, md2.getUser(adminUserName));

        /* Revoke DBAADMIN role from user */
        user.revokeRoles(dba);

        /* Revoke READONLY role from admin user */
        admin.revokeRoles(Collections.singleton(RoleInstance.READONLY_NAME));
        md2.updateUser(user.getElementId(), user);
        md2.updateUser(admin.getElementId(), admin);
        txn = env.beginTransaction(null, tconfig);
        store.putSecurityMetadata(txn, md2, false);
        txn.commit();

        txn = env.beginTransaction(null, tconfig);
        final SecurityMetadata md3 = store.getSecurityMetadata(txn);
        txn.commit();
        assertSecurityMDEquals(md2, md3);
        assertUserEquals(user, md3.getUser(userName));
        assertUserEquals(admin, md3.getUser(adminUserName));
        store.close();
        env.close();
    }

    @Test
    public void testUserDefinedRole() {
        final RoleInstance manager = new RoleInstance("manager");
        final RoleInstance employee = new RoleInstance("employee");

        final Set<String> managerRoles = buildRoleSet(
            RoleInstance.READWRITE_NAME, RoleInstance.SYSADMIN_NAME);
        final Set<String> employeeRoles = buildRoleSet(
            RoleInstance.READONLY_NAME, RoleInstance.SYSADMIN_NAME);

        manager.grantRoles(managerRoles);
        employee.grantRoles(employeeRoles);

        assertEquals(managerRoles, manager.getGrantedRoles());
        assertEquals(employeeRoles, employee.getGrantedRoles());

        manager.grantRoles(Collections.singleton("employee"));
        managerRoles.add("employee");
        assertEquals(managerRoles, manager.getGrantedRoles());
    }

    @Test
    public void testRoleIdLimit() {
        int currentId = Integer.MAX_VALUE - 20;
        md.setRoleMapId(currentId);

        for (int i = 1; i <= 20; i++) {
            currentId++;
            md.addRole(new RoleInstance("old" + i));
            assertEquals(md.getRole("old" + i).getElementId(), "r" + currentId);
        }
        assertEquals(currentId, Integer.MAX_VALUE);
        currentId = 1;

        for (int i = 0; i <= 20; i++) {
            md.addRole(new RoleInstance("new" + i));
            assertEquals(md.getRole("new" + i).getElementId(), "r" + currentId);
            currentId++;
        }
    }

    @Test
    public void testKerberosPrincipal() {
        /* Test id, name and type */
        assertEquals(SEC_ID, md.getId());
        assertEquals(STORE_NAME, md.getKVStoreName());
        assertEquals(MetadataType.SECURITY, md.getType());

        /* Still empty now */
        assertEquals(SecurityMetadata.EMPTY_SEQUENCE_NUMBER,
                     md.getSequenceNumber());
        int expectedSeqNum = 2;

        final StorageNodeId sn1 = new StorageNodeId(1);
        final StorageNodeId sn2 = new StorageNodeId(2);
        final KerberosInstance i1 = md.addKerberosInstanceName("first", sn1);
        final KerberosInstance i2 = md.addKerberosInstanceName("second", sn2);

        assertEquals(i1, md.getKrbInstance(sn1));
        assertEquals(i2, md.getKrbInstance(sn2));

        final Collection<KerberosInstance> ins = md.getAllKrbInstanceNames();
        assertEquals(2, ins.size());
        assertTrue(ins.contains(i1));
        assertTrue(ins.contains(i2));

        /* Test element id related operations */
        assertEquals(md.getKrbInstance(sn2).getElementId(), i2.getElementId());
        assertEquals(i1, (md.getKrbInstanceById("k" + 1)));

        /* Test adding principal for the same sn is not allowed */
        assertEquals(expectedSeqNum, md.getSequenceNumber());
        assertNull(md.addKerberosInstanceName("third", sn1));
        assertEquals(expectedSeqNum, md.getSequenceNumber());

        /* Test adding principal for invalid sn id is not allowed */
        assertNull(md.addKerberosInstanceName("error", new StorageNodeId(0)));

        /* Test new metadata apply Kerberos principal changes */

        /* Create new metadata */
        SecurityMetadata newMd = new SecurityMetadata(md.getKVStoreName(),
                                                      md.getId());
        final StorageNodeId sn3 = new StorageNodeId(3);
        final StorageNodeId sn4 = new StorageNodeId(4);
        md.addKerberosInstanceName("thrid", sn3);
        md.addUser(KVStoreUser.newInstance("user1"));
        md.removeKrbInstanceName(sn1);

        /* Test applying null or empty changes */
        assertFalse(newMd.apply(null));
        assertFalse(newMd.apply(new ArrayList<SecurityMDChange>()));

        /* Test applying non-continuous changes */
        final List<SecurityMDChange> gappedChanges = md.getChanges(3);

        try {
            newMd.apply(gappedChanges);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ise) {
            assertTrue(true); /* ignore */
        }

        /* Test applying non-overlapped and continuous changes */
        final List<SecurityMDChange> appliableChanges = md.getChanges();
        assertTrue(newMd.apply(appliableChanges));
        assertSecurityMDEquals(newMd, md);

        /* Test applying overlapped changes */
        md.addKerberosInstanceName("fourth", sn4);

        /* overlapped SN: 3-5 */
        final List<SecurityMDChange> overlappedChanges = md.getChanges(3);
        assertTrue(newMd.apply(overlappedChanges));
        assertSecurityMDEquals(newMd, md);

        /* Test new metadata log Kerberos principal changes */

        /* Initial sequence number should be EMPTY_SEQUENCE_NUMBER */
        newMd = new SecurityMetadata(md.getKVStoreName(), md.getId());
        assertEquals(0, newMd.getSequenceNumber());
        assertNull(newMd.getChanges(1));

        /* Test sequencing logged changes (3 changes will be logged)*/
        newMd.logChange(
            new SecurityMDChange.Add(new KerberosInstance("1", sn1)));
        newMd.logChange(
            new SecurityMDChange.Add(new KerberosInstance("2", sn2)));
        newMd.logChange(new SecurityMDChange.Remove("fooId",
            SecurityElementType.KRBPRINCIPAL, new KerberosInstance("3", sn3)));

        final int firstSeqNum = newMd.getFirstChangeSeqNum();
        assertEquals(1, firstSeqNum);
        assertEquals(3, newMd.getSequenceNumber());
        assertEquals(3, newMd.getChanges().size());

        /* Test getting partial changes */
        assertEquals(2, newMd.getChanges(2).size());
        assertNull(newMd.getChanges(4));
    }

    /**
     * Assert two security metadata copies are equal by comparing their ids,
     * StoreName, sequenceNumbers and the internal security elements.
     */
    public static void assertSecurityMDEquals(final SecurityMetadata expected,
                                              final SecurityMetadata actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getSequenceNumber(), actual.getSequenceNumber());
        assertEquals(expected.getKVStoreName(), actual.getKVStoreName());
        assertEquals(expected.getKVStoreUserMap(), actual.getKVStoreUserMap());
    }

    private static PasswordHashDigest
        makeDefaultHashDigest(final char[] plainPassword) {
        final byte[] saltValue =
                PasswordHash.generateSalt(random, PasswordHash.SUGG_SALT_BYTES);
        return PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                                PasswordHash.SUGG_HASH_ITERS,
                                                PasswordHash.SUGG_SALT_BYTES,
                                                saltValue, plainPassword);
    }

    private static void assertUserEquals(final KVStoreUser expected,
                                         final KVStoreUser actual) {
        assertEquals(expected, actual);
        assertEquals(expected.getGrantedRoles(), actual.getGrantedRoles());
    }

    private static Set<String> buildRoleSet(final String... element) {
        return new HashSet<>(Arrays.asList(element));
    }

    /**
     * Format security element Id by given digital number.
     *
     * @param type security element type, KVStoreUser and RoleInstance
     * @param id digital number of element id
     * @return formatted element in string
     */
    private String getSecurityElementId(SecurityElementType type, int id) {

        if (type == SecurityElementType.KVSTOREUSER) {
            return "u" + id;
        }
        return "r" + id;
    }

    /**
     * Get security element by given name with its element type as prefix.
     * - KVStoreUser, the actual name will be "user" + given string.
     * - RoleInstance, the actual name will be "role" + given string.
     *
     * @param type security element type, KVStoreUser and RoleInstance
     * @param name element name
     * @return security element
     */
    private SecurityElement getSecurityElement(SecurityElementType type,
                                               String name) {

        if (type == SecurityElementType.KVSTOREUSER) {
            return md.getUser("user" + name);
        }
        return md.getRole("role" + name);
    }

    /**
     * Add security element. The naming convention is as same as the
     * getSecurityElement method.
     *
     * @param type security element type, KVStoreUser and RoleInstance
     * @param name element name
     * @return added security element
     */
    private SecurityElement addSecurityElement(SecurityElementType type,
                                               String name) {

        if (type == SecurityElementType.KVSTOREUSER) {
            return
                md.addUser(KVStoreUser.newInstance("user" + name));
        }
        return md.addRole(new RoleInstance("role" + name));
    }

    /**
     * Update security element associated with the given Id.
     *
     * @param type security element type, KVStoreUser and RoleInstance
     * @param oldElementId update target element Id
     * @param name element new name
     * @return updated security element
     */
    private SecurityElement updateSecurityElement(SecurityElementType type,
                                                  String oldElementId,
                                                  String newName) {

        if (type == SecurityElementType.KVSTOREUSER) {
            return md.updateUser(oldElementId,
                KVStoreUser.newInstance("user" + newName));
        }
        return md.updateRole(oldElementId,
                             new RoleInstance("role" + newName));
    }

    /**
     * Remove security element associated with the given Id.
     *
     * @param type security element type, KVStoreUser and RoleInstance
     * @param elementId remove target element Id
     */
    private void removeSecurityElement(SecurityElementType type,
                                       String elementId) {

        if (type == SecurityElementType.KVSTOREUSER) {
            md.removeUser(elementId);
        } else {
            md.removeRole(elementId);
        }
    }

    /**
     * Get all element associated with given type.
     *
     * @param type security element type
     * @return all element with specific type
     */
    private Collection<? extends SecurityElement>
        getAllElements(SecurityElementType type) {

        if (type == SecurityElementType.KVSTOREUSER) {
            return md.getAllUsers();
        }
        return md.getAllRoles();
    }

    /**
     * Get security element by given digital Id.
     * - KVStoreUser, the actual name will be "u" + given digital Id.
     * - RoleInstance, the actual name will be "r" + given digital Id.
     *
     * @param type security element type
     * @param elementId element digital id
     * @return security element
     */
    private SecurityElement getSecurityElementById(SecurityElementType type,
                                                   int elementId) {

        if (type == SecurityElementType.KVSTOREUSER) {
            return md.getUserById("u" + elementId);
        }
        return md.getRoleById("r" + elementId);
    }

    /**
     * Build a new security element instance.
     *
     * @param type security element type
     * @param name security element name
     * @return the newly create security element
     */
    private SecurityElement newSecurityElement(SecurityElementType type,
                                               String name) {

        if (type == SecurityElementType.KVSTOREUSER) {
            return KVStoreUser.newInstance(name);
        }
        return new RoleInstance(name);
    }

    private SecurityMetadata persistAndRetrieve(SecurityMetadata metadata) {
        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        final Environment env = new Environment(TEST_DIR, envConfig);
        final TransactionConfig tconfig = new TransactionConfig();
        final SecurityStore store = SecurityStore.getTestStore(env);

        /* Persist the data */
        Transaction txn = env.beginTransaction(null, tconfig);
        store.putSecurityMetadata(txn, metadata, false);
        txn.commit();

        /* Retrieve the data */
        txn = env.beginTransaction(null, tconfig);
        final SecurityMetadata resultMd = store.getSecurityMetadata(txn);
        txn.commit();
        store.close();
        env.close();
        return resultMd;
    }
}
