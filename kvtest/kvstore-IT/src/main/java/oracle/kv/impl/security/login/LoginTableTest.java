/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static oracle.kv.impl.util.TestUtils.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.security.auth.Subject;

import oracle.kv.TestBase;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.metadata.KVStoreUser;

import org.junit.Test;

/**
 * Test the LoginTable API.
 */
public class LoginTableTest extends TestBase {

    private static final int N_ID_BYTES = 16;

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    @Test
    public void testBasic() {

        final Subject aSubject = new Subject();
        final LoginTable lt = new LoginTable(10, new byte[0], N_ID_BYTES);
        final String host = "localhost";
        final LoginSession ls = lt.createSession(aSubject, host, 0L);
        assertNotNull(ls);

        final LoginSession lookupLs = lt.lookupSession(ls.getId());
        assertEquals(ls, lookupLs);

        final long newExpire = 123456789L;
        final LoginSession updateLs =
            lt.updateSessionExpiration(ls.getId(), newExpire);
        assertNotNull(updateLs);
        assertEquals(newExpire, updateLs.getExpireTime());

        lt.logoutSession(ls.getId());
        final LoginSession logoutLs = lt.lookupSession(ls.getId());
        assertNull(logoutLs);
    }

    @Test
    public void testLRU() {
        final Subject aSubject = new Subject();
        final String host = "localhost";

        /* Test multiple table sizes */
        for (final int tabSize : new int[] { 3, 230, 777 }) {
            final LoginTable lt =
                new LoginTable(tabSize, new byte[0], N_ID_BYTES);

            final LinkedList<LoginSession> sessions = fillLRUTable(lt, tabSize);

            /* We'll create some more */
            final int addCount = (tabSize / 3);

            for (int i = 0; i < addCount; i++) {
                sessions.add(lt.createSession(aSubject, host, 0L));
            }

            assertEquals(tabSize, lt.size());

            /*
             * verify that the first addCount are missing and the rest are
             * present
             */
            for (int i = 0; i < sessions.size(); i++) {
                final LoginSession s = sessions.get(i);
                final LoginSession ls = lt.lookupSession(s.getId());
                if (i < addCount) {
                    assertNull(ls);
                } else {
                    assertNotNull(ls);
                    assertSame(s, ls);
                }
            }
        }
    }

    @Test
    public void testResizeLRU() {

        /* Test multiple table sizes */
        for (final int tabSize : new int[] { 3, 230, 777 }) {
            final LoginTable lt =
                new LoginTable(tabSize, new byte[0], N_ID_BYTES);

            final LinkedList<LoginSession> sessions = fillLRUTable(lt, tabSize);

            /* Resize downward to test LRU discard */
            final int newSize = tabSize / 2;
            final int dropCount = tabSize - newSize;
            assertTrue(dropCount > 0);
            lt.updateSessionLimit(newSize);

            /*
             * verify that the first dropCount are missing and the
             * rest are present.
             */
            for (int i = 0; i < sessions.size(); i++) {
                final LoginSession s = sessions.get(i);
                final LoginSession ls = lt.lookupSession(s.getId());
                if (i < dropCount) {
                    assertNull(ls);
                } else {
                    assertNotNull(ls);
                    assertSame(s, ls);
                }
            }
        }
    }

    @Test
    public void testResize() {
        final Subject aSubject = new Subject();
        final LoginTable lt = new LoginTable(10, new byte[0], N_ID_BYTES);
        final String host = "localhost";
        int count = 0;

        /* Verify the size is bound to 10 at first */
        for (count = 0; count <= 20; count++) {
            lt.createSession(aSubject, host, 0L);
        }
        assertEquals(10, lt.size());

        /*
         * Increase size to 50, verify the size is bounded by 50 after 70 more
         * insertions
         */
        lt.updateSessionLimit(50);
        for (count = 0; count <= 70; count++) {
            lt.createSession(aSubject, host, 0L);
        }
        assertEquals(50, lt.size());

        /*
         * Decrease size to 20, verify the size is bounded by 20 after 30 more
         * insertions
         */
        lt.updateSessionLimit(20);
        for (count = 0; count <= 30; count++) {
            lt.createSession(aSubject, host, 0L);
        }
        assertEquals(20, lt.size());
    }

    @Test
    public void testResizeWithConcurrentLogin() {
        final int oldSize = 50;
        final int newSize = 20;
        final Subject aSubject = new Subject();
        final LoginTable lt = new LoginTable(oldSize, new byte[0], N_ID_BYTES);
        final String host = "localhost";
        final CountDownLatch startFlag = new CountDownLatch(1);
        final Set<Thread> threads = new HashSet<Thread>();

        /* Populate the table with 70 entries */
        for (int count = 0; count <= 70; count++) {
            lt.createSession(aSubject, host, 0L);
        }
        assertEquals(oldSize, lt.size());

        /*
         * Resize to smaller capacity, and then verify the size does decrease
         * with concurrent login action
         */
        lt.updateSessionLimit(newSize);
        for (int i = 0; i < 100; i++) {
            final Thread loginTask = new Thread() {

                @Override
                public void run() {
                    try {
                        startFlag.await();
                        for (int j = 0; j < 100; j++) {
                            lt.createSession(aSubject, host, 0L);
                            sleep(10);
                        }
                    } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                }
            };
            loginTask.start();
            threads.add(loginTask);
        }

        startFlag.countDown();
        for (final Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        assertTrue(lt.size() <= newSize);
    }

    @Test
    public void testSessionUpdateBasic() {
        final LoginTable lt = new LoginTable(10, new byte[0], N_ID_BYTES);
        final String host = "localhost";

        final String[] bUserRoles = new String[] { "writeonly", "sysadmin"};
        final Subject subj1 = makeSubject("auser", "1", "readonly");
        final Subject subj2 = makeSubject("buser", "2", bUserRoles);

        for (int count = 0; count <5; count++) {
            lt.createSession(subj1, host, 0L);
            lt.createSession(subj2, host, 0L);
        }
        assertEquals(10, lt.size());

        final KVStoreUser user1 = KVStoreUser.newInstance("auser");
        user1.grantRoles(Arrays.asList(new String[] {"readonly", "sysadmin"}));
        List<LoginSession.Id> aUserIds = lt.lookupSessionByUser("auser");
        assertEquals(5, aUserIds.size());
        for (LoginSession.Id id : aUserIds) {
            lt.updateSessionSubject(id, user1.makeKVSubject());
        }

        /* Verify update auser role succeed */
        final Set<String> updatedRoles = set("sysadmin", "public", "readonly");
        for (LoginSession.Id id : aUserIds) {
            final LoginSession sess = lt.lookupSession(id);
            final String[] roles =
                ExecutionContext.getSubjectRoles(sess.getSubject());
            assertEquals(updatedRoles, set(roles));
        }

        /* Verify roles of buser are not changed */
        for (LoginSession.Id id : lt.lookupSessionByUser("buser")) {
            final LoginSession sess = lt.lookupSession(id);
            final String[] roles =
                ExecutionContext.getSubjectRoles(sess.getSubject());
            assertEquals(set(roles), set(bUserRoles));
        }
    }

    @Test
    public void testUpdateSessionWithConcurrentLogout() {
        final Subject subj1 = makeSubject("user", "1", "readonly");
        final LoginTable lt = new LoginTable(50, new byte[0], N_ID_BYTES);
        final String host = "localhost";
        final CountDownLatch startFlag = new CountDownLatch(1);
        final Set<Thread> threads = new HashSet<Thread>();

        /* Populate the table with 70 entries */
        for (int count = 0; count < 50; count++) {
            lt.createSession(subj1, host, 0L).getId();
        }
        assertEquals(50, lt.size());

        final KVStoreUser user = KVStoreUser.newInstance("user");
        user.grantRoles(Arrays.asList(
            new String[] {"sysadmin", "public", "readonly"}));
        final List<LoginSession.Id> ids = lt.lookupSessionByUser("user");
        try {
            final Thread updateTask = new Thread() {

                @Override
                public void run() {
                    try {
                        startFlag.await();
                        for (LoginSession.Id id : ids) {
                            lt.updateSessionSubject(id, user.makeKVSubject());
                        }
                    } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                }
            };
            updateTask.start();
            threads.add(updateTask);

            for (int i = 0; i < 50; i++) {
                final LoginSession.Id logoutId = ids.get(i);
                final Thread logoutTask = new Thread() {

                    @Override
                    public void run() {
                        try {
                            startFlag.await();
                            lt.logoutSession(logoutId);
                        } catch (InterruptedException e) {
                            fail("unexpected exception");
                        }
                    }
                };
                logoutTask.start();
                threads.add(logoutTask);
            }
            startFlag.countDown();
            for (final Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
            assertEquals(0, lt.size());
        } catch (Exception e) {
            fail("Unexpected exception");
        }
    }

    private Subject makeSubject(String userName,
                                String userId,
                                String... roles) {
        final Set<Principal> userPrincipals = new HashSet<Principal>();
        userPrincipals.add(new KVStoreUserPrincipal(userName, userId));

        for (String role : roles) {
            userPrincipals.add(KVStoreRolePrincipal.get(role));
        }
        final Set<Object> publicCreds = new HashSet<Object>();
        final Set<Object> privateCreds = new HashSet<Object>();
        return new Subject(true /* readOnly */,
                           userPrincipals, publicCreds, privateCreds);
    }

    /**
     * Given a new LoginTable, fill it with entries and then access them in
     * an order different than insertion order, and return a list of the
     * entries in order of least-recently access to most-recently accessed.
     */
    private LinkedList<LoginSession> fillLRUTable(LoginTable lt,
                                                  int tabSize) {
        final Subject aSubject = new Subject();
        final String host = "localhost";

        final LinkedList<LoginSession> sessions =
            new LinkedList<LoginSession>();

        /* fill the table */
        for (int i = 0; i < tabSize; i++) {
            sessions.add(lt.createSession(aSubject, host, 0L));
        }

        final LinkedList<LoginSession> updatedSessions =
            new LinkedList<LoginSession>();

        /*
         * verify that they are all there, but in alternating order
         * of ascending/descending
         */
        for (int i = 0; i < tabSize; i++) {
            final int idx =
                ((i % 2) == 0) ? (i / 2) : (tabSize - 1 - (i / 2));
            final LoginSession s = sessions.get(idx);
            final LoginSession ls = lt.lookupSession(s.getId());
            assertNotNull(ls);
            assertSame(s, ls);
            updatedSessions.add(s);
        }

        return updatedSessions;
    }
}
