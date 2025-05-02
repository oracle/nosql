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

package oracle.kv.impl.diagnostic.ssh;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.diagnostic.SNAInfo;
import oracle.kv.impl.util.ThreadUtils.ThreadPoolExecutorAutoClose;

/**
 * A manager to manage all SSH clients.
 */
public class SSHClientManager {
    private static String AT_SIGN = "@";

    /* Store host@username and SSHClient pairs */
    private static Map<String, SSHClient> clientCache =
        new ConcurrentHashMap<>();

    /**
     * Get clients according to the info of SNAs
     *
     * @param snaList the info of SNAs
     * @return Map of SNAInfo and SSHClient pairs
     * @throws Exception
     */
    public static Map<SNAInfo, SSHClient> getClient(List<SNAInfo> snaList)
        throws Exception {

        Map<SNAInfo, SSHClient> clientMap =
                new ConcurrentHashMap<SNAInfo, SSHClient>();
        int numberSSHThread = snaList.size();
        try (final ThreadPoolExecutorAutoClose threadExecutor =
             new ThreadPoolExecutorAutoClose(
                 numberSSHThread, numberSSHThread, 0L,
                 TimeUnit.MILLISECONDS,
                 new LinkedBlockingQueue<Runnable>())) {

            Map<SNAInfo, Future<SSHClient>> futurnMap =
                    new HashMap<SNAInfo, Future<SSHClient>>();

            /* Get clients in parallel to accelerate the performance */
            for (final SNAInfo snaInfo : snaList) {
                 Callable<SSHClient> snaClientCallable =
                         new Callable<SSHClient>() {

                    @Override
                    public SSHClient call() {
                        SSHClient client = clientCache.
                                get(getCacheKey(snaInfo.getHost(),
                                                snaInfo.getSSHUser()));
                        if (client == null) {
                            /*
                             * Instantiate a new SSHClient when do not find a
                             * existing client in cache
                             */
                            client = new SSHClient(snaInfo.getHost(),
                                                 snaInfo.getSSHUser());

                            /* Open client by authenticated file at first */
                            client.openByAuthenticatedFile();
                        }
                        return client;
                    }
                };
                futurnMap.put(snaInfo,
                              threadExecutor.submit(snaClientCallable));
            }

            /*
             * Check the open status of all clients to validate whether client
             * is open or not
             */
            for (Map.Entry<SNAInfo, Future<SSHClient>> entry :
                    futurnMap.entrySet()) {
                SNAInfo snaInfo = entry.getKey();
                SSHClient client = entry.getValue().get();

                if (client == null) {
                    continue;
                }

                /*
                 * Try to open client by password when the status of client is
                 * not open
                 */
                if (!client.isOpen()) {
                    SSHClient existingClient = clientCache.
                            get(getCacheKey(snaInfo.getHost(),
                                            snaInfo.getSSHUser()));

                    if (existingClient == null) {
                        client.openByPassword();
                    } else {
                        client = existingClient;
                    }
                }
                /*
                 * Put the open client into cache, and it can be reuse in the
                 * next time
                 */
                if (client.isOpen()) {
                    clientCache.put(getCacheKey(snaInfo.getHost(),
                                                snaInfo.getSSHUser()), client);
                }

                clientMap.put(snaInfo, client);
            }
        }

        return clientMap;
    }

    /**
     * Clear all cached clients
     */
    public static void clearClients() {
        for (Entry<String, SSHClient> entry : clientCache.entrySet()) {
            if (entry.getValue() != null) {
                entry.getValue().close();
            }
        }
        clientCache.clear();
    }

    /* Used for unit test */
    static SSHClient getClient(SNAInfo snaInfo) {
        SSHClient client = clientCache.get(getCacheKey(snaInfo.getHost(),
                                                       snaInfo.getSSHUser()));
        if (client == null) {
            /*
             * Instantiate a new SSHClient when do not find a existing client
             * in cache
             */
            client = new SSHClient(snaInfo.getHost(), snaInfo.getSSHUser());
            client.open();
            if (client.isOpen()) {
                clientCache.put(getCacheKey(snaInfo.getHost(),
                                            snaInfo.getSSHUser()), client);
            }
        }
        return client;
    }

    /* Get key to find SSH Client */
    private static String getCacheKey(String host, String username) {
        return username.toLowerCase(Locale.US).trim() + AT_SIGN +
               host.toLowerCase(Locale.US).trim();
    }
}
