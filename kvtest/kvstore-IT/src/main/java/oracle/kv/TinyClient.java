/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv;

/**
 * A tiny client.
 */
public class TinyClient {

    private final KVStore store;

    public static void main(String args[]) {
        try {
            (new TinyClient(args)).run();
            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }
    }

    public TinyClient(String[] args) {
        final int port = Integer.parseInt(args[0]);
        System.out.println(String.format(
            "Connecting to port %s with security file %s",
            port,
            System.getProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY)));
        this.store = KVStoreFactory.getStore(
            new KVStoreConfig("kvstore", "localhost:" + port));
    }

    public void run() throws Exception {
        final String keyString = "Tiny";
        final String valueString = "Client";

        store.put(Key.createKey(keyString),
                  Value.createValue(valueString.getBytes()));

        final ValueVersion valueVersion = store.get(Key.createKey(keyString));

        System.out.println(keyString + " " +
                           new String(valueVersion.getValue().getValue()));

        store.close();
    }
}
