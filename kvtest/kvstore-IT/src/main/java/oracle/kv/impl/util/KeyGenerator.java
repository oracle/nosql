/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util;

import java.util.Set;
import java.util.TreeSet;
import oracle.kv.Key;
import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.PartitionId;

/**
 * Generates keys that would map to partitions that live on a specific
 * RepNode
 */
public class KeyGenerator {
    private long keyVal = 0;
    private final RepNode rn;

    public KeyGenerator(RepNode rn) {
        this.rn = rn;
    }

    /**
     * Returns a set of keys that belong to partitions associated with the
     * RepNode. If the rn is null returns undiscriminated keys
     *
     * @param keyCount the number of keys to be created
     *
     * @return an array containing keyCount keys
     */
    public Key[] getKeys(int keyCount) {
        final Key keys[] = new Key[keyCount];
        for (int i = 0; i < keyCount; i++) {
            Key key = null;
            while (true) {
                /* Loop until we get a value that lives on this RepNode. */
                keyVal++;
                key = Key.createKey(Long.toString(keyVal));
                try {
                    if (rn == null) {
                        break;
                    }
                    rn.getPartitionDB(rn.getPartitionId(key.toByteArray()));
                    break;
                } catch (IncorrectRoutingException e) {
                    continue;
                }
            }
            keys[i] = key;
        }
        return keys;
    }

    /**
     * Returns a set of keys that belong to a specific partition associated with
     * the RepNode. The generator must have been created with a non-null rn.
     *
     * @param keyCount the number of keys to be created
     *
     * @return an array containing keyCount keys
     */
    public Key[] getKeys(PartitionId requiredPid, int keyCount) {
        if (rn == null) {
            throw new AssertionError("Generator created with a null RepNode");
        }
        try {
            rn.getPartitionDB(requiredPid);
        } catch (IncorrectRoutingException e) {
            throw new AssertionError("Partition " + requiredPid +
                                     " does not belong to RepNode " + rn);
        }

        final Key keys[] = new Key[keyCount];
        for (int i = 0; i < keyCount; i++) {
            Key key = null;
            while (true) {
                /* Loop until we get a value that lives on this RepNode. */
                keyVal++;
                key = Key.createKey(Long.toString(keyVal));

                if (rn == null) {
                    break;
                }
                if (rn.getPartitionId(key.toByteArray()).equals(requiredPid)) {
                    break;
                }
            }
            keys[i] = key;
        }
        return keys;
    }

    /**
     * Returns a sorted set of keys that belong to a specific partition
     * associated with the RepNode. The generator must have been created with
     * a non-null rn.
     *
     * @param keyCount the number of keys to be created
     *
     * @return an array containing keyCount keys
     */
    public Key[] getSortedKeys(PartitionId requiredPid, int keyCount) {
        if (rn == null) {
            throw new AssertionError("Generator created with a null RepNode");
        }
        try {
            rn.getPartitionDB(requiredPid);
        } catch (IncorrectRoutingException e) {
            throw new AssertionError("Partition " + requiredPid +
                                     " does not belong to RepNode " + rn);
        }

        final Set<Key> keys = new TreeSet<Key>();
        for (int i = 0; i < keyCount; i++) {
            Key key = null;
            while (true) {
                /* Loop until we get a value that lives on this RepNode. */
                keyVal++;
                key = Key.createKey(Long.toString(keyVal));

                if (rn == null) {
                    break;
                }
                if (rn.getPartitionId(key.toByteArray()).equals(requiredPid)) {
                    break;
                }
            }
            keys.add(key);
        }
        return keys.toArray(new Key[0]);
    }
}
