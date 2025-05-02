/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.topo.util;

import static org.junit.Assert.assertEquals;

import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;

/**
 * Houses Topology related utilities for use during testing
 */
public class TopoUtils {

    /**
     *
     */
    public static final String TOPOHOST = "localhost";
    private static final String FREE_PORT_START = "freePortStart";
    static int freePortStart = 5000;

    static {
        final String fps = System.getProperty(FREE_PORT_START);
        if (fps != null) {
            freePortStart = Integer.parseInt(fps);
        }
    }

    public static FreePortLocator makeFreePortLocator() {
        return  new FreePortLocator(TOPOHOST, freePortStart,
                                    freePortStart + 1000);
    }

    /**
     * Creates a test topology structure, all configured to use the local host.
     */
    public static Topology create(String kvsName,
                                  int nDC,
                                  int nSN,
                                  int repFactor,
                                  int nPartitions) {
        return create(kvsName, nDC, nSN, repFactor, nPartitions,
                      makeFreePortLocator());
    }

    /**
     * Creates a test topology structure, all configured to use the local host,
     * and explicitly provide a free port locator. Use this flavor of create
     * when the caller needs a reference to the free port locator to use for
     * setting up test-made RepNodeParams.
     */
    public static Topology create(String kvsName,
                                  int nDC,
                                  int nSN,
                                  int repFactor,
                                  int nPartitions,
                                  FreePortLocator portLocator) {

        return create(kvsName, nDC, nSN, repFactor, nPartitions, 0,
                      portLocator);
    }

    /**
     * Creates a test topology structure, all configured to use the local host,
     * including specifying the number of secondary zones.  Explicitly provide
     * a free port locator. Use this flavor of create when the caller needs a
     * reference to the free port locator to use for setting up test-made
     * RepNodeParams.
     */
    public static Topology create(String kvsName,
                                  int nDC,
                                  int nSN,
                                  int repFactor,
                                  int nPartitions,
                                  int nSecondaryZones,
                                  FreePortLocator portLocator) {

        return create(kvsName, nDC, nSN, repFactor, nPartitions,
                      nSecondaryZones, 0, portLocator);
    }

    /**
     * Creates a test topology structure, all configured to use the local host,
     * including specifying the number of shards.
     *
     * Creates the same number of SNs in each zone, so nSN must be a multiple
     * of nDC.
     *
     * Note that the repFactor applies to a single zone, not the entire store.
     *
     * Creates all SNs with the same capacity, so the total number of RNs
     * (nDC*repFactor*numRGs) must be a multiple of nSN.
     *
     * If numRGs is 0, then it is set to the number of SNs per data center.
     */
    public static Topology create(String kvsName,
                                  int nDC,
                                  int nSN,
                                  int repFactor,
                                  int nPartitions,
                                  int nSecondaryZones,
                                  int numRGs,
                                  FreePortLocator portLocator) {

        if (numRGs == 0) {
            numRGs = nSN / nDC;
        }

        assertEquals("Leftover SNs" +
                     " nSN=" + nSN +
                     " nDC=" + nDC +
                     " snPerDC=" + (nSN / nDC),
                     0, nSN % nDC);
        assertEquals("Leftover SNs per zone" +
                     " nSN=" + nSN +
                     " nDC=" + nDC +
                     " snPerDC=" + (nSN / nDC) +
                     " repFactor=" + repFactor +
                     " numRGs=" + numRGs +
                     " rnPerDC=" + (repFactor * numRGs) +
                     " snCapacity=" + ((repFactor * numRGs) / (nSN /nDC)),
                     0, (repFactor * numRGs) % (nSN / nDC));

        final Topology topo = new Topology(kvsName);
        if (nDC < nSecondaryZones) {
            throw new IllegalArgumentException(
                "nDC was less than nSecondaryZones");
        }
        Datacenter[] dcs = new Datacenter[nDC];
        /* All the datacenters will be created with the same repFactor */
        for (int i=0; i < nDC; i++) {
            dcs[i] = Datacenter.newInstance("DC" + i, repFactor,
                                            i < (nDC - nSecondaryZones) ?
                                            DatacenterType.PRIMARY :
                                            DatacenterType.SECONDARY, false,
                                            false);
            topo.add(dcs[i]);
        }

        StorageNode[] sns = new StorageNode[nSN];
        int dcNum=0;
        for (int i=0; i < nSN; i++) {
            Datacenter dc = dcs[dcNum++ % nDC];
            sns[i] = new StorageNode(dc,
                                     TOPOHOST,
                                     portLocator.next());
            topo.add(sns[i]);
        }

        final int numRNs = numRGs * repFactor * nDC;

        RepGroup rgs[] = new RepGroup[numRGs];
        RepNode rns[] = new RepNode[numRNs];
        int rnNum = 0;
        int snNum = 0;
        for (int i=0; i < numRGs; i++) {
            rgs[i] = new RepGroup();
            topo.add(rgs[i]);
            for (int j=0; j < repFactor * nDC; j++) {
                rns[rnNum] = new RepNode(sns[snNum++ % nSN].getResourceId());
                rgs[i].add(rns[rnNum]);
            }
        }

        for (int i=0; i < nPartitions; i++) {
            int rgNum = (i % numRGs);
            Partition p = new Partition(rgs[rgNum]);
            topo.add(p);
        }

        return topo;
    }
}
