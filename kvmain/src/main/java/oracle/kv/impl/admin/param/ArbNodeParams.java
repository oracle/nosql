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

package oracle.kv.impl.admin.param;

import static oracle.kv.impl.param.ParameterState.AN_HEAP_MB_MIN;
import static oracle.kv.impl.param.ParameterState.AP_AN_ID;
import static oracle.kv.impl.param.ParameterState.ARBNODE_TYPE;

import java.util.EnumSet;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * A class implementing ArbNodeParams contains all the per-ArbNode operational
 * parameters.
 */
public class ArbNodeParams extends GroupNodeParams {

    private static final long serialVersionUID = 1L;

    private final ParameterMap map;

    public ArbNodeParams(ParameterMap map) {
        this.map = map;
    }

    public ArbNodeParams(ArbNodeParams rnp) {
        this(rnp.getMap().copy());
    }

    /**
     * Create ArbNodeParams from existing map, which is probably a copy of the
     * policy map
     */
    public ArbNodeParams(ParameterMap newmap,
                         StorageNodeId snid,
                         ArbNodeId ArbNodeId,
                         boolean disabled,
                         String haHostname,
                         int haPort,
                         String helperHosts) {
        this.map = newmap.filter(EnumSet.of(ParameterState.Info.ARBNODE));
        setStorageNodeId(snid);
        setArbNodeId(ArbNodeId);
        setDisabled(disabled);
        setJENodeHostPort(HostPortPair.getString(haHostname, haPort));
        setJEHelperHosts(helperHosts);
        addDefaults();
        map.setName(ArbNodeId.getFullName());
        map.setType(ARBNODE_TYPE);
    }

    @Override
    public ParameterMap getMap() {
        return map;
    }

    private void addDefaults() {
        map.addServicePolicyDefaults(ParameterState.Info.ARBNODE);
    }

    public ArbNodeId getArbNodeId() {
        return ArbNodeId.parse(map.get(AP_AN_ID).asString());
    }

    public void setArbNodeId(ArbNodeId rnid) {
        map.setParameter(AP_AN_ID, rnid.getFullName());
    }

    /**
     * Set the AN heap and cache as a function of memory available on the
     * SN, and SN capacity. If either heap or cache is not specified, use
     * the JVM and JE defaults.
     */
    public void setARBHeap(long heapSizeMB) {
        /*
         * If the heap val is null, remove any current -Xms and Xmx flags,
         * else replace them with the new value.
         */
        setJavaMiscParams(replaceOrRemoveJVMArg
                          (getJavaMiscParams(), XMS_FLAG,
                           heapSizeMB + "M"));
        setJavaMiscParams(replaceOrRemoveJVMArg
                          (getJavaMiscParams(), XMX_FLAG,
                           heapSizeMB + "M"));
    }

    /**
     * Validate the JVM heap parameters.
     *
     * @param inTargetJVM is true if the current JVM is the target for
     * validation.  This allows early detection of potential memory issues for
     * ArbNode startup.

     * TODO: look at actual physical memory on the machine to validate.
     */
    public void validateHeap(boolean inTargetJVM) {
        long maxHeapMB = getMaxHeapMB();
        if (inTargetJVM) {
            long actualMemory = JVMSystemUtils.getRuntimeMaxMemory();
            long specHeap = maxHeapMB >> 20;
            if (specHeap > actualMemory) {
                throw new IllegalArgumentException
                ("Specified maximum heap of " + maxHeapMB  +
                 "MB exceeds available JVM memory of " + actualMemory +
                 ", see JVM parameters:" + getJavaMiscParams());
            }
        }

        long maxHeapBytes = getMaxHeapBytes();
        if (maxHeapBytes > 0) {
            checkMinSizes(maxHeapBytes);
        }

        long minHeapMB = getMinHeapMB();
        if (minHeapMB > maxHeapMB) {
            throw new IllegalArgumentException
            ("Mininum heap of " + minHeapMB + " exceeds maximum heap of " +
             maxHeapMB + ", see JVM parameters:" + getJavaMiscParams());
        }
    }
    
    @Override
    public String getJvmOverride() {
        return getMap().getOrDefault(ParameterState.JVM_AN_OVERRIDE).asString();
    }
    
    private void checkMinSizes(long heapBytes) {
        if ((heapBytes >> 20) < AN_HEAP_MB_MIN) {
            String msg = "JVM heap must be at least " + AN_HEAP_MB_MIN +
                " MB, specified value is " + (heapBytes>>20) + " MB";
            throw new IllegalArgumentException(msg);
        }
    }
}
