/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.test.TestStatus;
import oracle.nosql.proxy.ProxyTestBase;
import oracle.nosql.proxy.util.CreateStore;
import oracle.nosql.proxy.util.CreateStoreUtils;
import oracle.nosql.proxy.util.PortFinder;

/**
 * This utility is for testing query with elasticity when using non-Java
 * SDKs. Non-Java SDK tests can launch it as a separate process. It will
 * deploy a store according to parameters on the command line and wait for
 * commands on standard input. The commands currently supported are
 * expand, contract and exit/quit. Expand and contract commands can
 * take optional parameter indicating number of SNs to be added/removed.
 * Note that currently doing contraction requires doing expansion first
 * (done with the same number of SNs), so after the test the store ends
 * up in its initial state. This is the same as done in ElasticityTest.java,
 * but perhaps can be improved to do contraction independent of expansion.
 * This utility also uses its stderr to write status messages for non-Java
 * tests to read, mainly to know when expansion/contraction has finished.
 * Stdout is used for logging/debugging messages. The non-Java driver test
 * will look for status messages in stderr starting with certain prefix (see
 * STATUS_PREFIX), other than that stderr can still be normally used.
 * As with other tests, the test files will be stored in directory pointed to
 * "testsandbox" system property.
 * 
 * The store is started according to built-in test parameters:
 * store - localhost:13250, name: kvstore
 * proxy - localhost:8095, type: cloudsim
 * 
 * Command line options:
 * -subdir - subdirectory within the sandbox directory to store files for
 * given run. Defaults to none (use the sandbox itself).
 * -numsns - initial number of SNs in the store. Default is 3.
 * -capacity - SN capacity. Extra SNs for expansion/contraction will use the
 * same capacity. Default is 1.
 * -repFactor - Replication factor. Default is 1. To test the driver side,
 * number of replicas is not essential, but needed for testing proxy/kv side.
 * -numPartitions - Number of partitions in the store, defaults to 10.
 */
public class ElasticityTestSetup extends ProxyTestBase {
	
    private static final int START_PORT = 5000;
    private static final int HA_RANGE = 5;
	private static final int DEFAULT_EXTRA_SNS = 3;
	private static final String STATUS_PREFIX =
		"Elasticity Test Store Status: ";
	
	private final Logger logger = Logger.getLogger(getClass().getName());
	private File rootDir;
	private int numSNs = 3;
	private int capacity = 1;
	private int repFactor = 1;
	private int numPartitions = 10;
	private CreateStore createStore;
	private ArrayList<StorageNodeAgent> extraSNAs;
	private int topoSeqNum;
	
	static {
		verbose = Boolean.getBoolean(VERBOSE_PROP);
		TestStatus.setActive(true);
	}
	
    private static void verbose(String msg, Object... args) {
    	ProxyTestBase.verbose(String.format(msg, args));
    }
    
    private void startStore() throws Exception {
    	if (rootDir.exists()) {
    		clearDirectory(rootDir);
    	}
        
        int port = getKVPort();
        
        createStore = new CreateStore(rootDir.getAbsolutePath(),
        	getStoreName(), port, numSNs, repFactor, numPartitions, capacity,
        	512, false, null);
        
        rootDir.mkdirs();
        createStore.start();
        
        String endpoint = String.format("%s:%s", getHostName(), port);
        verbose("Started kvstore on %s", endpoint);

        proxy = startProxy();
        verbose("Started proxy on %s", getProxyEndpoint());
        System.err.println(STATUS_PREFIX + "started");
    }
    
    private void shutdownStore() throws Exception {
    	verbose("Shutting down...");
        if (proxy != null) {
            proxy.shutdown(3, TimeUnit.SECONDS);
            proxy = null;
            verbose("Shutdown proxy.");
        }

        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
            verbose("Shutdown kvstore.");
        }
    }
    
    private void runPlan(CommandServiceAPI cs, int planId) throws Exception {
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }
	
    private void expandStore(int addSNCnt) throws Exception {
        final String hostName = getHostName();
        final CommandServiceAPI cs = createStore.getAdmin();

    	verbose("Starting expansion...");

        final int portsPerFinder = 20;
        if (extraSNAs == null) {
        	extraSNAs = new ArrayList<StorageNodeAgent>();
        }
        final String poolName = CreateStore.STORAGE_NODE_POOL_NAME;
        
        for (int i = 0; i < addSNCnt; ++i) {
        	int snIdx = numSNs + extraSNAs.size();
            PortFinder pf = new PortFinder(
            	START_PORT + snIdx * portsPerFinder, HA_RANGE,
                getHostName());
            int port = pf.getRegistryPort();

            StorageNodeAgent sna = CreateStoreUtils.createUnregisteredSNA(
                rootDir.getAbsolutePath(),
                pf,
                capacity,
                String.format("config%s.xml", snIdx),
                false /* useThreads */,
                false /* createAdmin */,
                512 /* mb */,
                null /* extra params */);
            
            extraSNAs.add(sna);

            CreateStoreUtils.waitForAdmin(hostName, port, 20, logger);

            int planId = cs.createDeploySNPlan(
                String.format("deploy sn%s", snIdx + 1),
                new DatacenterId(1),
                hostName,
                port,
                "comment");

            runPlan(cs, planId);

            StorageNodeId snId = sna.getStorageNodeId();
            verbose("Deployed SN %d", snId.getStorageNodeId());

            cs.addStorageNodeToPool(poolName, snId);
            verbose("Added %s to %s", snId.toString(), poolName);
        }

        String expandTopoName = String.format("expand-%d", topoSeqNum++);
        cs.copyCurrentTopology(expandTopoName);
        cs.redistributeTopology(expandTopoName, poolName);
        verbose("Created expanded topology %s", expandTopoName);

        int planId = cs.createDeployTopologyPlan(
            "deploy expansion", expandTopoName, null);
        System.err.println(STATUS_PREFIX + "start expand");
        runPlan(cs, planId);
        verbose("Deployed topology %s", expandTopoName);
        
        createStore.setExpansionSnas(
        	extraSNAs.toArray(new StorageNodeAgent[0]));
        verbose("Expansion done.");
        System.err.println(STATUS_PREFIX + "expanded");
    }

    private void contractStore(int delSNCnt) throws Exception {
    	int extraSNCnt = extraSNAs != null ? extraSNAs.size() : 0;
    	if (delSNCnt <= 0 || delSNCnt > extraSNCnt) {
    		throw new IllegalArgumentException(String.format(
    			"Cannot contract by %d SNs, extra SN cnt: %d",
    			delSNCnt, extraSNCnt));
    	}
    	
    	verbose("Starting contraction...");
    	
        final CommandServiceAPI cs = createStore.getAdmin();        
        final String poolName = CreateStore.STORAGE_NODE_POOL_NAME;

        for (int i = 0; i < delSNCnt; ++i) {
        	StorageNodeId snId = new StorageNodeId(
        		numSNs + extraSNCnt - delSNCnt + i + 1);
            cs.removeStorageNodeFromPool(poolName, snId);
            verbose("Removed %s from %s", snId.toString(),
            	poolName);
        }

        String contractTopoName = String.format("contract-%d", topoSeqNum++);
        cs.copyCurrentTopology(contractTopoName);
        cs.contractTopology(contractTopoName, poolName);
        verbose("Created contracted topology %s", contractTopoName);

        int planId = cs.createDeployTopologyPlan(
            "deploy contraction", contractTopoName, null);
        System.err.println(STATUS_PREFIX + "start contract");
        runPlan(cs, planId);
        verbose("Deployed topology %s", contractTopoName);
        
        for (int i = 0; i < delSNCnt; ++i) {
        	int snIdx = numSNs + extraSNCnt - delSNCnt + i;
        	StorageNodeId snId = new StorageNodeId(snIdx + 1);
            planId = cs.createRemoveSNPlan(
                String.format("remove sn%s", snId.getStorageNodeId()), snId);
            runPlan(cs, planId);
            verbose("Removed %s", snId.toString());
            
            StorageNodeAgent sna = extraSNAs.get(snIdx - numSNs);
            verbose("Shutting down %s", sna.getStorageNodeId().toString());
            sna.shutdown(true, true, "contration");
            verbose("Shutdown %s", snId.toString());

            new File(rootDir, String.format("config%s.xml", snIdx)).delete();
            FileUtils.deleteDirectory(new File(rootDir, getStoreName()));
            verbose("Removed data for %s", snId.toString());
        }

        extraSNAs.subList(extraSNCnt - delSNCnt, extraSNCnt).clear();
        createStore.setExpansionSnas(
           	extraSNAs.toArray(new StorageNodeAgent[0]));
        verbose("Contraction done.");
        System.err.println(STATUS_PREFIX + "contracted");
    }
    
    public ElasticityTestSetup(String[] args) {
    	String subDir = null;
    	
    	for(int i = 0; i < args.length; i++) {
    		final String arg = args[i];
    		if (arg.equalsIgnoreCase("-subdir")) {
    			subDir = args[++i];
    		} else if (arg.equalsIgnoreCase("-numsns")) {
    			numSNs = Integer.parseInt(args[++i]);
    		} else if (arg.equalsIgnoreCase("-capacity")) {
    			capacity = Integer.parseInt(args[++i]);
    		} else if(arg.equalsIgnoreCase("-repFactor")) {
    			repFactor = Integer.parseInt(args[++i]);
    		} else if (arg.equalsIgnoreCase("-numPartitions")) {
    			numPartitions = Integer.parseInt(args[++i]);
    		}
    	}
    	
    	rootDir = subDir != null ?
    		new File(getTestDir(), subDir) : new File(getTestDir());
    }
    
    public void run() throws Exception {
    	startStore();
    	Scanner scanner = null;
    	try {
	    	scanner = new Scanner(System.in);
	    	while(scanner.hasNextLine()) {
	    		String line = scanner.nextLine();
	    		String[] words = line.split("\\s+");
	    		if (words.length == 0) {
	    			continue;
	    		}
	    		boolean done = false;
	    		switch(words[0].toLowerCase()) {
	    		case "expand":
	    			expandStore(words.length > 1 ?
	    				Integer.parseInt(words[1]) : DEFAULT_EXTRA_SNS);
	    			break;
	    		case "contract":
	    			contractStore(words.length > 1 ?
	    				Integer.parseInt(words[1]) : DEFAULT_EXTRA_SNS);
	    			break;
	    		case "exit": case "quit":
	    			done = true;
	    			break;
	    		}
	    		if (done) {
	    			break;
	    		}
	    	}
    	} finally {
    		if (scanner != null) {
    			scanner.close();
    		}
    	}
    	
    	shutdownStore();
    }
    
    public static void main(String[] args) throws Exception {
    	ElasticityTestSetup setup = new ElasticityTestSetup(args);
    	setup.run();
    }
    
}
