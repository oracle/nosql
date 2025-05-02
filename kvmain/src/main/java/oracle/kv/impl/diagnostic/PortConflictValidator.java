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

package oracle.kv.impl.diagnostic;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import oracle.kv.impl.param.ParameterUtils;

/**
 * Check whether assigned ports have conflict.
 *
 * A SNA usually needs more than one port. Bind Port check just check the port
 * is already in use or not, and it do not detect that a port assigned twice
 * before the port is really used. This class detects whether a port is
 * assigned more than once. As ports are submitted with the check() method,
 * PortConflictChecker accumulates a collection of all ports that have been
 * seen. The check() method compares a new port to that set of accumulated
 * ports.
 */
public class PortConflictValidator {
    
    /* Store all checked ports*/
    private final Set<PortInfo> gatheredPorts;
    
    public PortConflictValidator() {
        gatheredPorts = new HashSet<PortInfo>();
    }
    
    /**
     * Check whether ports with a range are available and are already 
     * assigned or not.
     * 
     * @param parameterName associated name of the range of ports
     * @param rangePort a range of ports
     * 
     * @return null when all ports are available and are not assigned; 
     * or return error message.
     */  
    public String check(String parameterName, String rangePort) {
        String retMsg;
        
        retMsg = checkPortConflict(parameterName, rangePort);
        if (retMsg != null) {
            return retMsg;
        }
        
        return null;
    }

    /**
     * Check whether a port is available and is already assigned or not.
     * 
     * @param parameterName associated name of port
     * @param port value of port
     * 
     * @return null when port is available and is not assigned; or return 
     * error message.
     */ 
    public String check(String parameterName, int port) {
        String retMsg;
        
        retMsg = checkPortConflict(parameterName, String.valueOf(port));
        if (retMsg != null) {
            return retMsg;
        }
        
        return null;
    }
    
    /**
     * Check whether a port or a range of port are available and are already 
     * assigned or not.
     * 
     * @param parameterName associated name of port
     * @param port value of port or a range of ports
     * 
     * @return null when port or a range of port are available and are not 
     * assigned; or return error message.
     */ 
    private String checkPortConflict(String parameterName, String port) {
        PortInfo currentPortInfo = new PortInfo(parameterName, port);
        
        PortConflictInfo portConflictInfo = 
                findAllPortsConflict(currentPortInfo, gatheredPorts);
        
        gatheredPorts.add(currentPortInfo);
        if (portConflictInfo !=null && portConflictInfo.getConflictPort() > 0) {
            return "Specified " + parameterName + " " + 
                   portConflictInfo.getConflictPort() + 
                   " is already assigned as " + portConflictInfo.getMessage();
        }
        return null;
    }
    
    /**
     * Compare whether two ports are equal or not.
     * 
     * @param port1 the first port
     * @param port2 the second port
     * 
     * @return 0 when the two ports are not equal; or return the value of the 
     * port.
     */ 
    private int findPortsConflict(int port1, int port2) {
        if (port1 == port2) {
            return port1;
        }
        return 0;
    }
    
    /**
     * Check whether a port is equal to any ports within a range of ports 
     * or not.
     * 
     * @param port port number
     * @param rangePorts a range of ports
     * 
     * @return 0 when the port is not equal to any ports within the range 
     * ports; or return the value of the port.
     */     
    private int findPortsConflict(int port, String rangePorts) {
        StringTokenizer tokenizer = new StringTokenizer(rangePorts, 
                ParameterUtils.HELPER_HOST_SEPARATOR);
        int firstHAPort = Integer.parseInt(tokenizer.nextToken());
        int secondHAPort = Integer.parseInt(tokenizer.nextToken());
        for (int i=firstHAPort; i<=secondHAPort; i++) {
            if (port == i) {
                return port;
            }
        }
        return 0;
    }
    
    /**
     * Check whether two ranges of ports have overlapped ports or not.
     * 
     * @param rangePorts1 the first range of ports
     * @param rangePorts2 the second range of ports
     * 
     * @return 0 when two ranges of ports have no overlapped ports; or return 
     * the value of the overlapped port
     */     
    private int findRangePortsConflict(String rangePorts1, 
                                              String rangePorts2) {
        StringTokenizer tokenizer1 = new StringTokenizer(rangePorts1, 
                ParameterUtils.HELPER_HOST_SEPARATOR);
        int firstHAPort1 = Integer.parseInt(tokenizer1.nextToken());
        int secondHAPort1 = Integer.parseInt(tokenizer1.nextToken());
        
        StringTokenizer tokenizer2 = new StringTokenizer(rangePorts2, ",");
        int firstHAPort2 = Integer.parseInt(tokenizer2.nextToken());
        int secondHAPort2 = Integer.parseInt(tokenizer2.nextToken());
        
        for (int i=firstHAPort1; i<=secondHAPort1; i++) {
            for (int j=firstHAPort2; j<=secondHAPort2; j++) {
                if (i==j)
                    return i;
            }
        }
        return 0;
    }
    
    /**
     * Check whether two ports or two ranges of ports have overlapped 
     * ports or not
     * 
     * @param port1 the first port or the first range of ports
     * @param port2 the second port or the second range of ports
     * 
     * @return 0 when they have no overlapped ports; or return 
     * the value of the overlapped port
     */ 
    private int findPortsConflict(String port1, String port2) {
        boolean isRangePort1 = false;
        boolean isRangePort2 = false;
        
        /* 
         * port1 is a range of ports when it contains the comma sign; 
         * Or it is a port number 
         */
        if (port1.indexOf(ParameterUtils.HELPER_HOST_SEPARATOR) >= 0) {
            isRangePort1 = true;
        }
        
        /* 
         * port2 is a range of ports when it contains the comma sign; 
         * Or it is a port number 
         */
        if (port2.indexOf(ParameterUtils.HELPER_HOST_SEPARATOR) >= 0) {
            isRangePort2 = true;
        }
        
        int conflictPort = 0;
        
        /* Then compare the two ports or two ranges of ports */
        if (!isRangePort1 && !isRangePort2) {
            conflictPort = findPortsConflict(Integer.parseInt(port1), 
                    Integer.parseInt(port2));
        }
        else if (!isRangePort1 && isRangePort2) {
            conflictPort = findPortsConflict(Integer.parseInt(port1), port2);            
        }
        else if (isRangePort1 && !isRangePort2) {
            conflictPort = findPortsConflict(Integer.parseInt(port2), port1);        
        }
        else {
            conflictPort = findRangePortsConflict(port1, port2); 
        }
        
        return conflictPort;
    }
    
    /**
     * Check whether ports in PortInfo and ports in a ArrayList of PortInfo 
     * have overlapped ports or not.
     * 
     * @param currPortInfo the PortInfo
     * @param portSet the Set of PortInfo
     * 
     * @return null when they have no overlapped ports; or return a 
     * PortConflictInfo contains the overlapped port and error message.
     */     
    private PortConflictInfo findAllPortsConflict(PortInfo currPortInfo, 
            Set<PortInfo> portSet) {
        int conflictPort;
        PortConflictInfo portConflictInfo = null;
        if (portSet == null || portSet.size() < 1) {
            return portConflictInfo;
        }
        
        for (PortInfo comPortInfo:portSet) {
            conflictPort = findPortsConflict(currPortInfo.getPort(), 
                    comPortInfo.getPort());
            if (conflictPort > 0) {
                portConflictInfo = new PortConflictInfo(comPortInfo.getName(), 
                        conflictPort);
                return portConflictInfo;
            }
        }
        
        return portConflictInfo;
    }
    
    /**
     * A bean class to store a port number or a range of ports 
     * and its associated name
     */
    public class PortInfo {
        private String name;
        private String port;


        public PortInfo(String name, String port) {
            this.name = name;
            this.port = port;
        }
        
        public String getName() {
            return name;
        }

        public String getPort() {
            return port;
        }
    }
    
    /**
     * A bean class to store the error message and conflict port number when 
     * find conflict among ports.
     */
    public static class PortConflictInfo {
        private String message;
        private int conflictPort;
        
        public PortConflictInfo(String message, int conflictPort) {
            this.message = message;
            this.conflictPort = conflictPort;
        }
        
        public String getMessage() {
            return message;
        }
        
        public int getConflictPort() {
            return conflictPort;
        }
    }
}
