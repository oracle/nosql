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

import java.net.InetAddress;

/**
 * A bean class to store info of SNA, including user name, host name,
 * port number, directory of root and directory of jar files.
 */

public class SNAInfo {
    private String storeName;
    private String storageNodeName;
    private String host;
    private String sshUser;
    private String rootdir;
    private InetAddress IP;
    private String remoteRootdir;

    private String LOCALHOST_NAME = "localhost";
    private String LOCALHOST_IP = "127.0.0.1";

    public SNAInfo(String storeName, String storageNodeName, String host,
                   String sshUser, String rootdir) {
        this.storeName = storeName;
        this.storageNodeName = storageNodeName;
        this.host = host;
        this.sshUser = sshUser;
        this.rootdir = rootdir;
        convertToIP();
    }

    public SNAInfo(String storeName, String storageNodeName, String host,
                   String rootdir) {
        this.storeName = storeName;
        this.storageNodeName = storageNodeName;
        this.host = host;
        this.rootdir = rootdir;
        convertToIP();
    }

    public SNAInfo(String snaInfoString) throws Exception {
        parse(snaInfoString);
        convertToIP();
    }

    /**
     * Parse snaInfo string and get value of all parts within snaInfo string.
     * @throws Exception
     */
    private void parse(String snaInfoString) throws Exception {
        try {
            String[] snaInfoStrs = snaInfoString.split("\\|");
            storeName = snaInfoStrs[0];
            storageNodeName = snaInfoStrs[1];
            int index = snaInfoStrs[2].indexOf("@");
            if (index > -1) {
                sshUser = snaInfoStrs[2].substring(0, index);
                host = snaInfoStrs[2].substring(index+1);
            } else {
                host =  snaInfoStrs[2];
            }
            rootdir = snaInfoStrs[3];
        } catch (Exception ex) {
            throw new Exception("Problem parsing " + snaInfoString + ": " +ex);
        }
    }

    public String getSNAInfo() {
        return "Store: " + storeName + ", SN: " + storageNodeName +
                ", Host: " + host;
    }

    @Override
    public String toString() {
        if (sshUser == null || sshUser.equals("")) {
            return storeName + "|" + storageNodeName + "|" + host + "|" +
                    rootdir;
        }
        return storeName + "|" + storageNodeName + "|" + sshUser + "@" + host +
                "|" + rootdir;
    }

    public String getStoreName() {
        return storeName;
    }

    public String getStorageNodeName() {
        return storageNodeName;
    }

    public String getHost() {
        return host;
    }

    public String getSSHUser() {
        return sshUser;
    }

    public void setSSHUser(String sshUser) {
        this.sshUser = sshUser;
    }

    public String getRootdir() {
        return rootdir;
    }

    public InetAddress getIP() {
        return IP;
    }

    public String getRemoteRootdir() {
        return remoteRootdir;
    }

    public void setRemoteRootdir(String remoteRootdir) {
        this.remoteRootdir = remoteRootdir;
    }

    /**
     * Convert host name to IP address
     * @return IP address when host is reachable; or null
     */
    private InetAddress convertToIP() {
        /*
         * Sometimes different host name actually point a same machine.
         * And a machine in network only has an unique IP. So use IP to
         * distinguish the machine.
         */
        try {
            if (host.equals(LOCALHOST_NAME) || host.equals(LOCALHOST_IP)) {
                IP = InetAddress.getLocalHost();
            } else {
                IP = InetAddress.getByName(host);
            }
            return IP;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        SNAInfo comparedSNAInfo = null;
        if (!(obj instanceof SNAInfo)) {
            return false;
        }
        comparedSNAInfo = (SNAInfo)obj;

        if (storeName == null || storageNodeName == null || IP == null ||
                comparedSNAInfo.storeName == null ||
                comparedSNAInfo.storageNodeName == null ||
                comparedSNAInfo.IP == null) {
            return false;
        }

        if (storeName.equals(comparedSNAInfo.storeName) &&
                storageNodeName.equals(comparedSNAInfo.storageNodeName) &&
                IP.equals(comparedSNAInfo.IP)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result +
                ((storeName == null) ? 0 : storeName.hashCode());
        result = prime * result +
                ((storageNodeName == null) ? 0 : storageNodeName.hashCode());
        result = prime * result +
                ((IP == null) ? 0 : IP.hashCode());
        return result;
    }
}
