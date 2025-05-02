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

package oracle.kv.impl.monitor;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * These are methods available on the server side, for a Tracker to interact
 * with a remote TrackerListener. Implementations of these methods should
 * return as quickly as possible and should avoid calling other remote
 * methods.
 */
public interface TrackerListener extends Remote {

    public long getInterestingTime() throws RemoteException;

    public void notifyOfNewEvents() throws RemoteException;
}
