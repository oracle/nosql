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

package oracle.kv.impl.sna;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * An interface for classes that instantiate and manage the execution contexts
 * of KV services.  Implementing classes are responsible for starting and
 * stopping services in either Threads or Processes.  This object is run in the
 * context of the Storage Node Agent.
 */
public abstract class ServiceManager {
    final protected ManagedService service;
    protected Logger logger;
    protected List<Listener> listeners = new ArrayList<Listener>();

    public ServiceManager(StorageNodeAgent sna, ManagedService service) {
        this.service = service;
        this.logger = sna.getLogger();
    }

    public ManagedService getService() {
        return service;
    }

    /**
     * Start a service instance.
     */
    public abstract void start() throws Exception;

    /**
     * Stop a running service instance by force.  This implies don't restart.
     */
    public abstract void stop();

    /**
     * Wait for a service instance to exit cleanly on clean shutdown.
     * @param millis wait for this long in milliseconds, 0 means wait forever
     */
    public abstract void waitFor(int millis);

    /**
     * Set state so an instance will not restart.
     */
    public abstract void dontRestart();

    /**
     * Check if the service is running, to the best of our knowledge.
     */
    public abstract boolean isRunning();

    /**
     * Reset restart state, default implementation is a no-op.
     */
    public void reset() {
        return;
    }

    public void resetLogger(Logger logger1) {
        this.logger = logger1;
    }

    /**
     * Can a force stop work for this manager?  Not by default (e.g. threads).
     * This is because killing the thread doesn't also eliminate RMI threads
     * created by the service itself.
     */
    public boolean forceOK(@SuppressWarnings("unused") boolean force) {
        return false;
    }

    /**
     * This is called when an unregistered SNA gets registered and there's a
     * running service, which can only be the bootstrap admin.
     */
    public void registered(@SuppressWarnings("unused")
                           StorageNodeAgent sna) {
        return;
    }

    /**
     * This is called when SNA parameter is changed and need reload new
     * parameter to this manager.
     */
    public void reloadSNParams() {
        return;
    }

    String[] createArgs() {
        return service.createArgs();
    }

    /**
     * A ServiceManager.Listener can be implemented by clients of this
     * interface to observe service startup and restart events.
     */
    public abstract class Listener {

        public Listener() {
            ServiceManager.this.addListener(this);
        }

        public void removeSelf() {
            ServiceManager.this.removeListener(this);
        }

        public abstract void startupCallback();
    }

    private void addListener(Listener lst) {
        listeners.add(lst);
    }

    private void removeListener(Listener lst) {
        listeners.remove(lst);
    }

    public void notifyStarted() {
        for (Listener lst : listeners) {
                lst.startupCallback();
        }
    }
}
