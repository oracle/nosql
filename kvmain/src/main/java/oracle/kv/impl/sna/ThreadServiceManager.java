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

/**
 * Implementation of ServiceManager that uses threads.
 */
public class ThreadServiceManager extends ServiceManager implements Runnable {

    private Thread thread;

    public ThreadServiceManager(StorageNodeAgent sna,
                                ManagedService service) {
        super(sna, service);
    }

    /**
     * This method must call the service as if it were invoked in a separate
     * process, using main().
     */
    @Override
    public void run() {
        String[] args = service.createArgs();
        ManagedService.main(args);
        logger.info("Thread returning for service " + service.getServiceName());
    }

    @Override
    public void start()
        throws Exception {

        thread = new Thread(this);
        thread.start();
        logger.fine("ThreadServiceManager started thread");
        notifyStarted();
    }

    /**
     * For threads stop() is the same as waitFor().  Interrupt() may not
     * work but it should.
     */
    @Override
    public void stop() {
        thread.interrupt();
        waitFor(0);
    }

    @Override
    public void waitFor(int millis) {
        try {
            thread.join(millis);
            if (thread.isAlive()) {
                logger.info("Thread join timed out for " +
                            service.getServiceName() + ", timeout millis: " +
                            millis);
            } else {
                logger.info("Joined thread for " + service.getServiceName());
            }
        } catch (InterruptedException ie) {
            logger.info("Thread wait for service was interrupted");
        }
    }

    /**
     * Nothing to do for threads -- they won't restart.
     */
    @Override
    public void dontRestart() {
    }

    /**
     * With threads it is hard to know for sure, but if we have a thread then
     * we will assume true.
     */
    @Override
    public boolean isRunning() {
        return thread != null;
    }
}
