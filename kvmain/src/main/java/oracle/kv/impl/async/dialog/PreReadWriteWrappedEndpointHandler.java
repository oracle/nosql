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

package oracle.kv.impl.async.dialog;

import java.util.concurrent.ScheduledExecutorService;

import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointHandler;
import oracle.kv.impl.async.EndpointHandlerManager;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.perf.DialogEndpointGroupPerfTracker;

/**
 * The abstract class of a pre-read or pre-write wrapped endpoint handler.
 */
public abstract class PreReadWriteWrappedEndpointHandler
<H extends AbstractDialogEndpointHandler>
    implements EndpointHandler, EndpointHandlerManager {

    protected final EndpointHandlerManager parentManager;
    protected final EndpointConfig endpointConfig;
    protected final NetworkAddress remoteAddress;

    protected PreReadWriteWrappedEndpointHandler(
        EndpointHandlerManager parentManager,
        EndpointConfig endpointConfig,
        NetworkAddress remoteAddress) {
        this.parentManager = parentManager;
        this.endpointConfig = endpointConfig;
        this.remoteAddress = remoteAddress;
    }

    protected abstract H getInnerEndpointHandler();

    @Override
    public NetworkAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public ScheduledExecutorService getSchedExecService() {
        return getInnerEndpointHandler().getSchedExecService();
    }

    @Override
    public long getUUID() {
        return getInnerEndpointHandler().getUUID();
    }

    @Override
    public long getConnID() {
        return getInnerEndpointHandler().getConnID();
    }

    @Override
    public String getStringID() {
        return getInnerEndpointHandler().getStringID();
    }

    @Override
    public void startDialog(int dialogType,
                            DialogHandler dialogHandler,
                            long timeoutMillis) {
        getInnerEndpointHandler().startDialog(
                dialogType, dialogHandler, timeoutMillis);
    }

    @Override
    public int getNumDialogsLimit() {
        return getInnerEndpointHandler().getNumDialogsLimit();
    }

    @Override
    public void shutdown(String detail, boolean force) {
        getInnerEndpointHandler().shutdown(detail, force);
    }

    @Override
    public void onHandlerShutdown(EndpointHandler handler) {
        parentManager.onHandlerShutdown(this);
    }

    @Override
    public DialogEndpointGroupPerfTracker getEndpointGroupPerfTracker() {
        return parentManager.getEndpointGroupPerfTracker();
    }
}
