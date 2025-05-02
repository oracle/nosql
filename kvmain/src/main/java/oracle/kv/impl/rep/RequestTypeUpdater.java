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

package oracle.kv.impl.rep;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;

/**
 * The class used to update the enabled request type of this RepNode. The
 * enabled request type is configured via parameters, a parameter listener
 * captures changes of {@link ParameterState#RN_ENABLED_REQUEST_TYPE} in order
 * to reflect in request handler of this RepNode.
 */
public class RequestTypeUpdater implements ParameterListener {

    public enum RequestType {
        ALL,
        NONE,
        READONLY
    }

    private final RequestHandlerImpl requestHandler;
    private final Logger logger;

    RequestTypeUpdater(RequestHandlerImpl requestHandler, Logger logger) {
        this.requestHandler = requestHandler;
        this.logger = logger;
    }

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        final String enabledRequestType = newMap.getOrDefault
            (ParameterState.RN_ENABLED_REQUEST_TYPE).asString();
        RequestType newType;
        try {
            newType = RequestType.valueOf(enabledRequestType);
        } catch (IllegalArgumentException iae) {
            logger.log(Level.WARNING,
                       "Specifying invalid type of enabled client type " +
                       enabledRequestType);
            return;
        }
        requestHandler.enableRequestType(newType);
        logger.log(Level.INFO,
                   "Set enabled request type " + enabledRequestType);
    }
}
