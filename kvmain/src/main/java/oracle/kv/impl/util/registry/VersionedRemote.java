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

package oracle.kv.impl.util.registry;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Base interface for all service interfaces.
 * <p>
 * The definition and implementation of a service involves several interfaces
 * and classes.  A service implementation is versioned by means of a
 * serialVersion parameter.  The overall approach and class interactions are
 * described below.
 * <p>
 * Two very different approaches are taken depending on whether "fast
 * serialization" or regular Java serialization is used by a particular
 * service.  Fast serialization is currently only used by the RequestHandler
 * service (which is used to implement the public API) and all other services
 * use Java serialization.  For information on fast serialization, see
 * o.kv.impl.util.FastExternalizable and o.kv.impl.api.*.  The approach taken
 * by all other services, using regular Java serialization, is described below.
 * <p>
 * A service definition and implementation includes the following interfaces
 * and classes.
 * <p>
 * <em>Service Interface</em> -- The service Java interface is an RMI
 * interface that extends VersionedRemote, which extends java.rmi.Remote and
 * adds the 'short getSerialVersion()' method.  The last parameter of all
 * other, service-specific methods must be 'short serialVersion'.
 * <p>
 * <em>Service Implementation</em> -- The service class extends
 * VersionedServiceImpl, which implements getSerialVersion and adds the
 * service-specific method implementations.  Service methods may adjust the
 * content or format of method parameters and return value to account for
 * version differences between client and service.  The version parameter is
 * necessary because a service has no per-client state.
 * <p>
 * <em>Service API</em> -- The service API class wraps the service interface to
 * provide an API called by clients of the remote service.  The API class
 * methods are generally identical to the remote interface methods, except the
 * API methods do not have a 'short serialVersion' parameter.  The main
 * function of the API wrapper is to add the serialVersion parameter when
 * forwarding each method to its corresponding remote interface method.
 * <p>
 * The API class extends RemoteAPI.  RemoteAPI caches the effective version to
 * be used for passing to service interface methods, and returns it from the
 * getSerialVersion method, which it implements.  The effective version is the
 * minimum of the current version of the client and service.
 * <p>
 * An API class may do addition processing before or after method forwarding.
 * Like service implementation methods, API methods may adjust the content or
 * format of method parameters and return value to account for version
 * differences between client and service.  Preliminary argument checking could
 * also be done before forwarding.
 * <p>
 * Because an API class may do version-specific processing, it may be desirable
 * to have API subclasses for different combinations of version.  Rather than
 * having a public constructor, each API class has a static factory method,
 * 'wrap(remoteInterface)', that could potentially create version-specific
 * subclasses.
 * <p>
 * The API wrapper is created by RegistryUtils after looking up the remote
 * interface.  The RegistryUtils methods return the API wrapper rather than the
 * underlying remote interface.  In other cases -- when a service interface is
 * obtained without using RegistryUtils -- the API wrapper should also be
 * created and used.  Although it is possible to directly call a service
 * interface method, this is discouraged because it bypasses the API class.
 * However, there are times when this is necessary.  Two special cases are:
 * <ul>
 * <li>A service method that is invoked by a client may sometimes need to call
 * another service method directly; in this case the serialVersion param passed
 * the client should be forwarded on to the other service method.</li>
 * <li>A service method may be invoked directly by the service process, for
 * example, when starting and stopping the service; in this case
 * SerialVersion.CURRENT should be passed as the serialVersion param.</li>
 * </ul>
 * <p>
 * <em>Version Adjustments</em> -- TODO: describe the types of version-specific
 * processing that an API or service method implementation might perform.  Only
 * applies to cases where an existing method is changed.
 * <p>
 * <em>Adding Interface Methods</em> -- TODO: describe how an interface method
 * can be added in a new version of the service, and how clients can check for
 * the availability of the method before calling it.
 * <p>
 * <em>Adding New Interfaces</em> -- TODO: describe how a completely new
 * service interface can be added in a new version of the service, and how
 * clients can check for the availability of the interface before calling it.
 *
 * @see RemoteAPI
 * @see VersionedRemoteImpl
 * @see RegistryUtils
 * @see oracle.kv.impl.util.SerialVersion
 */
public interface VersionedRemote extends Remote {

    /**
     * Returns the highest serialization version supported by this service,
     * which should always be SerialVersion.CURRENT.
     * <p>
     * A client of this service should calculate the minimum of the value
     * returned by this method and the highest version supported by the client.
     * This effective version should be cached by the client and passed as the
     * serialVersion parameter when calling a service method.  Calculation and
     * caching of the effective version is done by the RemoteAPI base class,
     * and passing the effective version is done by RemoteAPI subclasses during
     * method forwarding.
     * <p>
     * Note that for the RequestHandler interface, the serial version is also
     * embedded in the Request and Response objects so that it can be used
     * during custom serialization and deserialization.  The Response object
     * returned by execute will use the same serial version as the Request.
     */
    public short getSerialVersion()
        throws RemoteException;
}
