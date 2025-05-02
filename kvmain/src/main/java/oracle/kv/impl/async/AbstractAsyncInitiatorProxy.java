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

package oracle.kv.impl.async;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.util.AbstractInvocationHandler;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An abstract base class for creating dynamic proxies that provide initiator
 * (client-side) implementations of AsyncVersionedRemote interfaces.
 *
 * <p>By default, this class has content-based hashCode and equals methods, so
 * subclasses should make sure to override those methods as needed to account
 * for additional fields. In addition, the default equals method compares
 * initiators, so initiator classes should also be coded to do content-based
 * comparisons.
 */
public abstract class AbstractAsyncInitiatorProxy
        extends AbstractInvocationHandler {

    static final Class<?>[] EMPTY_CLASSES = { };

    /** The initiator used to make calls. */
    final AsyncVersionedRemoteInitiator initiator;

    AbstractAsyncInitiatorProxy(Class<?> serviceInterface,
                                MethodCall<Short> getSerialVersionCall,
                                CreatorEndpoint endpoint,
                                DialogType dialogType,
                                Logger logger)
    {
        initiator = new Initiator(serviceInterface, getSerialVersionCall,
                                  endpoint, dialogType, logger);
    }

    /**
     * Creates a dynamic proxy that implements the specified async versioned
     * remote interface with this instance as the invocation handler.
     *
     * @param <T> the type of the async versioned remote interface
     * @param serviceInterface the async versioned remote interface
     * @param expectedDialogTypeFamily the dialog type family that the dialog
     * type should belong to
     * @param dialogType the dialog type
     * @return the proxy
     * @throws IllegalArgumentException if the dialog type has the wrong type
     * family
     */
    <T> T createProxy(Class<T> serviceInterface,
                      DialogTypeFamily expectedDialogTypeFamily,
                      DialogType dialogType)
    {
        if (dialogType.getDialogTypeFamily() != expectedDialogTypeFamily) {
            throw new IllegalArgumentException(
                "Dialog type should have dialog type family " +
                expectedDialogTypeFamily + ", found: " +
                dialogType.getDialogTypeFamily());
        }
        return serviceInterface.cast(
            Proxy.newProxyInstance(serviceInterface.getClassLoader(),
                                   new Class<?>[] { serviceInterface },
                                   this));
    }

    /**
     * An initiator subclass that provides the MethodCall instance for
     * getSerialVersion.
     */
    private class Initiator extends AsyncVersionedRemoteInitiator {
        private final String abbrevClassName;
        private final MethodCall<Short> getSerialVersionCall;

        Initiator(Class<?> serviceInterface,
                  MethodCall<Short> getSerialVersionCall,
                  CreatorEndpoint endpoint,
                  DialogType dialogType,
                  Logger logger) {
            super(endpoint, dialogType, logger);
            abbrevClassName = getAbbreviatedClassName(serviceInterface) +
                "-initiator";
            this.getSerialVersionCall = getSerialVersionCall;
        }
        @Override
        protected MethodCall<Short> getSerialVersionCall() {
            return getSerialVersionCall;
        }
        /** Return name based on the service interface name. */
        @Override
        protected String getAbbreviatedClassName() {
            return abbrevClassName;
        }
    }

    /** Include the initiator. */
    @Override
    public String toString() {
        return String.format("%s[%s]",
                             getClass().getSimpleName(),
                             initiator);
    }

    /** Returns the hash code of the initiator. */
    @Override
    public int hashCode() {
        return initiator.hashCode();
    }

    /**
     * Returns true if the argument is an instance of the same class and
     * it's initiator is equal.
     */
    @Override
    public boolean equals(@Nullable Object object) {
        if ((object == null) || !getClass().equals(object.getClass())) {
            return false;
        }
        final AbstractAsyncInitiatorProxy other =
            (AbstractAsyncInitiatorProxy) object;
        return initiator.equals(other.initiator);
    }

    /**
     * Finds the MethodCall object associated with the specified method and
     * method arguments.
     *
     * @param method the method
     * @param methodCallArgs the method arguments, which may be null
     * @return the method call instance
     * @throws Exception if there is a problem getting the method call instance
     */
    abstract MethodCall<?> getMethodCall(Method method,
                                         Object @Nullable[] methodCallArgs)
        throws Exception;
}
