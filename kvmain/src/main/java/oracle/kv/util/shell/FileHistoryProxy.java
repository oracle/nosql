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

package oracle.kv.util.shell;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;

/**
 * A proxy class for <code>org.jline.reader.impl.history.DefaultHistory</code>
 * object that masks the secret information specified with given flag(s) in
 * command line with '*' before add to command history.
 */
public class FileHistoryProxy implements InvocationHandler {

    private static final String ADD_METHOD = "add";

    /* the org.jline.reader.impl.history.DefaultHistory object */
    private final Object obj;

    /* the flags whose values need to be mask */
    private final String[] maskFlags;

    public static Object create(Object obj, String[] maskFlags) {
        return Proxy.newProxyInstance(obj.getClass().getClassLoader(),
                                      obj.getClass().getInterfaces(),
                                      new FileHistoryProxy(obj, maskFlags));
    }

    private FileHistoryProxy(Object obj, String[] maskFlags) {
        this.obj = obj;
        assert (maskFlags != null && maskFlags.length > 0);
        this.maskFlags = maskFlags;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args)
        throws Throwable {

        if (isAddMethod(m) && args != null && args.length > 0) {
            final int idx = args.length - 1;
            args[idx] = Shell.toHistoryLine((String) args[idx], maskFlags);
        }
        return m.invoke(obj, args);
    }

    /**
     * Returns true if the method is add(String) or add(Instant, String).
     */
    private boolean isAddMethod(Method m) {
        return m.getName().equals(ADD_METHOD) &&
               (m.getParameterCount() == 1 &&
                m.getParameterTypes()[0] == String.class) ||
               (m.getParameterCount() == 2 &&
                m.getParameterTypes()[0] == Instant.class &&
                m.getParameterTypes()[1] == String.class);
    }

}
