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

package oracle.kv.impl.tif.esclient.httpClient;

import java.io.Serializable;

import static oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods.HttpURIScheme;

public final class HttpHost implements Serializable {
    private static final long serialVersionUID = -7529410654042457626L;
    private final String schemeName;
    private final String hostName;
    private final int port;
    public static final String DEFAULT_SCHEME = HttpURIScheme;
    public HttpHost(final String scheme, 
                    final String hostName, final int port) {
        this.schemeName = scheme != null ? scheme.toLowerCase() :
                                           DEFAULT_SCHEME;    
        this.hostName = hostName;
        this.port = port;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public String getSchemeName() {
        return schemeName;
    }

    public String toURI() {
        final StringBuilder buf = new StringBuilder();
        buf.append(this.schemeName);
        buf.append("://");
        buf.append(hostName);
        if (getPort() != -1) {
            buf.append(":");
            buf.append(getPort());
        }
        return buf.toString();
    }

    @Override
    public String toString() {
        return toURI();
    }
}

