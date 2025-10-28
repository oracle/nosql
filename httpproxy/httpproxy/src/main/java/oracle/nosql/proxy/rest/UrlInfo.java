/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.rest;

import java.util.ArrayList;
import java.util.List;

/**
* Represents the URL path information
*/
public class UrlInfo {

    private final String path;
    private final List<String> portions;
    private final List<Integer> indexParams;

    public UrlInfo(String path) {
        this.path = path;

        portions = new ArrayList<String>();
        indexParams = new ArrayList<Integer>();

        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        String[] paths = path.split("/");
        for (String portion : paths) {
            if (portion.startsWith("{")) {
                assert(portion.endsWith("}"));
                portion = portion.substring(1, portion.length() - 1);
                indexParams.add(portions.size());
            }
            portions.add(portion);
        }
    }

    public List<Integer> getIndexParams() {
        return indexParams;
    }

    public String getParamName(int index) {
        return portions.get(index);
    }

    public int size() {
        return portions.size();
    }

    /**
     * Compares the URL path with the given Request's URL path in case
     * sensitive manner.
     */
    public boolean match(RequestParams request) {
        final String[] requestPaths = request.getPaths();
        if (requestPaths.length != size()) {
            return false;
        }

        /* Compares path portions */
        for (int i = 0; i < portions.size(); i++) {
            if (indexParams.contains(i)) {
                continue;
            }
            if (!portions.get(i).equals(requestPaths[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("size=" + size());
        sb.append("; paths=");
        sb.append(portions);
        if (!indexParams.isEmpty()) {
            sb.append("; params=]");
            for (int index : indexParams) {
                sb.append(portions.get(index));
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
