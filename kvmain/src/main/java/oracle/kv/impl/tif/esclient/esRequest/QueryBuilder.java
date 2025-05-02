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

package oracle.kv.impl.tif.esclient.esRequest;

import java.io.IOException;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;

public abstract class QueryBuilder {

    /*
     * queryName seems to be required for many types of query. But may be not
     * all. Hence not part of constructor. Force the subclass to set it.
     */
    protected String queryName;

    public String queryName() {
        return queryName;
    }

    public abstract void setQueryName(String queryName);

    public abstract void buildQueryJson(ESJsonBuilder builder)
        throws IOException;

    public final byte[] querySource() throws IOException {
        ESJsonBuilder builder = ESJsonBuilder.builder();
        builder.startStructure(); // start
        builder.startStructure(
                               "query");
        builder.startStructure(
                               queryName());
        buildQueryJson(builder);
        builder.endStructure() // queryName ends
               .endStructure() // query Ends
               .endStructure(); // end
        return builder.byteArray();
    }

}
