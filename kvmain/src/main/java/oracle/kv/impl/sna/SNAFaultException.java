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

import java.io.DataInput;
import java.io.IOException;

import oracle.kv.impl.fault.InternalFaultException;

/**
 * Subclass of InternalFaultException used to indicate that the fault
 * originated in the SNA when satisfying an SNA service request.
 */
public class SNAFaultException extends InternalFaultException {

    private static final long serialVersionUID = 1L;

    public SNAFaultException(Throwable cause) {
        super(cause);
    }

    public SNAFaultException(DataInput in, short serialVersion)
        throws IOException
    {
        super(in, serialVersion);
    }
}
