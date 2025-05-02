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

package oracle.kv.impl.rep.admin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.sna.SNAFaultException;

/**
 * Subclass of InternalFaultException used to indicate that the fault originated
 * in the RepNode when satisfying an administrative service request.
 * <p>
 * Note that the original service request may have been issued by the SNA or by
 * the Admin which uses the SNA as its proxy on the SN to perform a request on
 * on the RN on its behalf. So the Admin may see this exception even though it
 * actually issued this request to the SNA. A fault that originates in the SNA
 * results in a {@link SNAFaultException}.
 */
public class RepNodeAdminFaultException extends InternalFaultException {
    private static final long serialVersionUID = 1L;

    public RepNodeAdminFaultException(Throwable cause) {
        super(cause);
    }

    public RepNodeAdminFaultException(DataInput in, short serialVersion)
        throws IOException
    {
        super(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        super.writeFastExternal(out, serialVersion);
    }
}
