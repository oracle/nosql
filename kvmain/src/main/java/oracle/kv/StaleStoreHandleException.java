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
package oracle.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Thrown when a KVStore instance is no longer valid. The application should
 * call {@link KVStore#close} and obtain a new handle from {@link
 * KVStoreFactory#getStore KVStoreFactory.getStore}.
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class StaleStoreHandleException extends FastExternalizableException {

    private static final long serialVersionUID = 1L;

    private final String rationale;

    /**
     * For internal use only.
     * @hidden
     */
    public StaleStoreHandleException(String rationale) {
        super(rationale);
        this.rationale = rationale;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public StaleStoreHandleException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);

        /*
         * Use the super's message so that the boilerplate added by this
         * class's getMessage method isn't added twice
         */
        rationale = super.getMessage();
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link FastExternalizableException}) {@code super}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        /*
         * Use the super's message so that the boilerplate added by this
         * class's getMessage method isn't added twice
         */
        writeFastExternal(out, serialVersion, super.getMessage());
    }

    @Override
    public String getMessage() {
        return "This KVStore instance is no longer valid and should be " +
            "closed: " + rationale;
    }
}
