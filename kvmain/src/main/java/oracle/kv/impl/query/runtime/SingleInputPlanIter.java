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

package oracle.kv.impl.query.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.impl.query.compiler.Expr;

/**
 * A convenience base class for concrete PlanIter subclasses with a single
 * input. Subclasses should declare the input field themselves, implement
 * {@link #getInput} to access it, and set it in constructors. This class will
 * handle serializing the input and taking it into account in {@link #equals}
 * and {@link #hashCode}.
 */
abstract class SingleInputPlanIter extends PlanIter {

    protected SingleInputPlanIter(Expr e, int resultReg) {
        super(e, resultReg);
    }

    protected SingleInputPlanIter(Expr e,
                                  int resultReg,
                                  boolean forCloud) {
        super(e, resultReg, forCloud);
    }

    protected SingleInputPlanIter(DataInput in, short serialVersion)
        throws IOException
    {
        super(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(getInput(), out, serialVersion);
    }

    /**
     * Get the single input associated with this iterator.
     *
     * @return the input
     */
    abstract protected PlanIter getInput();

    /**
     * Checks that the argument is of the same concrete type as this instance,
     * that the superclass says they are equal, and also that theInput is
     * equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) ||
            (obj == null) ||
            (getClass() != obj.getClass())) {
            return false;
        }
        final SingleInputPlanIter other = (SingleInputPlanIter) obj;
        return Objects.equals(getInput(), other.getInput());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getClass(), getInput());
    }
}
