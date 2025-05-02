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

package oracle.kv.impl.rep.migration.generation;

/**
 * Object represents partition generation number (PGN), start from 0.
 * Internally the PGN is represented as a Java int (32 bits), which allows
 * 2147483647 migrations at most, enough for about more than 4000 years if a
 * partition is migrated every minute.
 */
public class PartitionGenNum implements Comparable<PartitionGenNum> {

    /* the very first generation number for all partitions */
    private static final int MIN_PAR_GEN_NUM = 0;

    /* singleton instance to represent generation zero */
    private static final PartitionGenNum GENERATION_ZERO =
        new PartitionGenNum(MIN_PAR_GEN_NUM);

    /* generation number */
    private final int genNum;

    public PartitionGenNum(int genNum) {
        if (genNum < MIN_PAR_GEN_NUM) {
            throw new IllegalArgumentException("Generation number cannot be " +
                                               "less than min=" +
                                               MIN_PAR_GEN_NUM);
        }
        this.genNum = genNum;
    }

    /**
     * Returns the generation number
     *
     * @return  the generation number
     */
    public int getNumber() {
        return genNum;
    }

    /**
     * Returns a new instance with the next generation number
     *
     * @return the next generation number
     */
    public PartitionGenNum incrGenNum() {
        return new PartitionGenNum(genNum + 1);
    }

    @Override
    public int compareTo(PartitionGenNum o) {
        return genNum - o.getNumber();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final PartitionGenNum other = (PartitionGenNum) obj;
        return genNum == other.getNumber();
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(genNum);
    }

    @Override
    public String toString() {
        return Integer.toString(genNum);
    }

    /**
     * Returns an instance of very first generation
     *
     * @return an instance of very first generation
     */
    public static PartitionGenNum generationZero() {
        return GENERATION_ZERO;
    }
}
