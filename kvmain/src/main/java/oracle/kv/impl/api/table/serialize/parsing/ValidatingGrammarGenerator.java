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
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.kv.impl.api.table.serialize.parsing;

import java.util.Map;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldMapEntry;
import oracle.kv.impl.api.table.FixedBinaryDefImpl;
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;

/**
 * The class that generates validating grammar.
 */
class ValidatingGrammarGenerator {
    final public static String NULL = "null";

    /**
     * Returns the non-terminal that is the start symbol for the grammar for the
     * given schema <tt>sc</tt>. If there is already an entry for the given
     * schema in the given map <tt>seen</tt> then that entry is returned.
     * Otherwise a new symbol is generated and an entry is inserted into the
     * map.
     *
     * @param sc
     *            The schema for which the start symbol is required
     * @param seen
     *            A map of schema to symbol mapping done so far.
     * @return The start symbol for the schema
     */
    protected Symbol generate(FieldDefImpl sc, Map<LitS, Symbol> seen) {
        if (sc.isMRCounter()) {
            return Symbol.seq(Symbol.repeat(Symbol.CRDT_END,
                generate(sc.getCRDTElement(), seen),
                Symbol.INT), Symbol.CRDT_START);
        }
        switch (sc.getType()) {
        case BOOLEAN:
            return Symbol.BOOLEAN;
        case INTEGER:
            return Symbol.INT;
        case LONG:
            return Symbol.LONG;
        case FLOAT:
            return Symbol.FLOAT;
        case DOUBLE:
            return Symbol.DOUBLE;
        case STRING:
            return Symbol.STRING;
        case TIMESTAMP:
        case NUMBER:
        case BINARY:
        case JSON:
            return Symbol.BYTES;
        case FIXED_BINARY:
            return Symbol.seq(
                    Symbol.intCheckAction(((FixedBinaryDefImpl) sc).getSize()),
                    Symbol.FIXED);
        case ENUM:
            return Symbol.seq(
                    Symbol.intCheckAction(
                            ((EnumDefImpl) sc).getValues().length),
                    Symbol.ENUM);
        case ARRAY:
            return Symbol.seq(
                    Symbol.repeat(Symbol.ARRAY_END,
                            generate(((ArrayDefImpl) sc).getElement(), seen)),
                    Symbol.ARRAY_START);
        case MAP:
            return Symbol.seq(Symbol.repeat(Symbol.MAP_END,
                    generate(((MapDefImpl) sc).getElement(), seen),
                    Symbol.STRING), Symbol.MAP_START);
        case RECORD: {
            LitS wsc = new LitS(sc);
            Symbol rresult = seen.get(wsc);
            if (rresult == null) {
                Symbol[] production = new Symbol[((RecordDefImpl) sc)
                        .getNumFields()];

                // We construct a symbol without filling the array. Please see
                // {@link Symbol#production} for the reason.

                rresult = Symbol.seq(production);
                seen.put(wsc, rresult);

                int i = production.length;
                for (String fname : ((RecordDefImpl) sc).getFieldNames()) {
                    FieldMapEntry fme = ((RecordDefImpl) sc)
                            .getFieldMapEntry(fname, true);
                    production[--i] = generateNullable(fme, seen);
                }
            }
            return rresult;
        }

        default:
            throw new RuntimeException("Unexpected schema type");
        }
    }

    private Symbol generateNullable(FieldMapEntry fme, Map<LitS, Symbol> seen) {
        if (fme.isNullable()) {
            final int size = 2;
            Symbol[] symbols = new Symbol[size];
            String[] labels = new String[size];

            /**
             * We construct a symbol without filling the arrays. Please see
             * {@link Symbol#production} for the reason.
             */
            if (fme.getDefaultValue().isNull()) {
                symbols[0] = Symbol.NULL;
                labels[0] = NULL;
                symbols[1] = generate(fme.getFieldDef(), seen);
                labels[1] = fme.getFieldName();
            } else {
                symbols[0] = generate(fme.getFieldDef(), seen);
                labels[0] = fme.getFieldName();
                symbols[1] = Symbol.NULL;
                labels[1] = NULL;
            }

            return Symbol.seq(Symbol.alt(symbols, labels), Symbol.UNION);
        }
        return generate(fme.getFieldDef(), seen);
    }

    /** A wrapper around Schema that does "==" equality. */
    static class LitS {
        public final FieldDefImpl actual;

        public LitS(FieldDefImpl actual) {
            this.actual = actual;
        }

        /**
         * Two LitS are equal if and only if their underlying schema is the same
         * (not merely equal).
         */
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LitS))
                return false;
            return actual == ((LitS) o).actual;
        }

        @Override
        public int hashCode() {
            return actual.hashCode();
        }
    }
}

