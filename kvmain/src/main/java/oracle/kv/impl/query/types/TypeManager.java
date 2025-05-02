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

package oracle.kv.impl.query.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.ExprType.TypeCode;
import oracle.kv.table.FieldDef;

public class TypeManager {

    /*
     * Then, QUANT_SUBTYPE_MATRIX[q1][q2] is true iff for each possible set
     * S represented by q1, S is also represented by q2. For eaxmple,
     * QUANT_SUBTYPE_MATRIX[?][+] is false because ? represents the empty set,
     * which is not represented by +.
     */
    static final boolean QUANT_SUBTYPE_MATRIX[][] =
    {
        // 1      ?      +      *
        {  true,  true,  true,  true }, // 1
        {  false, true,  false, true }, // ?
        {  false, false, true,  true }, // +
        {  false, false, false, true }  // *
    };

    /*
     * QUANT_UNION_MATRIX[q1][q2] is the "minimum" quantifier that is equally
     * or more permissive than both q1 and q2.
     */
    static final Quantifier QUANT_UNION_MATRIX[][] =
    {
        // 1               ?                +                *
        { Quantifier.ONE,  Quantifier.QSTN, Quantifier.PLUS, Quantifier.STAR }, // 1
        { Quantifier.QSTN, Quantifier.QSTN, Quantifier.STAR, Quantifier.STAR }, // ?
        { Quantifier.PLUS, Quantifier.STAR, Quantifier.PLUS, Quantifier.STAR }, // +
        { Quantifier.STAR, Quantifier.STAR, Quantifier.STAR, Quantifier.STAR }, // *
    };

    /*
     * QUANT_INTERS_MATRIX[q1][q2] is the maximum quantifier that is equally
     * or less permissive than both q1 and q2.
     */
    static final Quantifier QUANT_INTERS_MATRIX[][] =
    {
        // 1              ?                +                *
        { Quantifier.ONE, Quantifier.ONE,  Quantifier.ONE,  Quantifier.ONE  },  // 1
        { Quantifier.ONE, Quantifier.QSTN, Quantifier.ONE , Quantifier.QSTN },  // ?
        { Quantifier.ONE, Quantifier.ONE,  Quantifier.PLUS, Quantifier.PLUS },  // +
        { Quantifier.ONE, Quantifier.QSTN, Quantifier.PLUS, Quantifier.STAR },  // *
    };

    /*
     *
     */
    static final Quantifier QUANT_CONCAT_MATRIX[][] =
    {
        // 1               ?                +                *
        { Quantifier.PLUS, Quantifier.PLUS, Quantifier.PLUS, Quantifier.PLUS }, // 1
        { Quantifier.PLUS, Quantifier.STAR, Quantifier.PLUS, Quantifier.STAR }, // ?
        { Quantifier.PLUS, Quantifier.PLUS, Quantifier.PLUS, Quantifier.PLUS }, // +
        { Quantifier.PLUS, Quantifier.STAR, Quantifier.PLUS, Quantifier.STAR }, // *
    };

    /*
     * SUBTYPE_MATRIX[sub][sup] == false means that sub is definitely not a
     * subtype of sup.
     *
     * SUBTYPE_MATRIX[sub][sup] == true means that sub may be a subtype of sup.
     */
    static final boolean SUBTYPE_MATRIX[][] =
    {
        // EMPTY  ANY   AATM   JATM   JSON   AREC   NUM
        // INT    LONG  FLT    DBL    STR    BOOL   BIN    FBIN   ENUM   TS
        // REC    ARR   MAP

        { true,  true,  true,  true,  true,  true,  true,
          true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
          true,  true,  true }, // EMPTY

        { false, true,  false, false, false, false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, false }, // ANY

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, false }, // ATM

        { false, true,  true,  true,  true,  false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, false }, // JATM

        { false, true,  false, false, true,  false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, false }, // JSON

        { false, true,  false, false, false, true,  false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, false }, // AREC

        { false, true,  true,  true,  true,  false, true,
          false, false, false, false, false, false, false, false, false, false,
          false, false, false }, // NUM

        { false, true,  true,  true,  true,  false, true,
          true,  true,  false, false, false, false, false, false, false, false,
          false, false, false }, // INT

        { false, true,  true,  true,  true,  false, true,
          false, true,  false, false, false, false, false, false, false, false,
          false, false, false }, // LONG

        { false, true,  true,  true,  true,  false, true,
          false, false, true,  true,  false, false, false, false, false, false,
          false, false, false }, // FLT

        { false, true,  true,  true,  true,  false, true,
          false, false, false, true,  false, false, false, false, false, false,
          false, false, false }, // DBL

        { false, true,  true,  true,  true, false, false,
          false, false, false, false, true, false, false, false, false,  false,
          false, false, false }, // STR

        { false, true,  true,  true,  true,  false, false,
          false, false, false, false, false, true,  false, false, false, false,
          false, false, false }, // BOOL

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, true,  false, false, false,
          false, false, false }, // BIN

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, true,  true,  false, false,
          false, false, false }, // FBIN

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, false, false, true,  false,
          false, false, false }, // ENUM

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, false, false, false, true,
          false, false, false }, // TS

        // EMPTY  ANY   AATM   JATM   JSON   AREC   NUM
        // INT    LONG  FLT    DBL    STR    BOOL   BIN    FBIN   ENUM   TS
        // REC    ARR   MAP

        { false, true,  false, false, false, true,  false,
          false, false, false, false, false, false, false, false, false, false,
          true,  false, false }, // REC

        { false, true,  false, false, true,  false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, true,  false }, // ARR

        { false, true,  false, false, true,  false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, true } // MAP
    };

    static final boolean INTERSECT_MATRIX[][] =
    {
        // EMPTY  ANY   AATM   JATM   JSON   AREC   NUM
        // INT    LONG  FLT    DBL    STR    BOOL   BIN    FBIN   ENUM   TS
        // REC    ARR   MAP

        { false, true,  false, false, false, false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, false }, // EMPTY

        { true,  true,  true,  true,  true,  true,  true,
          true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
          true,  true,  true }, // ANY

        { false, true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
          false, false, false }, // AATM

        { false, true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  true,  true,  false, false, false, false,
          false, false, false }, // JATM

        { false, true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  true,  true,  false, false, false, false,
          false, true,  true }, // JSON

        { false, true,  false, false, false, true,  false,
          false, false, false, false, false, false, false, false, false, false,
          true, false, false }, // AREC

        { false, true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  false, false, false, false, false, false,
          false, false, false }, // NUM

        { false, true,  true,  true,  true,  false, true,
          true,  true,  false, false, false, false, false, false, false, false,
          false, false, false }, // INT

        { false, true,  true,  true,  true,  false, true,
          true,  true,  false, false, false, false, false, false, false, false,
          false, false, false }, // LONG

        { false, true,  true,  true,  true,  false, true,
          false, false, true,  true,  false, false, false, false, false, false,
          false, false, false }, // FLT

        { false, true,  true,  true,  true,  false, true,
          false, false, true,  true,  false, false, false, false, false, false,
          false, false, false }, // DBL

        { false, true,  true,  true,  true, false, false,
          false, false, false, false, true, false, false, false,  false,  false,
          false, false, false }, // STR

        { false, true,  true,  true,  true,  false, false,
          false, false, false, false, false, true,  false, false, false, false,
          false, false, false }, // BOOL

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, true,  true,  false, false,
          false, false, false }, // BIN

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, true,  true,  false, false,
          false, false, false }, // FBIN

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, false, false, true,  false,
          false, false, false }, // ENUM

        { false, true,  true,  false, false, false, false,
          false, false, false, false, false, false, false, false, false, true,
          false, false, false }, // TS

        // EMPTY  ANY   AATM   JATM   JSON   AREC   NUM
        // INT    LONG  FLT    DBL    STR    BOOL   BIN    FBIN   ENUM   TS
        // REC    ARR   MAP

        { false, true,  false, false, false, true,  false,
          false, false, false, false, false, false, false, false, false, false,
          true,  false, false }, // REC

        { false, true,  false, false, true,  false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, true,  false }, // ARR

        { false, true,  false, false, true,  false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, true } // MAP
    };

    /*
     * COMPARISON_MATRIX[t1][t2] == false means that t1 and t2 are definitely
     * not comparable.
     *
     * COMPARISON_MATRIX[t1][t2] == true means that t1 and t2 may be comparable.
     */
    static final boolean COMPARISON_MATRIX[][] =
    {
        // EMPTY  ANY   AATM   JATM   JSON   AREC   NUM
        // INT    LONG  FLT    DBL    STR    BOOL   BIN    FBIN   ENUM   TS
        // REC    ARR   MAP

        // EMPTY
        { true,  true,  true,  true,  true,  true,  true,
          true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
          true,  true,  true },

        // ANY
        { true,  true,  true,  true,  true,  true,  true,
          true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
          true,  true,  true },
        // AATM
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  true,  true,  true,  true,  true,  true,
          false, false, false },
        // JATM
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  true,  true,  false, false, true,  false,
          false, false, false },
        // JSON
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  true,  true,  false, false, true,  false,
          false, true,  true },
        // AREC
        { true,  true,  false, false, false, true,  false,
          false, false, false, false, false, false, false, false, false, false,
          true,  false, false },

        // NUM
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  false, false, false, false, false, false,
          false, false, false },
        // INT
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  false, false, false, false, false, false,
          false, false, false },
        // LONG
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  false, false, false, false, false, false,
          false, false, false },
        // FLT
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  false, false, false, false, false, false,
          false, false, false },
        // DBL
        { true,  true,  true,  true,  true,  false, true,
          true,  true,  true,  true,  false, false, false, false, false, false,
          false, false, false },
        // STR
        { true,  true,  true,  true,  true,  false, false,
          false, false, false, false, true,  false, false, false, true,  false,
          false, false, false },
        // BOOL
        { true,  true,  true,  true,  true,  false, false,
          false, false, false, false, false, true,  false, false, false, false,
          false, false, false },
        // BIN
        { true,  true,  true,  false, false, false, false,
          false, false, false, false, false, false, true,  true,  false, false,
          false, false, false },
        // FBIN
        { true,  true,  true,  false, false, false, false,
          false, false, false, false, false, false, true,  true,  false, false,
          false, false, false },
        // ENUM
        { true,  true,  true,  false, false, false, false,
          false, false, false, false, true,  false, false, false, true,  false,
          false, false, false },
        // TS
        { true,  true,  true,  false, false, false, false,
          false, false, false, false, false,  false, false, false, false,  true,
          false, false, false },

        // EMPTY  ANY   AATM   JATM   JSON   AREC   NUM
        // INT    LONG  FLT    DBL    STR    BOOL   BIN    FBIN   ENUM   TS
        // REC    ARR   MAP

        // REC
        { true,  true,  false, false, false, true,  false,
          false, false, false, false, false, false, false, false, false, false,
          true,  false, false },
        // ARR
        { true,  true,  false, false, true, false,  false,
          false, false, false, false, false, false, false, false, false, false,
          false, true,  false },
        // MAP
        { true,  true,  false, false, true,  false, false,
          false, false, false, false, false, false, false, false, false, false,
          false, false, true }
    };

    static final ExprType TIMESTAMP_TYPES_ONE[] =
        new ExprType[FieldDefImpl.Constants.timestampDefs.length];

    static {
        for (int i = 0; i < FieldDefImpl.Constants.timestampDefs.length; i++) {
            TIMESTAMP_TYPES_ONE[i] =
                new ExprType(FieldDefImpl.Constants.timestampDefs[i],
                             Quantifier.ONE, false);
        }
    }

    static final ExprType TIMESTAMP_TYPES_QSTN[] =
        new ExprType[FieldDefImpl.Constants.timestampDefs.length];

    static {
        for (int i = 0; i < FieldDefImpl.Constants.timestampDefs.length; i++) {
            TIMESTAMP_TYPES_QSTN[i] =
                new ExprType(FieldDefImpl.Constants.timestampDefs[i],
                             Quantifier.QSTN, false);
        }
    }

    static final ExprType BUILTIN_TYPES[][] =
    {
        {
            new ExprType(FieldDefImpl.Constants.emptyDef, Quantifier.ONE, true)
        },
        {
            new ExprType(FieldDefImpl.Constants.anyDef, Quantifier.ONE, true),
            new ExprType(FieldDefImpl.Constants.anyDef, Quantifier.QSTN, true),
            new ExprType(FieldDefImpl.Constants.anyDef, Quantifier.PLUS, true),
            new ExprType(FieldDefImpl.Constants.anyDef, Quantifier.STAR, true)
        },
        {
            new ExprType(FieldDefImpl.Constants.anyAtomicDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.anyAtomicDef, Quantifier.QSTN, true),
            new ExprType(FieldDefImpl.Constants.anyAtomicDef, Quantifier.PLUS, true),
            new ExprType(FieldDefImpl.Constants.anyAtomicDef, Quantifier.STAR, true)
        },
        {
            new ExprType(FieldDefImpl.Constants.anyJsonAtomicDef,
                         Quantifier.ONE, true),
            new ExprType(FieldDefImpl.Constants.anyJsonAtomicDef,
                         Quantifier.QSTN, true),
            new ExprType(FieldDefImpl.Constants.anyJsonAtomicDef,
                         Quantifier.PLUS, true),
            new ExprType(FieldDefImpl.Constants.anyJsonAtomicDef,
                         Quantifier.STAR, true)
        },
        {
            new ExprType(FieldDefImpl.Constants.jsonDef, Quantifier.ONE, true),
            new ExprType(FieldDefImpl.Constants.jsonDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.jsonDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.jsonDef, Quantifier.STAR,
                         true)
        },

        {
            new ExprType(FieldDefImpl.Constants.anyRecordDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.anyRecordDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.anyRecordDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.anyRecordDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.numberDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.numberDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.numberDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.numberDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.integerDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.integerDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.integerDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.integerDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.longDef, Quantifier.ONE, true),
            new ExprType(FieldDefImpl.Constants.longDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.longDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.longDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.floatDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.floatDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.floatDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.floatDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.doubleDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.doubleDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.doubleDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.doubleDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.stringDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.stringDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.stringDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.stringDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.booleanDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.booleanDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.booleanDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.booleanDef, Quantifier.STAR,
                         true)
        },
        {
            new ExprType(FieldDefImpl.Constants.binaryDef, Quantifier.ONE,
                         true),
            new ExprType(FieldDefImpl.Constants.binaryDef, Quantifier.QSTN,
                         true),
            new ExprType(FieldDefImpl.Constants.binaryDef, Quantifier.PLUS,
                         true),
            new ExprType(FieldDefImpl.Constants.binaryDef, Quantifier.STAR,
                         true)
        },
        { /* Reserved for FIX_BINARY */ },
        { /* Reserved for ENUM */ },
        {
            TIMESTAMP_TYPES_ONE[TIMESTAMP_TYPES_ONE.length - 1],
            TIMESTAMP_TYPES_QSTN[TIMESTAMP_TYPES_QSTN.length - 1],
            new ExprType(FieldDefImpl.Constants.timestampDefs[9],
                         Quantifier.PLUS, false),
            new ExprType(FieldDefImpl.Constants.timestampDefs[9],
                         Quantifier.STAR, false)
        },
    };

    public static ExprType EMPTY() {
        return BUILTIN_TYPES[TypeCode.EMPTY.ordinal()][0];
    }

    public static ExprType ANY_ONE() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType ANY_QSTN() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType ANY_PLUS() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType ANY_STAR() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType ANY_ATOMIC_ONE() {
        return BUILTIN_TYPES[TypeCode.ANY_ATOMIC.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType ANY_ATOMIC_QSTN() {
        return BUILTIN_TYPES[TypeCode.ANY_ATOMIC.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType ANY_ATOMIC_PLUS() {
        return BUILTIN_TYPES[TypeCode.ANY_ATOMIC.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType ANY_ATOMIC_STAR() {
        return BUILTIN_TYPES[TypeCode.ANY_ATOMIC.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType ANY_JATOMIC_ONE() {
        return BUILTIN_TYPES[TypeCode.ANY_JSON_ATOMIC.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType ANY_JATOMIC_QSTN() {
        return BUILTIN_TYPES[TypeCode.ANY_JSON_ATOMIC.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType ANY_JATOMIC_PLUS() {
        return BUILTIN_TYPES[TypeCode.ANY_JSON_ATOMIC.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType ANY_JATOMIC_STAR() {
        return BUILTIN_TYPES[TypeCode.ANY_JSON_ATOMIC.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType JSON_ONE() {
        return BUILTIN_TYPES[TypeCode.JSON.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType JSON_QSTN() {
        return BUILTIN_TYPES[TypeCode.JSON.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType JSON_PLUS() {
        return BUILTIN_TYPES[TypeCode.JSON.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType JSON_STAR() {
        return BUILTIN_TYPES[TypeCode.JSON.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType ANY_RECORD_ONE() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType ANY_RECORD_QSTN() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType ANY_RECORD_PLUS() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType ANY_RECORD_STAR() {
        return BUILTIN_TYPES[TypeCode.ANY.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType NUMBER_ONE() {
        return BUILTIN_TYPES[TypeCode.NUMBER.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType NUMBER_QSTN() {
        return BUILTIN_TYPES[TypeCode.NUMBER.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType NUMBER_PLUS() {
        return BUILTIN_TYPES[TypeCode.NUMBER.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType NUMBER_STAR() {
        return BUILTIN_TYPES[TypeCode.NUMBER.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType INT_ONE() {
        return BUILTIN_TYPES[TypeCode.INT.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType INT_QSTN() {
        return BUILTIN_TYPES[TypeCode.INT.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType INT_PLUS() {
        return BUILTIN_TYPES[TypeCode.INT.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType INT_STAR() {
        return BUILTIN_TYPES[TypeCode.INT.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType LONG_ONE() {
        return BUILTIN_TYPES[TypeCode.LONG.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType LONG_QSTN() {
        return BUILTIN_TYPES[TypeCode.LONG.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType LONG_PLUS() {
        return BUILTIN_TYPES[TypeCode.LONG.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType LONG_STAR() {
        return BUILTIN_TYPES[TypeCode.LONG.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType FLOAT_ONE() {
        return BUILTIN_TYPES[TypeCode.FLOAT.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType FLOAT_QSTN() {
        return BUILTIN_TYPES[TypeCode.FLOAT.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType FLOAT_PLUS() {
        return BUILTIN_TYPES[TypeCode.FLOAT.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType FLOAT_STAR() {
        return BUILTIN_TYPES[TypeCode.FLOAT.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType DOUBLE_ONE() {
        return BUILTIN_TYPES[TypeCode.DOUBLE.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType DOUBLE_QSTN() {
        return BUILTIN_TYPES[TypeCode.DOUBLE.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType DOUBLE_PLUS() {
        return BUILTIN_TYPES[TypeCode.DOUBLE.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType DOUBLE_STAR() {
        return BUILTIN_TYPES[TypeCode.DOUBLE.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType STRING_ONE() {
        return BUILTIN_TYPES[TypeCode.STRING.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType STRING_QSTN() {
        return BUILTIN_TYPES[TypeCode.STRING.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType STRING_PLUS() {
        return BUILTIN_TYPES[TypeCode.STRING.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType STRING_STAR() {
        return BUILTIN_TYPES[TypeCode.STRING.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType BOOLEAN_ONE() {
        return BUILTIN_TYPES[TypeCode.BOOLEAN.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType BOOLEAN_QSTN() {
        return BUILTIN_TYPES[TypeCode.BOOLEAN.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType BOOLEAN_PLUS() {
        return BUILTIN_TYPES[TypeCode.BOOLEAN.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType BOOLEAN_STAR() {
        return BUILTIN_TYPES[TypeCode.BOOLEAN.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType BINARY_ONE() {
        return BUILTIN_TYPES[TypeCode.BINARY.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType BINARY_QSTN() {
        return BUILTIN_TYPES[TypeCode.BINARY.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType BINARY_PLUS() {
        return BUILTIN_TYPES[TypeCode.BINARY.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType BINARY_STAR() {
        return BUILTIN_TYPES[TypeCode.BINARY.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType TIMESTAMP_ONE() {
        return BUILTIN_TYPES[TypeCode.TIMESTAMP.ordinal()]
                            [Quantifier.ONE.ordinal()];
    }

    public static ExprType TIMESTAMP_ONE(int precision) {
        return TIMESTAMP_TYPES_ONE[precision];
    }

    public static ExprType TIMESTAMP_QSTN() {
        return BUILTIN_TYPES[TypeCode.TIMESTAMP.ordinal()]
                            [Quantifier.QSTN.ordinal()];
    }

    public static ExprType TIMESTAMP_QSTN(int precision) {
        return TIMESTAMP_TYPES_QSTN[precision];
    }

    public static ExprType TIMESTAMP_PLUS() {
        return BUILTIN_TYPES[TypeCode.TIMESTAMP.ordinal()]
                            [Quantifier.PLUS.ordinal()];
    }

    public static ExprType TIMESTAMP_STAR() {
        return BUILTIN_TYPES[TypeCode.TIMESTAMP.ordinal()]
                            [Quantifier.STAR.ordinal()];
    }

    public static ExprType ANY_ARRAY_STAR =
        createArrayType(TypeManager.ANY_ONE(), Quantifier.STAR);

    public static ExprType ANY_ARRAY_ONE =
        createArrayType(TypeManager.ANY_ONE(), Quantifier.ONE);

    TypeManager() {
    }

    /*
     * Return the quant that is the intersection of the 2 given quants.
     */
    public static Quantifier getIntersectionQuant(Quantifier q1, Quantifier q2) {
        return QUANT_INTERS_MATRIX[q1.ordinal()][q2.ordinal()];
    }

    /*
     * Return the quant that is the union of the 2 given quants.
     */
    public static Quantifier getUnionQuant(Quantifier q1, Quantifier q2) {
        return QUANT_UNION_MATRIX[q1.ordinal()][q2.ordinal()];
    }

    /*
     * Return the quant that is the concatenation of the 2 given quants.
     */
    public static Quantifier getConcatQuant(Quantifier q1, Quantifier q2) {
        return QUANT_CONCAT_MATRIX[q1.ordinal()][q2.ordinal()];
    }

    public static boolean isSubQuant(Quantifier sub, Quantifier sup) {
        return QUANT_SUBTYPE_MATRIX[sub.ordinal()][sup.ordinal()];
    }

    static boolean isSubTypeCode(TypeCode sub, TypeCode sup) {
        return SUBTYPE_MATRIX[sub.ordinal()][sup.ordinal()];
    }

    /*
     * Check whether 2 types intersect. If false is returned, it is certain
     * that the types do not intersect. If true is returned, the types may
     * or may not intersect.
     */
    public static boolean typesIntersect(ExprType t1, ExprType t2) {

        TypeCode c1 = t1.getCode();
        TypeCode c2 = t2.getCode();
        Quantifier q1 = t1.getQuantifier();
        Quantifier q2 = t2.getQuantifier();

        if ((q1 == Quantifier.QSTN || q1 == Quantifier.STAR) &&
            (q2 == Quantifier.QSTN || q2 == Quantifier.STAR)) {
            return true;
        }

        return INTERSECT_MATRIX[c1.ordinal()][c2.ordinal()];
    }

    static boolean areTypeCodesComparable(ExprType t1, ExprType t2) {

        return COMPARISON_MATRIX[t1.getCode().ordinal()]
                                [t2.getCode().ordinal()];
    }

    public static boolean areTypesComparable(ExprType t1, ExprType t2) {
        return areTypeCodesComparable(t1, t2);
    }

    public static boolean areTypesComparable(FieldDefImpl t1, FieldDefImpl t2) {

        TypeCode tc1 = ExprType.getTypeCodeForDefCode(t1.getType());
        TypeCode tc2 = ExprType.getTypeCodeForDefCode(t2.getType());

        return COMPARISON_MATRIX[tc1.ordinal()][tc2.ordinal()];
    }

    /*
     * Create a type T that is the union of the given 2 types.
     * T is a type that includes every instance of both t1 and t2,
     * including the empty sequence if it is included by either t1
     * or t2. Because we don't support union types, T will, in general,
     * also include instances that are neither in T1 nor in T2.
     */
    public static ExprType getUnionType(ExprType t1, ExprType t2) {

        Quantifier q = getUnionQuant(t1.getQuantifier(), t2.getQuantifier());
        return getUnionOrConcatType(t1, t2, q);
    }

    /*
     * Create a type T that is the concatenation of the given 2 types.
     * T is a type that represents the result of concatenating two sequences
     * of types t1 and t2 respectively. Because we don't support union types,
     * T will, in general, also include instances that are neither in T1 nor
     * in T2.
     */
    public static ExprType getConcatType(ExprType t1, ExprType t2) {

        Quantifier q = getConcatQuant(t1.getQuantifier(), t2.getQuantifier());
        return getUnionOrConcatType(t1, t2, q);
    }

    private static ExprType getUnionOrConcatType(
        ExprType t1,
        ExprType t2,
        Quantifier q) {

        FieldDefImpl def1 = t1.getDef();
        FieldDefImpl def2 = t2.getDef();
        FieldDefImpl union;

        if (def1.isEmpty()) {
            union = def2;
        } else if (def2.isEmpty()) {
            union = def1;
        } else {
            union = def1.getUnionType(def2);
        }

        return createType(union, q);
    }

    /*
     * Create an ExprType with a given quantifier and the same ItemType as the
     * one of a given type.
     */
    public static ExprType createType(ExprType t, Quantifier q) {

        if (t.getQuantifier() == q) {
            return t;
        }

        if (t.isBuiltin()) {
            return getBuiltinType(t.getCode(), q);
        }

        ExprType res = new ExprType(t);
        res.setQuant(q);
        return res;
    }

    /*
     * Create an ExprType out of a given FieldDef and quantifier.
     */
    public static ExprType createType(FieldDefImpl t, Quantifier q) {

        ExprType type = getBuiltinType(t, q);

        if (type == null) {
            type = new ExprType(t, q, false);
        }

        return type;
    }

    /*
     * Create a reord type to describe the columns of a table
     */
    public static ExprType createTableRecordType(
        TableImpl table,
        Quantifier q) {

        RecordDefImpl rowType = table.getRowDef();

        return new ExprType(rowType, q, false);
    }

    public static ExprType createArrayType(
        ExprType elemType,
        Quantifier q) {

        ArrayDefImpl typeDef = FieldDefFactory.createArrayDef(
            elemType.getDef());

        return createType(typeDef, q);
    }

    public static ExprType createMapType(
        ExprType elemType,
        Quantifier q) {

        MapDefImpl typeDef = FieldDefFactory.createMapDef(elemType.getDef());
        return createType(typeDef, q);
    }

    /*
     * Create an expr type based on the type of a value.
     */
    public static ExprType createValueType(FieldValueImpl value) {

        if (value.isNull()) {
            throw new QueryStateException(
                "Null values do not have types!: ");
        }

        if (value.isJsonNull()) {
            return JSON_ONE();
        }

        FieldDefImpl type;

        switch (value.getType()) {
        case INTEGER:
            return INT_ONE();
        case LONG:
            return LONG_ONE();
        case FLOAT:
            return FLOAT_ONE();
        case DOUBLE:
            return DOUBLE_ONE();
        case NUMBER:
            return NUMBER_ONE();
        case STRING:
            return STRING_ONE();
        case BOOLEAN:
            return BOOLEAN_ONE();
        case BINARY:
            return BINARY_ONE();
        case FIXED_BINARY:
        case ENUM:
        case TIMESTAMP:
        case RECORD:
        case ARRAY:
        case MAP:
            type = value.getDefinition();
            return new ExprType(type, false);
        case EMPTY:
            return EMPTY();
        default:
            throw new QueryStateException(
                "Unexpected value kind: " + value.getType());
        }
    }

    /*
     * Get one of the builtin types.
     * This is public so that PlanIter can access it for deserialization of
     * ExprType.
     */
    public static ExprType getBuiltinType(TypeCode t, Quantifier q) {

        assert(ExprType.isBuiltin(t));
        if (t == TypeCode.EMPTY) {
            return EMPTY();
        }

        return BUILTIN_TYPES[t.ordinal()][q.ordinal()];
    }

    /*
     * Get one of the builtin types, or null if the given typedef does not
     * correspond to a builtin type.
     */
    public static ExprType getBuiltinType(FieldDefImpl t, Quantifier q) {

        switch (t.getType()) {

        case EMPTY:
            return EMPTY();

        case ANY:
        case ANY_ATOMIC:
        case ANY_JSON_ATOMIC:
        case JSON:
        case ANY_RECORD:
            return getBuiltinType(
                ExprType.getTypeCodeForDefCode(t.getType()), q);

        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case NUMBER:
        case STRING:
        case BOOLEAN:
        case BINARY:
            if (!t.hasMin() && !t.hasMax()) {
                return getBuiltinType(
                    ExprType.getTypeCodeForDefCode(t.getType()), q);
            }

            return null;

        case ENUM:
        case TIMESTAMP:
        case FIXED_BINARY:
        case RECORD:
        case ARRAY:
        case MAP:
            return null;
        default:
            throw new QueryStateException(
                "Unexpected type: " + t.getType());
        }
    }

    /**
     * Promote the given value to a value of the given type, if possible.
     * Return null if promotion is not possible. If the value is already
     * a member of the given type, return the value itself.
     */
    public static FieldValueImpl promote(FieldValueImpl value, ExprType type) {

        if (type.containsValue(value)) {
            return value;
        }

        FieldDef.Type valueCode = value.getType();
        TypeCode typeCode = type.getCode();

        switch (valueCode) {
        case INTEGER:
            if (typeCode == TypeCode.FLOAT) {
                return castIntToFloat(value, type);
            } else if (typeCode == TypeCode.DOUBLE) {
                return castIntToDouble(value, type);
            }
            break;
        case LONG:
            if (typeCode == TypeCode.FLOAT) {
                return castLongToFloat(value, type);
            } else if (typeCode == TypeCode.DOUBLE) {
                return castLongToDouble(value, type);
            }
            break;
        case STRING:
            if (typeCode == TypeCode.ENUM) {
                return castStringToEnum(value, type);
            }
            break;
        default:
            break;
        }

        return null;
    }

    /**
     * Cast an integer value to a float. Return null if the cast is not
     * possible, which is the case when the target type is a FloatRange
     * and the input value (after being cast to float) is not within the
     * allowed range.
     */
    private static FieldValueImpl castIntToFloat(
        FieldValueImpl value,
        ExprType type) {

        assert(type.getDef() != null);
        int baseValue = value.getInt();

        try {
            return FieldDefImpl.Constants.floatDef.createFloat(baseValue);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Cast an integer value to a double. Return null if the cast is not
     * possible, which is the case when the target type is a DoubleRange
     * and the input value (after being cast to double) is not within the
     * allowed range.
     */
    private static FieldValueImpl castIntToDouble(
        FieldValueImpl value,
        ExprType type) {

        assert(type.getDef() != null);
        int baseValue = value.getInt();

        try {
            return FieldDefImpl.Constants.doubleDef.createDouble(baseValue);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Cast a long value to a double. Return null if the cast is not
     * possible, which is the case when the target type is a FloatRange
     * and the input value (after being cast to float) is not within the
     * allowed range.
     */
    private static FieldValueImpl castLongToFloat(
        FieldValueImpl value,
        ExprType type) {

        assert(type.getDef() != null);
        long baseValue = value.getLong();

        try {
            return FieldDefImpl.Constants.floatDef.createFloat(baseValue);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Cast a long value to a double. Return null if the cast is not
     * possible, which is the case when the target type is a DoubleRange
     * and the input value (after being cast to double) is not within the
     * allowed range.
     */
    private static FieldValueImpl castLongToDouble(
        FieldValueImpl value,
        ExprType type) {

        assert(type.getDef() != null);
        long baseValue = value.getLong();

        try {
            return FieldDefImpl.Constants.doubleDef.createDouble(baseValue);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Cast a string value to an Enum. Return null if the cast is not
     * possible, which is the case when the given string value is not
     * string-equal to any of the tokens in the target type.
     */
    private static FieldValueImpl castStringToEnum(
        FieldValueImpl value,
        ExprType type) {

        assert(type.getDef() != null);
        EnumDefImpl typeDef = (EnumDefImpl)(type.getDef());
        String baseValue = value.getString();

        try {
            return typeDef.createEnum(baseValue);
        } catch (Exception e) {
            return null;
        }
    }

    public static void serializeExprType(
        ExprType type,
        DataOutput out,
        short serialVersion) throws IOException {

        out.writeByte(type.getQuantifier().ordinal());
        FieldDefSerialization.writeFieldDef(type.getDef(), out, serialVersion);
    }

    /**
     * Read the type and quantifier and builtin boolean. If this is a builtin
     * type, get the (static) type directly from TypeManager.
     */
    public static ExprType deserializeExprType(
        DataInput in,
        short serialVersion) throws IOException {

        int ordinal = in.readByte();
        ExprType.Quantifier quant = ExprType.Quantifier.valueOf(ordinal);

        FieldDefImpl def =
            FieldDefSerialization.readFieldDef(in, serialVersion);

        return createType(def, quant);
    }
}
