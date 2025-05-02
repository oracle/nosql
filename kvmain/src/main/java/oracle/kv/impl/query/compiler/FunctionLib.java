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

package oracle.kv.impl.query.compiler;

import java.util.ArrayList;

import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * There is a single instance of the FunctionLib class, created during static
 * initialization as a static member of the CompilerAPI class. This single
 * instance  creates and stores the Function objs for all builtin functions.
 * It also registers these Function objs in the root static context.
 */
public class FunctionLib {

    /*
     * This enum defines a unique code for each builtin function.
     *
     * WARNING!!
     * ADD ALL NEW CODES AT THE END AND DO NOT REORDER CODES. This is because
     * function codes may appear in the query plan that gets send over the wire
     * from clients to servers.
     */
    public static enum FuncCode {

        OP_AND(0),
        OP_OR(1),

        OP_EQ(2),
        OP_NEQ(3),
        OP_GT(4),
        OP_GE(5),
        OP_LT(6),
        OP_LE(7),

        OP_EQ_ANY(8),
        OP_NEQ_ANY(9),
        OP_GT_ANY(10),
        OP_GE_ANY(11),
        OP_LT_ANY(12),
        OP_LE_ANY(13),

        OP_ADD_SUB(14),
        OP_MULT_DIV(15),
        OP_ARITH_NEGATE(16),

        FN_SEQ_CONCAT(17),

        OP_EXISTS(18),
        OP_NOT_EXISTS(19),

        OP_NOT(20),

        FN_SIZE(21),

        OP_IS_NULL(22),
        OP_IS_NOT_NULL(23),

        FN_YEAR(24),
        FN_MONTH(25),
        FN_DAY(26),
        FN_HOUR(27),
        FN_MINUTE(28),
        FN_SECOND(29),
        FN_MILLISECOND(30),
        FN_MICROSECOND(31),
        FN_NANOSECOND(32),
        FN_WEEK(33),
        FN_ISOWEEK(34),

        FN_CURRENT_TIME(35),
        FN_CURRENT_TIME_MILLIS(36),

        FN_EXPIRATION_TIME(37),
        FN_EXPIRATION_TIME_MILLIS(38),
        FN_REMAINING_HOURS(39),
        FN_REMAINING_DAYS(40),
        FN_VERSION(41),

        FN_COUNT_STAR(42),
        FN_COUNT(43),
        FN_COUNT_NUMBERS(44),
        FN_SUM(45),
        FN_AVG(46),
        FN_MIN(47),
        FN_MAX(48),

        FN_SEQ_COUNT(49),
        FN_SEQ_SUM(50),
        FN_SEQ_AVG(51),
        FN_SEQ_MIN(52),
        FN_SEQ_MAX(53),

        FN_GEO_INTERSECT(54),
        FN_GEO_INSIDE(55),
        FN_GEO_NEAR(56),
        FN_GEO_WITHIN_DISTANCE(57),
        FN_GEO_DISTANCE(58),
        FN_GEO_IS_GEOMETRY(59),

        FN_PARSE_JSON(60),

        FN_REGEX_LIKE(61),

        OP_CONCATENATE_STRINGS(62),
        FN_SUBSTRING(63),
        FN_UPPER(64),
        FN_LOWER(65),
        FN_TRIM(66),
        FN_LTRIM(67),
        FN_RTRIM(68),
        FN_LENGTH(69),
        FN_CONTAINS(70),
        FN_STARTS_WITH(71),
        FN_ENDS_WITH(72),
        FN_INDEX_OF(73),
        FN_REPLACE(74),
        FN_REVERSE(75),

        /* Internal versions of sequence aggregate functions. */
        FN_SEQ_COUNT_I(76),
        FN_SEQ_COUNT_NUMBERS_I(77),
        FN_SEQ_MIN_I(78),
        FN_SEQ_MAX_I(79),

        FN_MOD_TIME(80),
        FN_PARTITION(81),
        FN_SHARD(82),
        FN_ROW_STORAGE_SIZE(83),
        FN_INDEX_STORAGE_SIZE(84),
        FN_MKINDEX_STORAGE_SIZE(85),

        FN_UUID(86),

        FN_SEQ_DISTINCT(87),

        FN_TIMESTAMP_ADD(88),
        FN_TIMESTAMP_DIFF(89),
        FN_GET_DURATION(90),

        FN_ARRAY_COLLECT(91),
        // This is used only when (de)serializing a GroupIter for the cloud
        FN_ARRAY_COLLECT_DISTINCT(92),

        FN_SEQ_SORT(93),

        FN_MATH_ABS(94),
        FN_MATH_ACOS(95),
        FN_MATH_ASIN(96),
        FN_MATH_ATAN(97),
        FN_MATH_ATAN2(98),
        FN_MATH_CEIL(99),
        FN_MATH_COS(100),
        FN_MATH_COT(101),
        FN_MATH_DEGREES(102),
        FN_MATH_E(103),
        FN_MATH_EXP(104),
        FN_MATH_FLOOR(105),
        FN_MATH_LN(106),
        FN_MATH_LOG(107),
        FN_MATH_LOG10(108),
        FN_MATH_PI(109),
        FN_MATH_POWER(110),
        FN_MATH_RADIANS(111),
        FN_MATH_RAND(112),
        FN_MATH_ROUND(113),
        FN_MATH_SIGN(114),
        FN_MATH_SIN(115),
        FN_MATH_SQRT(116),
        FN_MATH_TAN(117),
        FN_MATH_TRUNC(118),

        FN_TIMESTAMP_CEIL(119),
        FN_TIMESTAMP_FLOOR(120),
        FN_TIMESTAMP_ROUND(121),
        FN_TIMESTAMP_TRUNC(122),

        FN_FORMAT_TIMESTAMP(123),
        FN_PARSE_TO_TIMESTAMP(124),
        FN_LAST_DAY_OF_MONTH(125),

		/* More function codes for extracting timestamp */
        FN_QUARTER(126),
        FN_DAY_OF_WEEK(127),
        FN_DAY_OF_MONTH(128),
        FN_DAY_OF_YEAR(129),
        FN_TIMESTAMP_BUCKET(130);

        private static final FuncCode[] VALUES = values();
        public static final int VALUES_COUNT = VALUES.length;

        FuncCode(int ord) {
            if (ord != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        public static FuncCode valueOf(int ordinal) {
            return VALUES[ordinal];
        }
    }

    ArrayList<Function> theFunctions;

    StaticContext theSctx;

    FunctionLib(StaticContext sctx) {

        theSctx = sctx;

        theFunctions = new ArrayList<Function>(128);

        theFunctions.add(new FuncAndOr(FuncCode.OP_AND, "AND"));
        theFunctions.add(new FuncAndOr(FuncCode.OP_OR, "OR"));

        theFunctions.add(new FuncCompOp(FuncCode.OP_EQ, "EQ"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_NEQ, "NEQ"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_GT, "GT"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_GE, "GE"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_LT, "LT"));
        theFunctions.add(new FuncCompOp(FuncCode.OP_LE, "LE"));

        theFunctions.add(new FuncAnyOp(FuncCode.OP_EQ_ANY, "EQ_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_NEQ_ANY, "NEQ_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_GT_ANY, "GT_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_GE_ANY, "GE_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_LT_ANY, "LT_ANY"));
        theFunctions.add(new FuncAnyOp(FuncCode.OP_LE_ANY, "LE_ANY"));

        theFunctions.add(new FuncArithOp(FuncCode.OP_ADD_SUB, "+-"));
        theFunctions.add(new FuncArithOp(FuncCode.OP_MULT_DIV, "*/"));
        theFunctions.add(new FuncArithUnaryOp());

        theFunctions.add(new FuncConcat());

        theFunctions.add(new FuncExists(FuncCode.OP_EXISTS, "EXISTS"));
        theFunctions.add(new FuncExists(FuncCode.OP_NOT_EXISTS, "NOT_EXISTS"));

        theFunctions.add(new FuncNot());

        theFunctions.add(new FuncSize());

        theFunctions.add(new FuncIsNull(FuncCode.OP_IS_NULL, "IS_NULL"));
        theFunctions.add(new FuncIsNull(FuncCode.OP_IS_NOT_NULL, "IS_NOT_NULL"));

        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_YEAR,
                                                      "year"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MONTH,
                                                      "month"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_DAY,
                                                      "day"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_HOUR,
                                                      "hour"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MINUTE,
                                                      "minute"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_SECOND,
                                                      "second"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MILLISECOND,
                                                      "millisecond"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_MICROSECOND,
                                                      "microsecond"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_NANOSECOND,
                                                      "nanosecond"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_WEEK,
                                                      "week"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_ISOWEEK,
                                                      "isoweek"));

        theFunctions.add(new FuncCurrentTime());
        theFunctions.add(new FuncCurrentTimeMillis());

        theFunctions.add(new FuncExpirationTime());
        theFunctions.add(new FuncExpirationTimeMillis());
        theFunctions.add(new FuncRemainingHours());
        theFunctions.add(new FuncRemainingDays());
        theFunctions.add(new FuncVersion("version"));

        theFunctions.add(new FuncCountStar());
        theFunctions.add(new FuncCount(FuncCode.FN_COUNT, "count"));
        theFunctions.add(new FuncCount(FuncCode.FN_COUNT_NUMBERS,
                                       "count_numbers"));
        theFunctions.add(new FuncSum());
        theFunctions.add(new FuncAvg());
        theFunctions.add(new FuncMinMax(FuncCode.FN_MIN, "min"));
        theFunctions.add(new FuncMinMax(FuncCode.FN_MAX, "max"));

        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_COUNT, "seq_count"));
        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_SUM, "seq_sum"));
        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_AVG, "seq_avg"));
        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_MIN, "seq_min"));
        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_MAX, "seq_max"));

        ArrayList<ExprType> geoTypes = new ArrayList<ExprType>(2);
        geoTypes.add(TypeManager.JSON_STAR());
        geoTypes.add(TypeManager.JSON_STAR());

        theFunctions.add(new FuncGeoSearch(FuncCode.FN_GEO_INTERSECT,
                                           "geo_intersect", geoTypes));
        theFunctions.add(new FuncGeoSearch(FuncCode.FN_GEO_INSIDE,
                                           "geo_inside", geoTypes));

        geoTypes = new ArrayList<ExprType>(3);
        geoTypes.add(TypeManager.ANY_STAR());
        geoTypes.add(TypeManager.ANY_STAR());
        geoTypes.add(TypeManager.DOUBLE_ONE());

        theFunctions.add(new FuncGeoSearch(FuncCode.FN_GEO_NEAR,
                                           "geo_near", geoTypes));

        theFunctions.add(new FuncGeoSearch(FuncCode.FN_GEO_WITHIN_DISTANCE,
                                           "geo_within_distance", geoTypes));

        theFunctions.add(new FuncGeoDistance());
        theFunctions.add(new FuncGeoIsGeometry());

        theFunctions.add(new FuncParseJson());

        /* String functions */
        theFunctions.add(FuncRegexLike.getFuncRegexLike());

        theFunctions.add(new FuncConcatenateStringsOp());
        theFunctions.add(new FuncSubstring());
        theFunctions.add(new FuncUpperLower(FuncCode.FN_UPPER, "upper"));
        theFunctions.add(new FuncUpperLower(FuncCode.FN_LOWER, "lower"));
        theFunctions.add(new FuncTrim());
        theFunctions.add(new FuncLRTrim(FuncCode.FN_LTRIM, "ltrim"));
        theFunctions.add(new FuncLRTrim(FuncCode.FN_RTRIM, "rtrim"));
        theFunctions.add(new FuncLength());
        theFunctions.add(new FuncContainsStartsEndsWith(FuncCode.FN_CONTAINS,
                                                        "contains"));
        theFunctions.add(new FuncContainsStartsEndsWith(FuncCode.FN_STARTS_WITH,
                                                        "starts_with"));
        theFunctions.add(new FuncContainsStartsEndsWith(FuncCode.FN_ENDS_WITH,
                                                        "ends_with"));
        theFunctions.add(new FuncIndexOf());
        theFunctions.add(new FuncReplace());
        theFunctions.add(new FuncReverse());

        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_COUNT_I,
                                         "seq_count_i"));
        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_COUNT_NUMBERS_I,
                                         "seq_count_numbers_i"));
        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_MIN_I, "seq_min_i"));
        theFunctions.add(new FuncSeqAggr(FuncCode.FN_SEQ_MAX_I, "seq_max_i"));

        theFunctions.add(new FuncModTime());
        theFunctions.add(new FuncPartition());
        theFunctions.add(new FuncShard());
        theFunctions.add(new FuncRowStorageSize());
        theFunctions.add(new FuncIndexStorageSize(
            FuncCode.FN_INDEX_STORAGE_SIZE, "index_storage_size"));
        theFunctions.add(new FuncIndexStorageSize(
            FuncCode.FN_MKINDEX_STORAGE_SIZE, "mkindex_storage_size"));

        theFunctions.add(new FuncUUID());

        theFunctions.add(new FuncSeqDistinct());

        theFunctions.add(new FuncTimestampAdd());
        theFunctions.add(new FuncTimestampDiff());
        theFunctions.add(new FuncGetDuration());

        theFunctions.add(new FuncCollect(false/*distinct*/));
        theFunctions.add(new FuncCollect(true));

        theFunctions.add(new FuncSeqSort());

        ArrayList<ExprType> mathTypes = new ArrayList<>();
        mathTypes.add(TypeManager.ANY_QSTN());
        mathTypes.add(TypeManager.ANY_QSTN());
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_ABS, "abs"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_ACOS, "acos"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_ASIN, "asin"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_ATAN, "atan"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_ATAN2, "atan2",
                mathTypes, false));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_CEIL, "ceil"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_COS, "cos"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_COT, "cot"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_DEGREES, "degrees"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_E, "e",
                TypeManager.ANY_JATOMIC_ONE()));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_EXP, "exp"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_FLOOR, "floor"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_LN, "ln"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_LOG, "log",
                mathTypes, false));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_LOG10, "log10"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_PI, "pi",
                TypeManager.ANY_JATOMIC_ONE())); /* retType */
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_POWER, "power",
                mathTypes, false));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_RADIANS, "radians"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_RAND, "rand",
                TypeManager.ANY_JATOMIC_ONE()));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_ROUND, "round",
                mathTypes, true));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_SIGN, "sign"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_SIN, "sin"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_SQRT, "sqrt"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_TAN, "tan"));
        theFunctions.add(new FuncMath(FuncCode.FN_MATH_TRUNC, "trunc",
                mathTypes, true));

        theFunctions.add(new FuncTimestampRound(FuncCode.FN_TIMESTAMP_CEIL,
                                                "timestamp_ceil"));
        theFunctions.add(new FuncTimestampRound(FuncCode.FN_TIMESTAMP_FLOOR,
                                                "timestamp_floor"));
        theFunctions.add(new FuncTimestampRound(FuncCode.FN_TIMESTAMP_ROUND,
                                                "timestamp_round"));
        theFunctions.add(new FuncTimestampRound(FuncCode.FN_TIMESTAMP_TRUNC,
                                                "timestamp_trunc"));

        theFunctions.add(new FuncFormatTimestamp());
        theFunctions.add(new FuncParseToTimestamp());
        theFunctions.add(new FuncToLastDayOfMonth());

        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_QUARTER,
                                                      "quarter"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_DAY_OF_WEEK,
                                                      "day_of_week"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_DAY_OF_MONTH,
                                                      "day_of_month"));
        theFunctions.add(new FuncExtractFromTimestamp(FuncCode.FN_DAY_OF_YEAR,
                                                      "day_of_year"));
        theFunctions.add(new FuncTimestampBucket(FuncCode.FN_TIMESTAMP_BUCKET,
                                                 "timestamp_bucket"));

        for (Function func : theFunctions) {
            sctx.addFunction(func);
        }

        sctx.addFunction(new FuncVersion("row_version"));
    }

    public Function getFunc(FuncCode c) {
        return theFunctions.get(c.ordinal());
    }

    public Function getFunc(String name, int arity) {
        return theSctx.findFunction(name, arity);
    }
}
