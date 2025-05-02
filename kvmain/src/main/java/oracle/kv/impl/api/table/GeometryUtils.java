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

package oracle.kv.impl.api.table;

import java.util.List;

import oracle.kv.query.ExecuteOptions;
import oracle.kv.impl.util.Pair;

/**
 * The Geometry and GeometryUtils intefaces are implemented by the GeometryImpl
 * and GeometryUtilsImpl classes, respectively. These interfaces are needed
 * because GeometryImpl and GeometryUtilsImpl make calls to the SDO external
 * jars, which are available in the Enterprise Edition only. So for the Community
 * Edition to compile successfully, GeometryUtilsImpl.java and GeometryImpl.java
 * are not included in the CE source-code files. As a result the rest of the
 * code should not import any SDO classes directly, but instead import the
 * Geometry and GeometryUtils interfaces. 
 *
 * For EE, a single instance of GeometryUtilsImpl must be created. This is done
 * in CompilerAPI.getGeoUtils() by dynamically loadig the GeometryUtilsImpl class
 * and constructing an instance via Class.getDeclaredConstructor().newInstance();
 */
public interface GeometryUtils {

    /*
     * Various JGeometry methods have a "tolerance" param, which is a number of
     * meters. Points that are closer to each other than the given tolerance are
     * considered identical. In the code here we use a default tolerance of
     * 0.005m
     */
    public static final double theDefaultTolerance = 0.005;

    /*
     * The default max/min number of geocells to use for covering an MBR
     * during the indexing of a non-point geometry. These bounds are used
     * in computing the size (hash length) of the cells to use to cover the
     * MBR. The max is a string bound. The min is a "soft" bound: if for a
     * hashlen L the number of cells is less than the min, we will increase
     * L to get more cells, but if this results to more than max cells, we
     * will decrease L and allow less than min cells.
     * 
     * These default numbers can be overriden by numbers provided in the
     * CREATE INDEX statement.
     */
    public static final int theMaxCoveringCellsForIndex = 500;
    public static final int theMinCoveringCellsForIndex = 50;

    /*
     * Same as above, but for covering search MBRs.
     *
     * These default numbers can be overriden by seting corresponding fields
     * in ExecuteOptions.
     */
    public static final int theMaxCoveringCellsForSearch = 1000;
    public static final int theMinCoveringCellsForSearch = 100;

    /*
     * The default max number of index scan ranges to generate per search MBR.
     *
     * This default can be overriden by seting a corresponding field in
     * ExecuteOptions.
     */
    public static final int theMaxRanges = 300;

    /*
     * If the ratio of the area of a geometry over the area of its associated
     * MRB is <= theSplitRatio, then the MBR is split into a number of sub-
     * MBRs. Any of the sub-MBRs that do not intersect with the geometry are
     * pruned away. Each split of an MBR results in 4 child MRBs and the max 
     * number of split levels is given theMaxSplits.
     *
     * theSplitRatio and theMaxSplits apply to both a search geometry and a
     * geometry to index. These are actually the default values. They can be
     * overriden by use of the ExecuteOptions or the CREATE INDEX statement.
     */
    public static final double theSplitRatio = 0.2;
    public static final int theMaxSplits = 5;

    Geometry castAsGeometry(FieldValueImpl val);

    Geometry castAsGeometry(
        FieldValueImpl val,
        StringBuilder sb);

    Geometry castAsGeometry(String geojson);

    String hashPoint(FieldValueImpl val);
  
    List<String> hashGeometry(
        FieldValueImpl val,
        int maxCoveringCells,
        int minCoveringCells,
        int maxSplits,
        double splitRatio);

    List<Pair<String,String>> ranges(
        Geometry searchGeom,
        double distance,
        ExecuteOptions options);

    List<String> keys(List<Pair<String,String>> ranges);
}
