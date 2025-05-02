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

import oracle.kv.impl.query.QueryException.Location;

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
public interface Geometry {

    double defaultTolerance();

    boolean isPolygon(); 

    String toGeoJson();

    double area(Location loc);

    Geometry buffer(double distance, Location loc);

    double distance(Geometry g, Location loc);

    boolean interact(Geometry g, Location loc);

    boolean inside(Geometry g, Location loc);
}
