/*-
 * Copyright (C) 2002, 2024, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.dbi;

import java.util.Map;

/*
 * As -1 to -255 are databaseid's which are reserved, this class maintain the 
 * static mappings of reserved databases. Assumes that what ever reserved
 * database we will add , is also added as a enum in DbType
 */
public class ReservedDBMap {

	private static final Map<String, Integer> reservedMap = Map.ofEntries(
			Map.entry(DbType.BEFORE_IMAGE.getInternalName(), -1));
	
	/*cannot instantiate */
	private ReservedDBMap() {
		
	}
	
	public static Integer getValue(String name) {
		return reservedMap.get(name);
	}
}
