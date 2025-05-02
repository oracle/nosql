
/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;

public interface QTFactory {

    QTSuite createQTSuite(QTOptions opts, File configFile);

    QTRun createQTRun();

    QTCase createQTCase();
}
