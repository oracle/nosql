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

package oracle.kv.impl.admin;

import java.io.Serializable;
import java.util.List;

import oracle.kv.impl.admin.VerifyConfiguration.Problem;

/**
 * Return progress information and results from a verification run.
 */
public class VerifyResults implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Problem> violations;
    private final List<Problem> warnings;
    private final String progressReport;

    public VerifyResults(String progressReport,
                         List<Problem> violations,
                         List<Problem> warnings) {
        this.progressReport = progressReport;
        this.violations = violations;
        this.warnings = warnings;
    }

    VerifyResults(List<Problem> violations,
                  List<Problem> warnings) {
        this.progressReport = null;
        this.violations = violations;
        this.warnings = warnings;
    }

    public int numWarnings() {
        return warnings.size();
    }

    public List<Problem> getViolations() {
        return violations;
    }

    public int numViolations() {
        return violations.size();
    }

    public List<Problem> getWarnings() {
        return warnings;
    }

    public boolean okay() {
        return (violations.size() == 0) && (warnings.size() == 0);
    }

    public String display() {
        return progressReport;
    }
}
