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

package oracle.kv.impl.diagnostic;

/**
 * Encapsulates a definition and mechanism for verifying arguments or 
 * parameters of NoSQL are valid or not against environment and requirement. 
 * Subclasses of DiagnosticVerifier will define the different types of 
 * DiagnosticVerifier that can be carried out.
 */

public abstract class DiagnosticVerifier {
    
    /* Return immediately when get error message */
    protected boolean returnOnError;
    
    /**
     * Work of verification is done in this method
     */
    public abstract boolean doWork();
    
    public DiagnosticVerifier(boolean returnOnError) {
        this.returnOnError = returnOnError;
    }

    /**
     * Print out error message
     * 
     * @param errorMsg 
     */
    public void printMessage(String errorMsg) {
         if (errorMsg != null) {
             System.err.println(errorMsg);
         }
    }
    
    /**
     * Do verification and return its result
     * 
     * @return result of verification
     */
    public boolean verify() {
        return doWork();
    }
}
