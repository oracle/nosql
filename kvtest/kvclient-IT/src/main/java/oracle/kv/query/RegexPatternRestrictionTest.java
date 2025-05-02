/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.query;

import static org.junit.Assert.assertTrue;

import oracle.kv.StaticClientTestBase;
import oracle.kv.impl.query.compiler.FuncRegexLike;

import org.junit.Test;

/**
 *  Tests the validation of the regex_like pattern string. 
 *
 */
public class RegexPatternRestrictionTest extends StaticClientTestBase {

    @Test 
    public void testPatternRestriction() throws Exception {
        FuncRegexLike.verifyPattern("WayToGo .*");
        FuncRegexLike.verifyPattern(".* WayToGo .*");
        FuncRegexLike.verifyPattern(".WayToGo.");
        FuncRegexLike.verifyPattern("WayToGo \\ .*");
        FuncRegexLike.verifyPattern("Way.*ToGo");
        FuncRegexLike.verifyPattern("WayToGo \\\\ .*");
        FuncRegexLike.verifyPattern("WayToGo \\u1234 .*");
        FuncRegexLike.verifyPattern(".Way.ToGo.");
        FuncRegexLike.verifyPattern("\\(ava\\)");
        FuncRegexLike.verifyPattern("\\tava.*");
        FuncRegexLike.verifyPattern(".*ava\\n");
        FuncRegexLike.verifyPattern("\\x{1234}");
        FuncRegexLike.verifyPattern("ava\\Qava?\\E.*");
        FuncRegexLike.verifyPattern("ava\\Qava?");
        FuncRegexLike.verifyPattern("ava*");
        FuncRegexLike.verifyPattern("!=<>-:");
        
        verifyNegative("ava?");
        verifyNegative("ava+");
        verifyNegative("ava{3}");
        verifyNegative("ava*?");
        verifyNegative("ava|avb");
        verifyNegative("\\p{Print}");
        verifyNegative("\\d");
        verifyNegative("\\W");
        verifyNegative("WayToGo .?");
        verifyNegative(" (ava)");
        verifyNegative("(a|aa)+");
        verifyNegative("[av].*");
        verifyNegative("^ava");
        verifyNegative("ava$");
        
    }
    
    private void verifyNegative(String pattern) {
        boolean gotException = false;
        try {
            FuncRegexLike.verifyPattern(pattern);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue(gotException);
    }
    
}