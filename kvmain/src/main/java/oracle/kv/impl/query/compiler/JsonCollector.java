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

import oracle.kv.impl.query.compiler.parser.KVQLParser.ArrayOfJsonValuesContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.EmptyJsonArrayContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.EmptyJsonObjectContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.JsonArrayValueContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.JsonAtomContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.Json_textContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.JsonObjectContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.JsonObjectValueContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.JsonPairContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.JspairContext;
import oracle.kv.impl.query.compiler.parser.KVQLParser.JsvalueContext;

import org.antlr.v4.runtime.tree.ParseTreeProperty;

/*
 * The JsonCollector is essentially an identity map from parse tree nodes to
 * strings.  After the tree has been walked, the root of every json tree should
 * be associated in this map with a string that is equivalent to the original
 * JSON fragment.
 */
public class JsonCollector extends ParseTreeProperty<String> {

    public JsonCollector() {
    }

    public void exitJsonAtom(JsonAtomContext ctx) {
        put(ctx, ctx.getText());
    }

    public void exitJsonObjectValue(JsonObjectValueContext ctx) {
        put(ctx, get(ctx.jsobject()));
    }

    public void exitJsonArrayValue(JsonArrayValueContext ctx) {
        put(ctx, get(ctx.jsarray()));
    }

    public void exitJsonPair(JsonPairContext ctx) {
        String tag = ctx.DSTRING().getText();
        JsvalueContext valuectx = ctx.jsvalue();
        String x = String.format("%s : %s", tag, get(valuectx));
        put(ctx, x);
    }

    public void exitArrayOfJsonValues(ArrayOfJsonValuesContext ctx) {
        StringBuilder s = new StringBuilder();
        s.append("[");
        for (JsvalueContext valuectx : ctx.jsvalue()) {
            s.append(get(valuectx)).append(", ");
        }
        dropFinalComma(s);
        s.append("]");
        put(ctx, s.toString());
    }

    public void exitEmptyJsonArray(EmptyJsonArrayContext ctx) {
        put(ctx, "[]");
    }

    public void exitJsonObject(JsonObjectContext ctx) {
        StringBuilder s = new StringBuilder();
        s.append("{");
        for (JspairContext pairctx : ctx.jspair()) {
            s.append(get(pairctx)).append(", ");
        }
        dropFinalComma(s);
        s.append("}");
        put(ctx, s.toString());
    }

    public void exitEmptyJsonObject(EmptyJsonObjectContext ctx) {
        put(ctx, "{}");
    }

    public void exitJson_text(Json_textContext ctx) {
        put(ctx, get(ctx.getChild(0)));
    }

    /*
     * If the final char of s is ',' or if the final 2 chars are ", " then
     * delete them.
     */
    private static void dropFinalComma(StringBuilder s) {
        final int length = s.length();
        if (length >= 1 && s.charAt(length - 1) == ',') {
            s.delete(s.length() - 1, s.length());
        } else if (length >= 2 && s.charAt(length - 1) == ' '
                   && s.charAt(length - 2) == ',') {
            s.delete(s.length() - 2, s.length());
        }
    }

}
