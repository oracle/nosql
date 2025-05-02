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

package oracle.kv.impl.query.compiler.parser;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import oracle.kv.impl.query.QueryException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * This class is responsible for parsing a DML or DDL statement (given as a
 * String or an InputStream) and generating an abstract syntax tree (AST).
 *
 * Parsing is done by calling one of the parse() methods on an instance of
 * this class. If the parsing is successful the KVSParser instance can provide
 * the generated AST via its getParseTree() method. If there was an error in
 * the parsing the KVSParser instance will return false from its succeeded()
 * method and the particular error can be retrieved via the getParseException()
 * method.
 *
 * In practice, KVParser is just a thin wrapper over the KVQLParser class that
 * is created by Antlr from a grammar file (KVQL.g).
 *
 * Usage warnings:
 * The syntax error messages are currently fairly
 * cryptic. oracle.kv.shell.ExecuteCmd implements a getUsage() method which
 * attempts to augment those messages. This should be moved into TableDdl.
 */
public class KVParser {

    private QueryException parseException = null;

    private ParseTree tree = null;

    public KVParser() {
    }

    public ParseTree getParseTree() {
        return tree;
    }

    public QueryException getParseException() {
        return parseException;
    }

    public void setParseException(QueryException de) {
        parseException = de;
    }

    public boolean succeeded() {
        return parseException == null;
    }

    /**
     * Parse input from stdin, ending with EOF (^D)
     */
    public void parse() throws QueryException {

        parse(System.in);
    }

    /**
     * Parse String input.
     */
    public void parse(String input) throws QueryException {

        CharStream antlrStream = CharStreams.fromString(input);

        parse(antlrStream);
    }

    /**
     * Parse char array input.
     */
    public void parse(char[] input) throws QueryException {

        CharArrayReader reader = null;
        try {
            reader = new CharArrayReader(input);
            CharStream antlrStream = CharStreams.fromReader(reader);
            parse(antlrStream);
        } catch (IOException ioe) {
            throw new QueryException(ioe);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    /**
     * Parses from an InputStream, ending in EOF (^D)
     */
    public void parse(InputStream input) throws QueryException {

        try {
            CharStream antlrStream = CharStreams.fromStream(input);

            parse(antlrStream);

        } catch (IOException ioe) {
            throw new QueryException(ioe);
        }
    }

    /**
     * Drive the parsing.
     * TODO: look at using the BailErrorStrategy and setSLL(true) to do faster
     * parsing with a bailout.
     */
    private void parse(CharStream input) throws QueryException {

        /* creates a buffer of tokens pulled from the lexer */
        KVQLLexer lexer = new KVQLLexer(input);

        /* create a parser that feeds off the tokens buffer */
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        KVQLParser parser = new KVQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new DdlErrorListener());

        try {
            tree = parser.parse();
        } catch (RecognitionException re) {
            parseException = new QueryException(re);
        } catch (QueryException de) {
            parseException = de;
        }
    }

    /**
     * Handle parser errors.  Consider additional interface methods and
     * some custom notifications (in the grammar) for common errors.  See
     * p. 170 in the book for examples.
     */
    private static class DdlErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String msg,
            RecognitionException re) {

            List<String> stack = ((Parser) recognizer).getRuleInvocationStack();
            Collections.reverse(stack);

            String errorMsg = msg + ", at line " + line + ":" +
                charPositionInLine + // " at " + offendingSymbol +
                "\nrule stack: " + stack;

            throw new QueryException(errorMsg,
                                           new QueryException.Location(
                                               line,
                                               charPositionInLine,
                                               line,
                                               charPositionInLine));
        }
    }
}
