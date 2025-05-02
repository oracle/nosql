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

import java.util.logging.Logger;
import java.lang.reflect.InvocationTargetException;

import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.impl.api.query.PreparedDdlStatementImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.table.GeometryUtils;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PreparedStatement;

/*
 * This class drives the compilation of a KVSQL program.
 */
public class CompilerAPI {

    static StaticContext theRootSctx = new StaticContext(null/*parent*/);

    static volatile GeometryUtils theGeomUtils;

    static FunctionLib theFunctionLib = new FunctionLib(theRootSctx);

    public static GeometryUtils getGeoUtils() {

        if (theGeomUtils == null) {

            /*
             * Dynamically load the GeometryUtilsImpl class and create the
             * GeometryUtilsImpl instance. See javadoc in GeometryUtils interface
             * for more details.
             */
            Class<?> geomUtilsClass;

            try {
                geomUtilsClass = Class.forName(
                    "oracle.kv.impl.api.table.GeometryUtilsImpl");
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(
                    "Support for GeoJson data is not available in the " +
                    "Community Edition of Oracle NoSQL");
            }

            try {
                theGeomUtils = (GeometryUtils)geomUtilsClass.
                               getDeclaredConstructor().newInstance();
            } catch (InstantiationException ie) {
                throw new IllegalArgumentException(ie);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(iae);
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException(nsme);
            } catch (InvocationTargetException ite) {
                throw new IllegalArgumentException(ite);
            }
        }

        return theGeomUtils;
    }

    public static StaticContext getRootSctx() {
        return theRootSctx;
    }

    public static FunctionLib getFuncLib() {
        return theFunctionLib;
    }

    public static PreparedStatement prepare(
        TableAPIImpl tableAPI,
        char[] queryString,
        ExecuteOptions options) {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final String namespace = options.getNamespace();
        final PrepareCallback prepareCallback = options.getPrepareCallback();

        try {
            /* Create an sctx for the query as a child of the roo sctx */
            StaticContext querySctx = new StaticContext(theRootSctx);

            QueryControlBlock qcb = new QueryControlBlock(
                tableAPI, options, queryString, querySctx, namespace,
                prepareCallback);

            qcb.compile();

            if (qcb.succeeded()) {

                /* if the callback doesn't need the result, return */
                if (prepareCallback != null &&
                    !prepareCallback.prepareNeeded()) {
                    return null;
                }
                return new PreparedStatementImpl(
                    qcb.getQueryPlan(),
                    qcb.getResultDef(),
                    qcb.getNumRegs(),
                    qcb.getNumIterators(),
                    qcb.getInitSctx().getExternalVars(),
                    qcb);
            }

            //qcb.getException().printStackTrace();

            /*
             * QueryStateException is an internal error in the query compiler
             * or engine (probably a bug). It is a subclass of
             * IllegalStateException, so it can be passed on directly
             */
            if (qcb.getException() instanceof QueryStateException) {
                Logger logger = tableAPI.getStore().getLogger();
                if (logger != null) {
                    logger.warning(qcb.getException().toString());
                }
                throw qcb.getException();
            }

            /*
             * Pass FaultException directly. May be thrown if, for example,
             * there was a problem with accessing metadata about the tables
             * used in the query.
             */
            if (qcb.getException() instanceof FaultException) {
                throw qcb.getException();
            }

            /*
             * QueryException: semantic or syntactic error with the query.
             */
            if (qcb.getException() instanceof QueryException) {
                QueryException qe = (QueryException) qcb.getException();
                throw qe.getIllegalArgument();
            }

            /*
             * IllegalArgumentException -- rethrow
             */
            if (qcb.getException() instanceof IllegalArgumentException) {
                throw qcb.getException();
            }

            /*
             * Security exceptions while connecting to the store.
             */
            if (qcb.getException() instanceof KVSecurityException) {
                throw qcb.getException();
            }

            /*
             * Anything else translate into IllegalArgumentException
             */
            throw new IllegalArgumentException(qcb.getErrorMessage());

        } catch (DdlException ddle) {
            return new PreparedDdlStatementImpl(queryString, namespace,
                                                options);
        }
    }

    /**
     * Used by the admin to parse DDL statement.
     * The caller is responsible for determining success or failure by
     * calling QueryControlBlock.succeeded(). On failure there may be
     * an exception which can be obtained using
     * QueryControlBlock.getException().
     */
    public static QueryControlBlock compile(
        char[] queryString,
        ExecuteOptions options,
        TableMetadataHelper metadataHelper,
        StatementFactory statementFactory,
        String namespace,
        PrepareCallback prepareCallback) {

        /* Create an sctx for the query as a child of the roo sctx */
        StaticContext querySctx = new StaticContext(theRootSctx);

        QueryControlBlock qcb = new QueryControlBlock(
            metadataHelper, statementFactory, queryString, options, querySctx,
            namespace, prepareCallback);

        qcb.compile();

        return qcb;
    }
}
