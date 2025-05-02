/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util;

import static oracle.nosql.util.http.HttpConstants.APPLICATION_JSON;
import static oracle.nosql.util.http.HttpConstants.AUTHORIZATION;
import static oracle.nosql.util.http.HttpConstants.CONTENT_TYPE;
import static oracle.nosql.util.http.HttpConstants.REQUEST_LOGCONTEXT_HEADER;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.RequestFaultException;

/**
 * A helper class using HttpURLConnection to issue HTTP request with retries
 * upon the status code and IOException thrown from each request execution.<p>
 *
 * By default, this utility retries a request in following cases:<p>
 * <li>Status code of HTTP response is larger than 500</li>
 * <li>Status code of HTTP response is 401, HTTP_UNAUTHORIZED error, if a
 * ConnectionHandler is given, it would first attempt using handler to renew
 * the credentials, if succeed then retry the request, otherwise, exit retry
 * loop and return.</li>
 * <li> IOException that are retryable errors, it will retry unless the
 * IOException is SSLException, UnknownHostException and
 * UnknownServiceException.</li><p>
 *
 * If the service that process the HTTP request has some special status codes
 * that could allow client to retry, override the <code>needRetry()</code>.
 * After getting a HTTP response or executions keep failing but exceed the retry
 * limit, this utility checks the final response and last cached IOException to
 * determine if the final response should be returned. By default, a HTTP
 * response will be returned directly without checking status code, it will
 * throw {@link RequestFaultException} for other failure cases. Override the
 * method <code>handleFinalResponse()</code> if need different handling. <p>
 *
 * There is also a overall retry switch that could be set when initialize this
 * class by setting retry as false. Or after instantiation, explicitly invoke
 * <code>disableRetry()</code><p>
 *
 * All the doHttpXXX methods in this utility are re-entrant.
 */
public class HttpRequest {

    /**
     * HTTP methods supported by this helper class.
     */
    public enum HttpMethod {
        GET,
        POST,
        PUT,
        DELETE
    }

    /* Default total retry wait time of each execution */
    private static final long DEFAULT_TOTAL_WAIT_MS = 10_000;

    /* Default retry interval of each execution */
    private static final long DEFAULT_RETRY_INTERVAL_MS = 1_000;

    /* Default connect timeout of each connection */
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 3_000;

    /* Default read timeout of each connection */
    private static final int DEFAULT_READ_TIMEOUT_MS = 3_000;

    /* Base64 encoder used to encode LogContext object */
    private static final Base64.Encoder encoder = Base64.getEncoder();

    /* Time of retry interval */
    private final long retryInter;

    /* Total retry wait time */
    private final long totalWait;

    /*
     * Timeout value, in milliseconds, to be used to set in
     * HttpURLConnection connect timeout.
     */
    private int connectTimeout;

    /*
     * Timeout value, in milliseconds, to be used to set in
     * HttpURLConnection read timeout.
     */
    private int readTimeout;

    /*
     * The overall switch to enable retry.
     */
    private boolean retry;

    /*
     * The flag to enable returning headers in the response, default is false.
     */
    private boolean retRespHeaders;

    /*
     * A set of headers to add to every request.
     */
    private final Map<String, String> standingHeaders =
        new ConcurrentHashMap<>();

    private boolean disableHostVerification = false;

    private HostnameVerifier allHostsValid = new HostnameVerifier() {
        @Override
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    /**
     * Initialize this class with default retry interval, total wait, and
     * use default connect and read timeout for each connection used to issue
     * HTTP request.
     */
    public HttpRequest() {
        this.retryInter = DEFAULT_RETRY_INTERVAL_MS;
        this.totalWait = DEFAULT_TOTAL_WAIT_MS;
        this.connectTimeout = DEFAULT_CONNECT_TIMEOUT_MS;
        this.readTimeout = DEFAULT_READ_TIMEOUT_MS;
        this.retry = true;
        this.retRespHeaders = false;
    }

    public HttpRequest(long retryInterval,
                       long totalWait,
                       int connectTimeout,
                       int readTimeout,
                       boolean retry) {
        this.retryInter = retryInterval;
        this.totalWait = totalWait;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.retry = retry;
        this.retRespHeaders = false;
    }

    /**
     * Disable retries while issuing HTTP requests.
     * @return this
     */
    public HttpRequest disableRetry() {
        this.retry = false;
        return this;
    }

    /**
     * Set connect and read timeout for each connection.
     *
     * @param connectTimeout timeout value, in milliseconds, to be used to set
     * in HttpURLConnection connect timeout.
     * @param readTimeout timeout value, in milliseconds, to be used to set
     * in HttpURLConnection read timeout.
     * @return this
     */
    public HttpRequest setTimeout(int connectTimeout, int readTimeout) {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        return this;
    }

    /**
     * Enable or disable returning headers in the response.
     * @return this
     */
    public HttpRequest setReturnResponseHeaders(boolean value) {
        retRespHeaders = value;
        return this;
    }

    /**
     * Add a standing header to be sent with every request.
     */
    public HttpRequest addStandingHeader(String key, String value) {
        standingHeaders.put(key, value);
        return this;
    }

    /**
     * Disable hostname verification while issuing HTTPS requests.
     * @return this
     */
    public HttpRequest disableHostVerification() {
        this.disableHostVerification = true;
        return this;
    }

    /**
     * Issue HTTP GET request with Content-Type: application/json.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpGet(String url) throws RequestFaultException {

        return doHttpGet(url, null, null);
    }

    /**
     * Issue HTTP POST request with Content-Type: application/json.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpPost(String url, String payload)
        throws RequestFaultException {

        return doHttpPost(url, payload, null, null);
    }

    /**
     * Issue HTTP PUT request with Content-Type: application/json.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpPut(String url, String payload)
        throws RequestFaultException {

        return doHttpPut(url, payload, null);
    }

    /**
     * Issue HTTP DELETE request with Content-Type: application/json.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpDelete(String url, String payload)
        throws RequestFaultException {

        return doHttpDelete(url, payload, null);
    }

    /**
     * Issue HTTP GET request with Content-Type: application/json; and
     * pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpGet(String url, LogContext lc)
        throws RequestFaultException {

        return doHttpGet(url, null, lc);
    }

    /**
     * Issue HTTP POST request with Content-Type: application/json; and
     * pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpPost(String url, String payload, LogContext lc)
        throws RequestFaultException {

        return doHttpPost(url, payload, null, lc);
    }

    /**
     * Issue HTTP PUT request with Content-Type: application/json; and
     * pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpPut(String url, String payload, LogContext lc)
        throws RequestFaultException {

        return doHttpPut(url, payload, null, lc);
    }

    /**
     * Issue HTTP DELETE request with Content-Type: application/json; and
     * pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpDelete(String url, String payload, LogContext lc)
        throws RequestFaultException {

        return doHttpDelete(url, payload, null, lc);
    }

    /**
     * Issue HTTP GET request with Content-Type: application/json, and with
     * given {@link ConnectionHandler} to configure the connection used to issue
     * this request; and pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param handler the ConnectionHandler used to configure all connections
     * used in all executions of this request
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpGet(String url,
                                  ConnectionHandler handler,
                                  LogContext lc)
        throws RequestFaultException {

        return doRequest(
            HttpMethod.GET, url, null /* no payload */, handler, lc);
    }

    /**
     * Issue HTTP POST request with Content-Type: application/json, and with
     * given {@link ConnectionHandler} to configure the connection used to issue
     * this request; and pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @param handler the ConnectionHandler used to configure all connections
     * used in all executions of this request
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpPost(String url,
                                   String payload,
                                   ConnectionHandler handler,
                                   LogContext lc)
        throws RequestFaultException {

        return doRequest(HttpMethod.POST, url, payload, handler, lc);
    }

    /**
     * Issue HTTP PUT request with Content-Type: application/json, and with
     * given {@link ConnectionHandler} to configure the connection used to issue
     * this request; and pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @param handler the ConnectionHandler used to configure all connections
     * used in all executions of this request
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpPut(String url,
                                  String payload,
                                  ConnectionHandler handler,
                                  LogContext lc)
        throws RequestFaultException {

        return doRequest(HttpMethod.PUT, url, payload, handler, lc);
    }

    /**
     * Issue HTTP DELETE request with Content-Type: application/json, and with
     * given {@link ConnectionHandler} to configure the connection used to issue
     * this request; and pass a LogContext object in the HTTP header.
     *
     * Note that this method is re-entrant.
     *
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format
     * @param handler the ConnectionHandler used to configure all connections
     * used in all executions of this request
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if the final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    public HttpResponse doHttpDelete(String url,
                                     String payload,
                                     ConnectionHandler handler,
                                     LogContext lc)
        throws RequestFaultException {

        return doRequest(HttpMethod.DELETE, url, payload, handler, lc);
    }

    /**
     * Determine if given response represents an error that needs to retry.
     * Typically, it checks status code in response, if status code is larger
     * than 500, the request may encounter remote service internal errors,
     * retry. If status code is 401, it uses specified handler to attempt to
     * renew the credentials, if it succeed retry.
     *
     * @return true if the response represents an error that needs to retry,
     * false if the error cannot be retried or the response is a OK response,
     * whose status code is larger than 200 and smaller than 299.
     */
    protected boolean needRetry(HttpResponse response,
                                ConnectionHandler handler) {
        if (!retry) {
            return false;
        }
        final int statusCode = response.getStatusCode();
        if (statusCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
            if (handler != null && handler.renewCredentails()) {
                return true;
            }
        }
        if (statusCode >= 500) {
            /* Retry */
            return true;
        }
        return false;
    }

    /**
     * Handle the final response. This is the final step of HTTP request
     * retries, at this phase, it probably gets a HTTP response with 2xx, 3xx
     * and 4xx status code, or exceed the retry limit because of IOException or
     * HTTP response with 5xx status code. If there is a HTTP response after
     * numbers of retries, it returned directly without checking status code,
     * it's up to caller to check the status code and handle accordingly.<p>
     *
     * By default, this method throws RequestFaultException with ErrorCode
     * SERVICE_UNAVAILABLE, only if no HTTP response returned and last
     * execution is failed because of an IOException.
     *
     * @throws RequestFaultException if there final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    protected HttpResponse handleFinalResponse(HttpResponse res,
                                               IOException ioe,
                                               int totalExecutions,
                                               long totalWaitTime)
        throws RequestFaultException {

        if (ioe != null) {
            throw new RequestFaultException(
                String.format("Problem occurred after %d executions, " +
                              "total wait time %d",
                              totalExecutions, totalWaitTime),
                ioe, ErrorCode.SERVICE_UNAVAILABLE);
        }
        if (res == null) {
            throw new RequestFaultException(
                String.format("Problem occurred after %d executions, " +
                              "total wait time %d",
                              totalExecutions, totalWaitTime),
                ioe, ErrorCode.UNKNOWN_INTERNAL_ERROR);
        }
        return res.setTotalExecutions(totalExecutions).
                   setTotalWaitTime(totalWaitTime);
    }

    /**
     * Execute HTTP request with Content-Type: application/json, and with
     * given {@link ConnectionHandler} to configure the connection used to issue
     * this request; and pass a LogContext object in the HTTP header.
     *
     * @param httpMethod the request method, could be GET, PUT, POST and DELETE
     * @param url the URL request is sent to
     * @param payload the payload this request convey in JSON format, if
     * request is POST, PUT and DELETE
     * @param handler the ConnectionHandler used to configure all connections
     * used in all executions of this request
     * @param lc the LogContext Object that contains logging context
     * @throws RequestFaultException if there final response is not valid and
     * failed because of a IOException.
     * @return HttpResponse the result including a HTTP response with status
     * code, and also the total number of executions and wait time.
     */
    private HttpResponse doRequest(HttpMethod httpMethod,
                                   String url,
                                   String payload,
                                   ConnectionHandler handler,
                                   LogContext lc)
        throws RequestFaultException {

        final RetryHelper retryHelper = new RetryHelper(totalWait, retryInter);
        final long startTime = System.currentTimeMillis();
        HttpResponse response = null;
        IOException lastException = null;

        int numExecution = 0;
        while (!retryHelper.isDone()) {
            try {
                numExecution++;

                /* clear results from last attempt */
                lastException = null;
                response = null;

                if (httpMethod == HttpMethod.GET) {
                    response = doGetRequest(url, handler, lc);
                } else {
                    response = httpRequestWithResponse(
                        httpMethod.name(), url, payload, handler, lc);
                }

                /*
                 * HttpResponse either returns a valid HttpResponse or throws
                 * IOException, a null HttpResponse here means some
                 * unexpected errors occur.
                 */
                if (response == null) {
                    throw new RequestFaultException(
                        "Invalid null HTTP response",
                        ErrorCode.UNKNOWN_INTERNAL_ERROR);
                }

                if (needRetry(response, handler)) {
                    retryHelper.sleep();
                    continue;
                }
                break;
            } catch (MalformedURLException mue) {
                /*
                 * URL is formed by our code, error here indicates an
                 * unexpected error.
                 */
                throw new RequestFaultException(
                    "Malformed URL " + url, mue,
                    ErrorCode.UNKNOWN_INTERNAL_ERROR);
            } catch (InterruptedException ie) {
                /* Unexpected interrupted */
                throw new RequestFaultException(
                    "Unexpected interruption during reties",
                    ie, ErrorCode.UNKNOWN_INTERNAL_ERROR);
            } catch (SSLException |
                     UnknownHostException |
                     UnknownServiceException |
                     NoSuchAlgorithmException |
                     KeyManagementException e) {
                throw new RequestFaultException(
                    "Unexpected network error",
                    e, ErrorCode.UNKNOWN_INTERNAL_ERROR);
            } catch (IOException ioe) {
                lastException = ioe;

                /* Other IOE is considered as retryable error */
                if (!retry) {
                    break;
                }
                try {
                    retryHelper.sleep();
                } catch (InterruptedException ie) {
                    /* Unexpected interrupted */
                    throw new RequestFaultException(
                        "Unexpected interruption during reties",
                        ie, ErrorCode.UNKNOWN_INTERNAL_ERROR);
                }
                continue;
            }
        }
        final long totalWaitTime = System.currentTimeMillis() - startTime;

        /*
         * Exceed the retries limit or get a HTTP response with 2xx status code,
         * resolve response and last cached IOException.
         */

        return handleFinalResponse(response, lastException,
                                   numExecution, totalWaitTime);
    }

    /**
     * Issue a GET with Content-Type: application/json, specified connect and
     * read timeout, and also with given {@link ConnectionHandler} to configure
     * the connection used to issue this request; and pass a LogContext
     * object in the HTTP header.
     *
     * @throws MalformedURLException if URL specified is malformed
     * @throws IOException encountered during the connection issue the request
     */
    private HttpResponse doGetRequest(String url,
                                      ConnectionHandler handler,
                                      LogContext lc)

        throws MalformedURLException,
               IOException,
               NoSuchAlgorithmException,
               KeyManagementException {

        /* Setup the connection */
        final HttpURLConnection con = getConnection(url, handler);
        con.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON);
        if (handler != null) {
            final String authHeader = handler.getAuthorizationHeader();
            if (authHeader != null) {
                con.setRequestProperty(AUTHORIZATION, authHeader);
            }
            handler.configureConnection(con);
        }
        con.setRequestMethod(HttpMethod.GET.name());
        con.setConnectTimeout(connectTimeout);
        con.setReadTimeout(readTimeout);
        con.setDoOutput(true);
        con.setDoInput(true);

        if (lc != null) {
            String encodedLc =
                new String(encoder.encode(lc.toString().getBytes()));
            con.setRequestProperty(REQUEST_LOGCONTEXT_HEADER, encodedLc);
        }

        /* Issue the request */
        return getResponse(con);
    }

    /**
     * Issue an HTTP request of application/json type with the specified
     * request method, request body, and timeout, and also with given
     * {@link ConnectionHandler} to configure the connection used to issue
     * this request.
     *
     * @throws MalformedURLException if URL specified is malformed
     * @throws IOException encountered during the connection issue the request
     */
    private HttpResponse httpRequestWithResponse(String requestMethod,
                                                 String url,
                                                 String jsonPayload,
                                                 ConnectionHandler handler,
                                                 LogContext lc)
        throws MalformedURLException,
               IOException,
               NoSuchAlgorithmException,
               KeyManagementException {

        /* Setup the connection */
        final HttpURLConnection con = getConnection(url, handler);
        con.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON);
        if (handler != null) {
            final String authHeader = handler.getAuthorizationHeader();
            if (authHeader != null) {
                con.setRequestProperty(AUTHORIZATION, authHeader);
            }
            handler.configureConnection(con);
        }
        con.setRequestMethod(requestMethod);
        con.setConnectTimeout(connectTimeout);
        con.setReadTimeout(readTimeout);
        con.setDoOutput(true);
        con.setDoInput(true);

        if (lc != null) {
            final String encodedLc =
                new String(encoder.encode(lc.toString().getBytes()));
            con.setRequestProperty(REQUEST_LOGCONTEXT_HEADER, encodedLc);
        }

        /* Write the json payload into the request body */
        if (jsonPayload != null) {
            final OutputStreamWriter wr =
                new OutputStreamWriter(con.getOutputStream());
            wr.write(jsonPayload);
            wr.flush();
        }

        /* Issue the request */
        return getResponse(con);
    }

    private HttpURLConnection getConnection(String url,
                                            ConnectionHandler handler)
         throws MalformedURLException,
                IOException,
                KeyManagementException,
                NoSuchAlgorithmException {

        final HttpURLConnection con = (HttpURLConnection)
            new URL(url).openConnection();
        if (con instanceof HttpsURLConnection) {
            HttpsURLConnection conHttps = (HttpsURLConnection) con;
            if (handler != null && handler.getSSLSocketFactory() != null) {
                conHttps.setSSLSocketFactory(handler.getSSLSocketFactory());
            }
            if (disableHostVerification) {
                conHttps.setHostnameVerifier(allHostsValid);
            }
        }
        for (Map.Entry<String, String> header : standingHeaders.entrySet()) {
            con.setRequestProperty(header.getKey(), header.getValue());
        }
        return con;
     }

    private HttpResponse getResponse(HttpURLConnection con)
        throws IOException {

        int responseCode = 0;
        try {
            responseCode = con.getResponseCode();

            /* See if there's something to return */
            InputStreamReader inputStreamReader = null;
            if ((responseCode >= 200) && (responseCode < 400)) {
                if (con.getInputStream() != null) {
                    InputStream in = con.getInputStream();
                    inputStreamReader =
                        new InputStreamReader(in);
                }
            } else {
                /* There isn't always an error stream */
                if (con.getErrorStream() != null) {
                    inputStreamReader =
                        new InputStreamReader(con.getErrorStream());
                }
            }

            String output = readResponse(inputStreamReader);

            if (!retRespHeaders) {
                return new HttpResponse(responseCode, output);
            }
            return new HttpResponse(responseCode, output,
                                    new HashMap<>(con.getHeaderFields()));
        } catch (IOException ioe) {
            /* Read error message from error stream if there is any */
            String errMsg = null;
            if (con.getErrorStream() != null) {
                try {
                    InputStream is = con.getErrorStream();
                    errMsg = readResponse(new InputStreamReader(is));
                } catch (IOException ex) {
                    /* do nothing */
                }
            }

            /* Return http error response if repsonseCode != 0 */
            if (responseCode != 0) {
                return new HttpResponse(responseCode,
                    "error getting output " +
                    (errMsg != null ? errMsg + ": " + ioe.getMessage() :
                                      ioe.getMessage()));
            }

            /*
             * If read error message from error stream, throw IOE with that
             * error message from error stream included.
             */
            if (errMsg != null) {
                throw new IOException(errMsg + ": " + ioe.getMessage(),
                                      ioe.getCause());
            }

            throw ioe;
        }
    }

    private String readResponse(InputStreamReader inputStreamReader)
        throws IOException {

        final StringBuilder sb = new StringBuilder();

        /* Read the response if there is one */
        if (inputStreamReader != null) {
            final BufferedReader in = new BufferedReader(inputStreamReader);
            String inputLine;
            try {
                while ((inputLine = in.readLine()) != null) {
                    sb.append(inputLine);
                }
            } finally {
                in.close();
            }
        }

        return sb.toString();
    }

    /**
     * A helper interface to configure the HttpURLConnection with authorization
     * header, request properties and etc. The caller of this handler must
     * complete the configuration of connection before the connection actually
     * sends the request.
     */
    public interface ConnectionHandler {
        /**
         * SSLSocketFactory used to initialize HttpsURLConnection. Note that
         * HttpsURLConnection won't reuse the connection unless the same
         * socket factory is given. Return cached factory object to enable
         * HTTP persistent connection.
         *
         * @return SSLSocketFactory
         */
        public default SSLSocketFactory getSSLSocketFactory() {
            return null;
        }

        /**
         * Configure the given HttpURLConnection. It could be some of request
         * properties and other HttpURLConnection settings.
         * @param con the HttpURLConnection connection is used to issue request
         */
        public void configureConnection(HttpURLConnection con);

        /**
         * Provide ability to renew credentials.
         *
         * @return true if renewal succeed, false only if renewal failed or
         * the implementation doesn't support credentials renewal.
         */
        public default boolean renewCredentails() {
            return false;
        }

        /**
         * Build an authorization header in form of "<type> <credentials>" to
         * authenticate, which could be directly assigned as "Authorization"
         * request header.
         *
         * @return an authorization header used to authenticate request
         */
        public default String getAuthorizationHeader() {
            return null;
        }
    }
}
