/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.security;

import static oracle.nosql.driver.util.HttpConstants.REQUEST_COMPARTMENT_ID;
import static oracle.nosql.driver.util.HttpConstants.AUTHORIZATION;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.Request;

import io.netty.handler.codec.http.HttpHeaders;

public class TestSignatureProvider extends SignatureProvider {

    private String tenantId;
    private String userId;

    public TestSignatureProvider() {
    	super(null, 0, 0);
        tenantId = "TestTenant";
        userId = "TestUser";
    }

    public TestSignatureProvider setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    @Override
    public String getAuthorizationString(Request request) {
        /*
         * IAM signature format:
         * Signature version="%s",headers="%s",keyId="%s",
         *           algorithm="rsa-sha256",signature="%s"
         *
         * Note that the tenantId/compartmentId are not inherently
         * easily available from the auth string ("keyId" may include the
         * tenancy, but may also be different for, say, instance
         * principals)
         *
         * example real header:
         * Signature headers="(request-target) host date",keyId="ocid1.tenancy.oc1..aaaaaaaaba3pv6wuzr4h25vqstifsfdsq/ocid1.user.oc1..aaaaaaaa65vwl75tewwm32rgqvm6i34unq/9b:39:03:07:c6:fa:5c:58:7d:60:85:d8:3e:5c:be:7e",algorithm="rsa-sha256",signature="LLszR7k+iORqsLNOVXdPVjRupFDnV99PhByYqWGxsJi6/04xWD0jVA4hnawCG5ciyXA4O2eUH+Ggh/glEnbLht3yowdLelPDnI6nQ9fC7tsQjIM5YsFka0k9AzPPRkpX6l2Ic3/CWvonf9zjeR6KM1ICcakCrYj6Xjmla5tapbJJ5AOv1r5jzCiIAq6avZSS+rRHrFjFVbgKkGekFJKJjh4CPA1beO1YYBF+ZcIGwxL7ItvWkV2AFTEv/0L15W4hEkEbDjQq5eeCvJdLUD8VfLYt1ELLmMZdnUvPXVfYrCHM1qQWLKS6KSerIjdaSKvzYD71idCDDQ+FGFYxcOPA8Q==",version="1"
         *
         * This needs to at least be in the format above such that the
         * cloudsim tests run with "-Dsecurity=true" will pass
         */
        return "Signature headers=\"(request-target) host date\",keyId=\"" +
            tenantId + "/" + userId + "/dummy\"," +
            "algorithm=\"rsa-sha256\",signature=\"dummy\",version=\"1\"";
    }

    @Override
    public void setRequiredHeaders(String authString,
                                   Request request,
                                   HttpHeaders headers,
                                   byte[] content) {
        String compartment = request.getCompartment();
        if (compartment == null) {
            /*
             * If request doesn't has compartment id, set the tenant id as the
             * default compartment, which is the root compartment in IAM if
             * using user principal.
             */
            compartment = tenantId;
        }
        if (compartment != null) {
            headers.add(REQUEST_COMPARTMENT_ID, compartment);
        }
        headers.add(AUTHORIZATION, getAuthorizationString(null));
    }

    @Override
    public void close() {
    }

    /**
     * @since 5.2.27, prepare would throw NPE without specifying
     * AuthenticationProfileProvider to SignatureProvider.
     */
    @Override
    public SignatureProvider prepare(NoSQLHandleConfig config) {
        return this;
    }
}
