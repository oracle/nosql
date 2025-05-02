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

package oracle.kv.impl.tif.esclient.esResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ESResponse {

    public static final String _INDEX = "_index";
    public static final String _TYPE = "_type";
    public static final String _ID = "_id";
    public static final String _VERSION = "_version";

    private int httpStatusCode;
    /*
     * When the response is not parsed fully, but the response object is good
     * enough for consumption, buildFromJson method can return the object
     * without marking parsed true.
     * 
     * An example could be bulk items. It might not be required to parse it
     * fully.
     * 
     */
    private boolean parsed = false;

    public int statusCode() {
        return httpStatusCode;
    }

    public void statusCode(int statusCode) {
        this.httpStatusCode = statusCode;
    }

    public boolean isParsed() {
        return parsed;
    }

    public void parsed(boolean parsed1) {
        this.parsed = parsed1;
    }

    /**
     * Replica Info - Informs about total no of replicas this operation was
     * carried out, and successful ones. And failures at replica.
     *
     */
    public static class ShardInfo
            implements JsonResponseObjectMapper<ShardInfo> {

        private static final String TOTAL = "total";
        private static final String SUCCESSFUL = "successful";
        private static final String FAILURES = "failures";
        private static final String FAILED = "failed";

        private int total;
        private int successful;
        @SuppressWarnings("unused")
        private int failed = -1;
        private List<ReplicaFailure> failures =
            new ArrayList<ReplicaFailure>();

        public ShardInfo() {
        }

        public ShardInfo(int total,
                int successful,
                int failed,
                List<ReplicaFailure> failures)
                throws InvalidResponseException {
            if (total < 0 && successful < 0) {
                throw new InvalidResponseException("ShardInfo has total" +
                                                   " and successful less" +
                                                   " than zero");
            }
            this.total = total;
            this.successful = successful;
            this.failures = failures;
            this.failed = failed;
        }

        /* Builder Methods - setters */

        public ShardInfo total(int total1) {
            this.total = total1;
            return this;
        }

        public ShardInfo successful(int successful1) {
            this.successful = successful1;
            return this;
        }

        public ShardInfo failed(int failed1) {
            this.failed = failed1;
            return this;
        }

        public ShardInfo failures(List<ReplicaFailure> failures1) {
            this.failures = failures1;
            return this;
        }

        /**
         * Total no of shards write request was executed on. It is possible
         * that this number is greater than total no of shards, as it is
         * possible to send write request to both primary and replica of a
         * shard.
         * 
         * @return total no of shards
         */
        public int total() {
            return total;
        }

        /**
         * @return Total no of shards write request was successful on.
         */
        public int successful() {
            return successful;
        }

        /**
         * @return TODO: check what are these failures we get in the rest API
         *         response.
         */
        public int failed() {
            return failures.size();
        }

        /**
         * @return The failure structures received in Rest Response.
         */
        public List<ReplicaFailure> getFailures() {
            return failures;
        }

        /*
         * TODO: ESWriteResponse status can be set directly from RestResponse
         * by the ESRestClient. But then that status does not offer control on
         * shard level responses. Some replicas might have failed to replicate
         * the mutation.
         * 
         * Do we use this status as ESWriteResponse status or not?
         * 
         */
        public RestStatus shardStatus() {
            RestStatus status = RestStatus.OK;
            for (ReplicaFailure failure : failures) {
                if (failure.primary() &&
                        failure.status().getStatus() > status.getStatus()) {
                    status = failure.status();
                }
            }
            return status;
        }

        /**
         * Builds the structure "_shards" in the ES Response.
         */
        @Override
        public ShardInfo buildFromJson(JsonParser parser) throws IOException {
            JsonToken token = parser.currentToken();
            ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);
            String currentFieldName = null;
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isScalarValue()) {
                    if (TOTAL.equals(currentFieldName)) {
                        total = parser.getIntValue();
                    } else if (SUCCESSFUL.equals(currentFieldName)) {
                        successful = parser.getIntValue();
                    } else if (FAILED.equals(currentFieldName)) {
                        failed = parser.getIntValue();
                    }
                } else if (token == JsonToken.START_ARRAY) {
                    if (FAILURES.equals(currentFieldName)) {
                        while ((token =
                            parser.nextToken()) != JsonToken.END_ARRAY) {
                            ReplicaFailure failure = new ReplicaFailure();
                            failures.add(failure.buildFromJson(parser));
                        }
                    } else {
                        throw new IOException(new InvalidResponseException
                                              ("Unknown Field: " +
                                               currentFieldName + " at:" +
                                               parser.currentTokenLocation()));

                    }
                }
            }

            return this;

        }

        @Override
        public String toString() {
            return "ShardInfo{" + "total=" + total + ", successful=" +
                    successful + ", failures=" + failures + '}';
        }

        /**
         * This is a failure structure for a particular shardId on a particular
         * nodeid.
         * 
         * It also specifies if the failed shard is primary or not.
         *
         */
        public static class ReplicaFailure
                implements JsonResponseObjectMapper<ReplicaFailure> {

            private static final String _INDEX_REPLICA = "_index";
            private static final String _SHARD = "_shard";
            private static final String _NODE = "_node";
            private static final String REASON = "reason";
            private static final String STATUS = "status";
            private static final String PRIMARY = "primary";

            private ShardId shardId;

            public boolean isPrimary() {
                return primary;
            }

            private String nodeId;
            private String cause;
            private RestStatus status;
            private boolean primary;

            public ReplicaFailure(ShardId shardId,
                    String nodeId,
                    String cause,
                    RestStatus status,
                    boolean primary) {
                this.shardId = shardId;
                this.nodeId = nodeId;
                this.cause = cause;
                this.status = status;
                this.primary = primary;
            }

            ReplicaFailure() {
            }

            /* Builder Methods - setters */

            public ReplicaFailure primary(boolean primary1) {
                this.primary = primary1;
                return this;
            }

            public ReplicaFailure shardId(ShardId shardId1) {
                this.shardId = shardId1;
                return this;
            }

            public ReplicaFailure nodeId(String nodeId1) {
                this.nodeId = nodeId1;
                return this;
            }

            public ReplicaFailure cause(String cause1) {
                this.cause = cause1;
                return this;
            }

            public ReplicaFailure status(RestStatus status1) {
                this.status = status1;
                return this;
            }

            /**
             * @return On what index the failure occurred.
             */
            public String index() {
                return shardId.getIndex();
            }

            /* Getters */
            /**
             * @return On what shard id the failure occurred.
             */
            public int shardId() {
                return shardId.id();
            }

            public ShardId fullShardId() {
                return shardId;
            }

            /**
             * @return On what node the failure occurred.
             */
            public String nodeId() {
                return nodeId;
            }

            /**
             * @return A text description of the failure
             */
            public String reason() {
                return cause;
            }

            /**
             * @return The status to report if this failure was a primary
             *         failure.
             */
            public RestStatus status() {
                return status;
            }

            public String getCause() {
                return cause;
            }

            /**
             * If the replica is primary, return true. This means failure
             * occurred on primary replica. This is not a possible case for FTS
             * client operations but Rest Response has it, so parse it.
             */
            public boolean primary() {
                return primary;
            }

            @Override
            public ReplicaFailure buildFromJson(JsonParser parser)
                throws IOException {
                JsonToken token = parser.currentToken();
                ESJsonUtil.validateToken(JsonToken.START_OBJECT, token,
                                         parser);
                String index = null;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                    if (token == JsonToken.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isScalarValue()) {
                        if (_INDEX_REPLICA.equals(currentFieldName)) {
                            index = parser.getText();
                        } else if (_SHARD.equals(currentFieldName)) {
                            shardId = new ShardId(index, parser.getIntValue());
                        } else if (_NODE.equals(currentFieldName)) {
                            nodeId = parser.getText();
                        } else if (STATUS.equals(currentFieldName)) {
                            status = RestStatus.valueOf(parser.getText());
                        } else if (PRIMARY.equals(currentFieldName)) {
                            primary = parser.getBooleanValue();
                        } else {
                            throw new IOException
                            (new InvalidResponseException("Unknown Field: " +
                                                          currentFieldName +
                                                          " at:" +
                                                          parser
                                                          .currentTokenLocation()
                                                          ));
                        }
                    } else if (token == JsonToken.START_OBJECT) {
                        if (REASON.equals(currentFieldName)) {
                            cause = parser.getText();
                        } else {
                            throw new IOException(new InvalidResponseException
                                                  ("Unknown Field: " +
                                                   currentFieldName +
                                                   " at:" +
                                                   parser
                                                   .currentTokenLocation()));
                        }
                    }
                }
                return this;

            }

            @Override
            public ReplicaFailure buildErrorReponse(ESException e) {
                return null;
            }

            @Override
            public ReplicaFailure
                    buildFromRestResponse(RestResponse restResp) {
                return null;
            }

        }

        static class ShardId implements Comparable<ShardId> {

            private String index;

            private int shardId;

            private String indexUUID;

            public ShardId(String index, int shardId) {
                this.index = index;
                this.shardId = shardId;
            }

            public ShardId(String index, String indexUUID, int shardId) {
                this.index = index;
                this.shardId = shardId;
                this.indexUUID = indexUUID;
            }

            public String getIndex() {
                return index;
            }

            public String getIndexUUID() {
                return indexUUID;
            }

            public int id() {
                return this.shardId;
            }

            public int getId() {
                return id();
            }

            @Override
            public String toString() {
                return "[" + index + "][" + shardId + "]";
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null)
                    return false;
                ShardId shardId1 = (ShardId) o;
                return shardId == shardId1.shardId &&
                        index.equals(shardId1.index);
            }

            @Override
            public int hashCode() {
                String indexString = (index == null) ? "" : index;
                return 17 * (shardId) + 19 * (indexString.hashCode());
            }

            @Override
            public int compareTo(ShardId o) {
                if (o.getId() == shardId) {
                    int compare = index.compareTo(o.getIndex());
                    if (compare != 0) {
                        return compare;
                    }
                    return getIndexUUID().compareTo(o.getIndexUUID());
                }
                return Integer.compare(shardId, o.getId());
            }
        }

        @Override
        public ShardInfo buildErrorReponse(ESException e) {
            return null;
        }

        @Override
        public ShardInfo buildFromRestResponse(RestResponse restResp) {
            /*
             * This method is called when response is not parseable by the FTS
             * DMLClient code. However the rest response is successful. So an
             * empty response is needed and no exception to be thrown.
             */
            return new ShardInfo();
        }
    }

}
