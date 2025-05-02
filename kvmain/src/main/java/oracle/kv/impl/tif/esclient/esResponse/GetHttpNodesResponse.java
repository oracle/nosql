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
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient.Scheme;
import oracle.kv.impl.tif.esclient.httpClient.HttpHost;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.JsonResponseObjectMapper;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * This is meant to be a monitoring API for getting available nodes
 * information. This API is used in ESNodeMonitorto determine current available
 * nodes with published http/https port.
 * 
 * A note on ES 2.x and ES 5.x changes: - in ES 2.x http_address was available.
 * -in ES 5.x there is a publish_address inside a structure named http. - ES
 * 2.x also has this publish_address. And this will be used as the host
 * address.
 *
 *
 */
public class GetHttpNodesResponse extends ESResponse
        implements JsonResponseObjectMapper<GetHttpNodesResponse> {

    @SuppressWarnings("unused")
    private static final String _NODES = "_nodes"; // A new structure in ES
                                                   // 5.x.Not parsed currently.
    private static final String NODES = "nodes";

    private static final String CLUSTER_NAME = "cluster_name";

    private List<HttpNode> httpNodes = new ArrayList<HttpNode>();
    private String clusterName = null;

    public GetHttpNodesResponse() {

    }

    public GetHttpNodesResponse(List<HttpNode> httpNodes, String clusterName) {

        this.httpNodes = httpNodes;
        this.clusterName = clusterName;

    }

    /**
     * A convenience method to get a list of HttpHosts from the publish address
     * of ES nodes.
     * 
     * @param scheme
     * @return List of Available Http Hosts on ES Cluster
     */
    public List<HttpHost> getHttpHosts(Scheme scheme, Logger logger) {

        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
        for (HttpNode httpNode : httpNodes) {
            String publishAddress =
                httpNode.getHttpInfo().getPublish_address();
            /*
             * publishAddress can come as 'hostName/IPAddress' format Just take
             * the hostName
             */
            if (publishAddress.contains("/")) {
                publishAddress = publishAddress.split("/")[0] + ":" +
                        publishAddress.split(":")[1];
            }
            URI uri =
                URI.create(scheme.getProtocol() + "://" + publishAddress);
            String resolvedHostName = null;
            try {
                resolvedHostName =
                    InetAddress.getByName(uri.getHost()).getHostName();
            } catch (UnknownHostException e) {
                logger.log(Level.SEVERE, "Could not resolve host", e);
            }
            HttpHost host =
                new HttpHost(uri.getScheme(),
                             ESRestClientUtil.isEmpty(resolvedHostName)
                             ? uri.getHost() : resolvedHostName, uri.getPort());
            httpHosts.add(host);
        }

        return httpHosts;

    }

    public List<HttpNode> getHttpNodes() {
        return httpNodes;
    }

    public void setHttpNodes(List<HttpNode> httpNodes) {
        this.httpNodes = httpNodes;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public GetHttpNodesResponse buildFromJson(JsonParser parser)
        throws IOException {

        JsonToken token = parser.nextToken();
        ESJsonUtil.validateToken(JsonToken.START_OBJECT, token, parser);

        String currentFieldName = null;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue;
            }
            if (CLUSTER_NAME.equals(currentFieldName)) {
                if (token.isScalarValue()) {
                    clusterName = parser.getText();
                }
            }
            if (token == JsonToken.START_OBJECT) {
                if (NODES.equals(currentFieldName)) {
                    /*
                     * For some reason Nodes in response is not an array. and
                     * node structure starts with a nodeId.
                     */

                    while (parser.nextToken() == JsonToken.FIELD_NAME) {
                        HttpNode httpNode = new HttpNode();
                        httpNode.buildFromJson(parser);
                        httpNodes.add(httpNode);
                        // token is at END_OBJECT of the currentNode.
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }
        parsed(true);
        return this;
    }

    @Override
    public GetHttpNodesResponse buildErrorReponse(ESException e) {
        return null;
    }

    @Override
    public GetHttpNodesResponse buildFromRestResponse(RestResponse restResp)
        throws IOException {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GetHttpNodesResponse:[ clusterName:");
        sb.append(getClusterName());
        sb.append(" HttpNodes:").append(getHttpNodes()).append("]");

        return sb.toString();

    }

    public static class HttpNode {

        private static final String NAME = "name";
        private static final String TRANSPORT_ADDRESS = "transport_address";
        private static final String HOST = "host";
        private static final String IP = "ip";
        private static final String VERSION = "version";
        private static final String ROLES = "roles"; // For ES 5.x
        private static final String ATTRIBUTES = "attributes"; // For ES 2.x
        private static final String HTTP = "http";

        /*
         * Let everything be string here. They need not be an object for now.
         */
        private String nodeId;
        private HttpInfo httpInfo;
        private String name;
        private String transportAddress;
        private String host;
        private String ip;
        private String version;

        private boolean master = false;

        /*
         * Ideally roles should be an enum. For now FTS ES Client does not use
         * this info. So leaving it as string.
         */
        List<String> roles = new ArrayList<String>();
        Map<String, String> attributes = new HashMap<String, String>();

        private HttpNode() {

        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public HttpInfo getHttpInfo() {
            return httpInfo;
        }

        public void setHttpInfo(HttpInfo httpInfo) {
            this.httpInfo = httpInfo;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTransportAddress() {
            return transportAddress;
        }

        public void setTransportAddress(String transportAddress) {
            this.transportAddress = transportAddress;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public List<String> getRoles() {
            return roles;
        }

        public void setRoles(List<String> roles) {
            this.roles = roles;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public void setAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
        }

        public boolean isMaster() {
            return master;
        }

        public void setMaster(boolean master) {
            this.master = master;
        }

        public HttpNode buildFromJson(JsonParser parser) throws IOException {
            /*
             * The parser is positioned at NodeId.
             */
            JsonToken token = parser.currentToken();
            ESJsonUtil.validateToken(JsonToken.FIELD_NAME, token, parser);
            String currentFieldName = parser.currentName();
            nodeId = currentFieldName;
            ESJsonUtil.validateToken(JsonToken.START_OBJECT,
                                     parser.nextToken(), parser);
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    continue;
                }
                if (token.isScalarValue()) {
                    if (NAME.equals(currentFieldName)) {
                        name = parser.getText();
                    } else if (TRANSPORT_ADDRESS.equals(currentFieldName)) {
                        transportAddress = parser.getText();
                    } else if (TRANSPORT_ADDRESS.equals(currentFieldName)) {
                        transportAddress = parser.getText();
                    } else if (HOST.equals(currentFieldName)) {
                        host = parser.getText();
                    } else if (IP.equals(currentFieldName)) {
                        ip = parser.getText();
                    } else if (VERSION.equals(currentFieldName)) {
                        version = parser.getText();
                    } else if (TRANSPORT_ADDRESS.equals(currentFieldName)) {
                        transportAddress = parser.getText();
                    }
                } else if (token == JsonToken.START_ARRAY) {
                    if (ROLES.equals(currentFieldName)) {
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            String role = parser.getText();
                            roles.add(role);
                            if (role.toLowerCase().equals("master")) {
                                master = true;
                            }
                        }
                    } else {
                        parser.skipChildren();
                    }

                } else if (token == JsonToken.START_OBJECT) {
                    if (ATTRIBUTES.equals(currentFieldName)) {
                        while ((token =
                            parser.nextToken()) != JsonToken.END_OBJECT) {
                            String key = null;
                            String value = null;
                            // Not Expecting null Key to have a non-null value.
                            if (token == JsonToken.FIELD_NAME) {
                                key = parser.currentName();
                                token = parser.nextToken();
                            }
                            if (token.isScalarValue() ||
                                    token == JsonToken.VALUE_NULL) {
                                if (token != JsonToken.VALUE_NULL) {
                                    value = parser.getText();
                                }
                            } else {
                                continue;
                            }

                            if (key != null && value != null) {
                                attributes.put(key, value);
                                if (key.toLowerCase().equals("master") &&
                                        value.toLowerCase().equals("true")) {
                                    master = true;
                                }
                                key = null;
                                value = null;
                            }

                        }
                    } else if (HTTP.equals(currentFieldName)) {
                        httpInfo = new HttpInfo();
                        httpInfo.buildFromJson(parser);
                    }
                }
            }

            return this;
        }

        @Override
        public String toString() {
            return "NodeId:" + nodeId + " NodeName:" + getName() +
                    " httpInfo:[" + getHttpInfo() + " ]" + " roles: " +
                    getRoles() + " attributes:" + getAttributes() +
                    " transportAddress:" + getTransportAddress() + " host:" +
                    getHost() + " ip:" + getIp() + " version:" + getVersion();
        }

        public static class HttpInfo {
            private static final String BOUND_ADDRESS = "bound_address";
            private static final String PUBLISH_ADDRESS = "publish_address";

            private List<String> bound_addresses = new ArrayList<String>();
            private String publish_address;

            public List<String> getBound_addresses() {
                return bound_addresses;
            }

            public void setBound_addresses(List<String> bound_addresses) {
                this.bound_addresses = bound_addresses;
            }

            public String getPublish_address() {
                return publish_address;
            }

            public void setPublish_address(String publish_address) {
                this.publish_address = publish_address;
            }

            private HttpInfo() {

            }

            public HttpInfo(List<String> bound_addresses,
                    String publish_address) {
                this.bound_addresses = bound_addresses;
                this.publish_address = publish_address;
            }

            public HttpInfo buildFromJson(JsonParser parser)
                throws IOException {
                JsonToken token = parser.currentToken();
                ESJsonUtil.validateToken(JsonToken.START_OBJECT, token,
                                         parser);
                String currentFieldName = parser.currentName();
                while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                    if (token == JsonToken.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        continue;
                    }
                    if (token.isScalarValue()) {
                        if (PUBLISH_ADDRESS.equals(currentFieldName)) {
                            this.publish_address = parser.getText();
                        }
                    } else if (token == JsonToken.START_ARRAY) {
                        if (BOUND_ADDRESS.equals(currentFieldName)) {
                            while (parser.nextToken() != JsonToken.END_ARRAY) {
                                bound_addresses.add(parser.getText());
                            }
                        } else {
                            parser.skipChildren();
                        }
                    } else {
                        parser.skipChildren();
                    }

                }

                return this;
            }

            @Override
            public String toString() {
                return "publish_address:" + publish_address +
                        " bound_addresses:" + bound_addresses;
            }
        }

    }

}
