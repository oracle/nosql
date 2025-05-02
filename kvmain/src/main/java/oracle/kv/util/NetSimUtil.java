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

package oracle.kv.util;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.Map;

/**
 * NetSimUtil provides the common functionalities used by instance of
 * NetSim.java. In the current implementation, NetSimUtil is providing
 * functionality to set network configuration parameter in the Socket
 * Channel for network simulation tool.
 */

public class NetSimUtil {

    /**
     * settcpparams initializes the SocketChannel with required
     * network configuration parameters and returns SocketChannel.
     */
    public static SocketChannel setTcpParams(SocketChannel sc,
            Map<String, String> paramsAsMap)
                    throws IOException, SocketException{

    for (Map.Entry<String,String> params_itr : paramsAsMap.entrySet()) {

        /**
         * Network configuration parameter Key
         */
        final String paramKey = params_itr.getKey();

        /**
         * Network configuration parameter Value
         */
        final String paramValue = params_itr.getValue();

        if ("tcpNoDelay".equals(paramKey)) {
            if (isCorrectBoolean(paramValue)) {
                sc.socket().setTcpNoDelay(Boolean.valueOf(paramValue));
            } else {
                throw new IllegalArgumentException(paramValue +
                        " is not a boolean.");
            }
        } else if ("receiveBufferSize".equals(paramKey)) {
            try {
                sc.socket().setReceiveBufferSize(
                        Integer.parseInt(paramValue));
            }
            catch (NumberFormatException e) {
                throw new NumberFormatException("receiveBufferSize is not" +
            " valid Integer.");
            }
        } else if ("reuseAddress".equals(paramKey)) {
            if (isCorrectBoolean(paramValue)){
                sc.socket().setReuseAddress(Boolean.valueOf(paramValue));
            } else {
                throw new IllegalArgumentException(paramValue +
                        " is not a boolean.");
            }
        } else if ("sendBufferSize".equals(paramKey)) {
            try {
                sc.socket().setSendBufferSize(Integer.parseInt(paramValue));
            }
            catch (NumberFormatException e) {
                throw new NumberFormatException("sendBufferSize is not" +
            " valid Integer.");
            }
        }
    }

    return sc;
  }

  private static boolean isCorrectBoolean(String value) {
      return value.equalsIgnoreCase("true") ||
             value.equalsIgnoreCase("false");
  }

}
