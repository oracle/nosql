package oracle.nosql.util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Utility methods for managing the hostname:port tuple.
 * Special note: a KVStoreConfig requires an array of [host:port, ..]
 * It's easy and incorrect to pass a single String of "host:port,host:port"
 */
public class HostPort {

    private String host;
    private int port;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public HostPort(String hostPort) {
        String[] parts = hostPort.split(":");
        this.host = parts[0];
        port = Integer.parseInt(parts[1]);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toUrl(boolean useSSL) {
        return (useSSL ? "https://": "http://") + getHost() + ":" + getPort();
    }

    /**
     * Given a List<hostnames> and a registry port, turn it into an array of
     * [host:port, host:port] suitable for passing to KVStoreConfig.
     */
    public static String[] getHelperHosts(List<String> hosts, int port) {

        List<String> hps = new ArrayList<>();
        for (String host: hosts) {
            hps.add(host + ":" + port);
        }
        return hps.toArray(new String[0]);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    /**
     * Given a string in the format of host:port,host:port, return an array of
     * [host:port, host:port] suitable for passing to KVStoreConfig
     */
    public static String[] getHelperHosts(String helpers) {
        StringTokenizer tokenizer = new StringTokenizer(helpers, ",");
        List<String> hps = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            hps.add(tokenizer.nextToken());
        }
        return hps.toArray(new String[0]);
    }

    /**
     * Take an array of [host:port, ] and display as a string.
     */
     public static String display(String[] helperHosts) {
         StringBuilder sb = new StringBuilder();
         for (String s : helperHosts) {
             sb.append(s).append(",");
         }
         return sb.toString();
     }
}
