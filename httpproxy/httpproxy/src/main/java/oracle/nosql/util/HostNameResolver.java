package oracle.nosql.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A utility for all cloud components to get the hostname for the node that is
 * running the Docker container.
 * Nodes managed by OKE are given hostnames that are OCIDs.
 * After we migrate to OKE, it was hard to understand which instance is
 * referred to; the instance name is easier for humans than the ocid. To be
 * able to provide the instance name, we save the instance name to the file
 * /etc/instance-name, which is mounted to /tmp/etc/instance-name inside the
 * container.
 * As we want to use instance name as host name if possible, we will resolve
 * host name in following order:
 * 1. Read from /tmp/etc/instance-name file.
 * 2. Read from HOST_NAME system environment.
 * 3. Resolve host name from DNS.
 */
public class HostNameResolver {

    private static final String INSTANCE_NAME_PATH = "/tmp/etc/instance-name";
    private static String HOST_NAME_ENV = "HOST_NAME";

    public static String getHostName() {
        String hostName = readFileLine(INSTANCE_NAME_PATH);
        if (hostName != null && !hostName.isEmpty()) {
            return hostName;
        }
        hostName = System.getenv(HOST_NAME_ENV);
        if (hostName != null && !hostName.isEmpty()) {
            return hostName;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Cannot resolve local host name: " + e);
        }
    }

    private static String readFileLine(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine();
            return line;
        } catch (IOException e) {
            return null;
        }
    }
}
