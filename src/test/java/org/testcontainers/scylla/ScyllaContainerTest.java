package org.testcontainers.scylla;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.scylla.wait.ScyllaQueryWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Eugeny Karpov
 * @author Jochen Schalanda
 */
public class ScyllaContainerTest {

    private static final DockerImageName SCYLLA_IMAGE = DockerImageName.parse("scylladb/scylla:4.2.1");
    private static final String SCYLLA_CASSANDRA_FAKE_VERSION = "3.0.8";

    private static final String TEST_CLUSTER_NAME_IN_CONF = "Test Cluster Integration Test";

    @Test
    public void testSimple() throws IOException {
        try (ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>(SCYLLA_IMAGE)) {
            scyllaContainer.start();
            ResultSet resultSet = performQuery(scyllaContainer, "SELECT release_version FROM system.local");
            assertTrue(resultSet.wasApplied(), "Query was not applied");
            assertNotNull(resultSet.one().getString(0), "Result set has no release_version");
            assertTrue(getScyllaVersion(scyllaContainer).startsWith("4.2.1"), "Scylla has wrong version");
        }
    }

    @Test
    public void testSpecificVersion() throws IOException {
        String scyllaVersion = "4.0.10";
        try (ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>(SCYLLA_IMAGE.withTag(scyllaVersion))) {
            scyllaContainer.start();
            ResultSet resultSet = performQuery(scyllaContainer, "SELECT release_version FROM system.local");
            assertTrue(resultSet.wasApplied(), "Query was not applied");
            assertEquals(SCYLLA_CASSANDRA_FAKE_VERSION, resultSet.one().getString(0), "Scylla has wrong version");
            assertTrue(getScyllaVersion(scyllaContainer).startsWith(scyllaVersion), "Scylla has wrong version");
        }
    }

    @Test
    public void testLegacyVersion() throws IOException {
        String scyllaVersion = "3.3.4";
        try (ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>(SCYLLA_IMAGE.withTag(scyllaVersion))) {
            scyllaContainer.start();
            ResultSet resultSet = performQuery(scyllaContainer, "SELECT release_version FROM system.local");
            assertTrue(resultSet.wasApplied(), "Query was not applied");
            assertEquals(SCYLLA_CASSANDRA_FAKE_VERSION, resultSet.one().getString(0), "Scylla has wrong version");
            assertTrue(getScyllaVersion(scyllaContainer).startsWith(scyllaVersion), "Scylla has wrong version");
        }
    }

    @Test
    public void testConfigurationOverride() {
        try (
                ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>(SCYLLA_IMAGE)
                        .withConfigurationOverride("scylla-test-configuration-example")
        ) {
            scyllaContainer.start();
            ResultSet resultSet = performQuery(scyllaContainer, "SELECT cluster_name FROM system.local");
            assertTrue(resultSet.wasApplied(), "Query was not applied");
            assertEquals(TEST_CLUSTER_NAME_IN_CONF, resultSet.one().getString(0), "Scylla configuration is not overridden");
        }
    }

    @Test
    public void testEmptyConfigurationOverride() {
        try (
                ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>(SCYLLA_IMAGE)
                        .withConfigurationOverride("scylla-empty-configuration")
        ) {
            assertThrows(ContainerLaunchException.class, scyllaContainer::start);
        }
    }

    @Test
    public void testInitScript() {
        try (
                ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>(SCYLLA_IMAGE)
                        .withInitScript("initial.cql")
        ) {
            scyllaContainer.start();
            testInitScript(scyllaContainer);
        }
    }

    @Test
    public void testInitScriptWithLegacyScylla() {
        try (
                ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>(DockerImageName.parse("scylladb/scylla:3.3.4"))
                        .withInitScript("initial.cql")
        ) {
            scyllaContainer.start();
            testInitScript(scyllaContainer);
        }
    }

    @Test
    public void testScyllaQueryWaitStrategy() {
        try (
                ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>()
                        .waitingFor(new ScyllaQueryWaitStrategy())
        ) {
            scyllaContainer.start();
            ResultSet resultSet = performQuery(scyllaContainer, "SELECT release_version FROM system.local");
            assertTrue(resultSet.wasApplied(), "Query was not applied");
        }
    }

    @Test
    public void testScyllaGetCluster() {
        try (ScyllaContainer<?> scyllaContainer = new ScyllaContainer<>()) {
            scyllaContainer.start();
            ResultSet resultSet = performQuery(scyllaContainer.getCluster(), "SELECT release_version FROM system.local");
            assertTrue(resultSet.wasApplied(), "Query was not applied");
            assertNotNull(resultSet.one().getString(0), "Result set has no release_version");
        }
    }

    private void testInitScript(ScyllaContainer<?> scyllaContainer) {
        ResultSet resultSet = performQuery(scyllaContainer, "SELECT * FROM keySpaceTest.catalog_category");
        assertTrue(resultSet.wasApplied(), "Query was not applied");
        Row row = resultSet.one();
        assertEquals(1, row.getLong(0), "Inserted row is not in expected state");
        assertEquals("test_category", row.getString(1), "Inserted row is not in expected state");
    }

    private ResultSet performQuery(ScyllaContainer<?> scyllaContainer, String cql) {
        Cluster explicitCluster = Cluster.builder()
                .addContactPoint(scyllaContainer.getHost())
                .withPort(scyllaContainer.getMappedPort(ScyllaContainer.CQL_PORT))
                .build();
        return performQuery(explicitCluster, cql);
    }

    private ResultSet performQuery(Cluster cluster, String cql) {
        try (Cluster closeableCluster = cluster) {
            Session session = closeableCluster.newSession();
            return session.execute(cql);
        }
    }

    private String getScyllaVersion(ScyllaContainer<?> scyllaContainer) throws IOException {
        String host = scyllaContainer.getHost();
        Integer port = scyllaContainer.getMappedPort(ScyllaContainer.SCYLLA_API_PORT);
        URL url = new URL("http://" + host + ":" + port + "/storage_service/scylla_release_version");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try (InputStream responseStream = connection.getInputStream()) {
            return IOUtils.toString(responseStream, StandardCharsets.UTF_8).replaceAll("\"", "");
        }
    }
}
