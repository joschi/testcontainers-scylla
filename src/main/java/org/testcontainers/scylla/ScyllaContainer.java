package org.testcontainers.scylla;

import com.datastax.driver.core.Cluster;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.commons.io.IOUtils;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.scylla.delegate.ScyllaDatabaseDelegate;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.ext.ScriptUtils.ScriptLoadException;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.script.ScriptException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Scylla container
 * <p>
 * Supports 2.x and 3.x Scylla versions
 *
 * @author Eugeny Karpov
 */
public class ScyllaContainer<SELF extends ScyllaContainer<SELF>> extends GenericContainer<SELF> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("scylladb/scylla");
    private static final String DEFAULT_TAG = "4.1.8";

    @Deprecated
    public static final String IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();

    public static final Integer CQL_PORT = 9042;
    public static final Integer THRIFT_PORT = 9160;
    public static final Integer JMX_PORT = 7199;
    public static final Integer SCYLLA_API_PORT = 10000;

    private static final String CONTAINER_CONFIG_LOCATION = "/etc/scylla";
    private static final String USERNAME = "scylla";
    private static final String PASSWORD = "scylla";

    private String configLocation;
    private String initScriptPath;
    private boolean enableJmxReporting;

    public ScyllaContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    /**
     * @deprecated Use {@link #ScyllaContainer(DockerImageName)}
     */
    @Deprecated
    public ScyllaContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public ScyllaContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        addExposedPort(CQL_PORT);
        addExposedPort(THRIFT_PORT);
        addExposedPort(JMX_PORT);
        addExposedPort(SCYLLA_API_PORT);

        setCommand("--disable-version-check", "--api-address", "0.0.0.0");
        setStartupAttempts(3);
        this.enableJmxReporting = false;
    }

    @Override
    protected void configure() {
        optionallyMapResourceParameterAsVolume(CONTAINER_CONFIG_LOCATION, configLocation);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        runInitScriptIfRequired();
    }

    /**
     * Load init script content and apply it to the database if initScriptPath is set
     */
    private void runInitScriptIfRequired() {
        if (initScriptPath != null) {
            try {
                URL resource = Thread.currentThread().getContextClassLoader().getResource(initScriptPath);
                if (resource == null) {
                    logger().warn("Could not load classpath init script: {}", initScriptPath);
                    throw new ScriptLoadException("Could not load classpath init script: " + initScriptPath + ". Resource not found.");
                }
                String cql = IOUtils.toString(resource, StandardCharsets.UTF_8);



                DatabaseDelegate databaseDelegate = getDatabaseDelegate();
                ScriptUtils.executeDatabaseScript(databaseDelegate, initScriptPath, cql);
            } catch (IOException e) {
                logger().warn("Could not load classpath init script: {}", initScriptPath);
                throw new ScriptLoadException("Could not load classpath init script: " + initScriptPath, e);
            } catch (ScriptException e) {
                logger().error("Error while executing init script: {}", initScriptPath, e);
                throw new ScriptUtils.UncategorizedScriptException("Error while executing init script: " + initScriptPath, e);
            }
        }
    }

    /**
     * Map (effectively replace) directory in Docker with the content of resourceLocation if resource location is not null
     * <p>
     * Protected to allow for changing implementation by extending the class
     *
     * @param pathNameInContainer path in docker
     * @param resourceLocation    relative classpath to resource
     */
    protected void optionallyMapResourceParameterAsVolume(String pathNameInContainer, String resourceLocation) {
        Optional.ofNullable(resourceLocation)
                .map(MountableFile::forClasspathResource)
                .ifPresent(mountableFile -> withCopyFileToContainer(mountableFile, pathNameInContainer));
    }

    /**
     * Initialize Scylla with the custom overridden Scylla configuration
     * <p>
     * Be aware, that Docker effectively replaces all /etc/scylla content with the content of config location, so if
     * Scylla.yaml in configLocation is absent or corrupted, then Scylla just won't launch
     *
     * @param configLocation relative classpath with the directory that contains scylla.yaml and other configuration files
     */
    public SELF withConfigurationOverride(String configLocation) {
        this.configLocation = configLocation;
        return self();
    }

    /**
     * Initialize Scylla with init CQL script
     * <p>
     * CQL script will be applied after container is started (see using WaitStrategy)
     *
     * @param initScriptPath relative classpath resource
     */
    public SELF withInitScript(String initScriptPath) {
        this.initScriptPath = initScriptPath;
        return self();
    }

    /**
     * Initialize Scylla client with JMX reporting enabled or disabled
     */
    public SELF withJmxReporting(boolean enableJmxReporting) {
        this.enableJmxReporting = enableJmxReporting;
        return self();
    }

    /**
     * Get username
     * <p>
     * By default Scylla has authenticator: AllowAllAuthenticator in scylla.yaml
     * If username and password need to be used, then authenticator should be set as PasswordAuthenticator
     * (through custom Scylla configuration) and through CQL with default scylla-scylla credentials
     * user management should be modified
     */
    public String getUsername() {
        return USERNAME;
    }

    /**
     * Get password
     * <p>
     * By default Scylla has authenticator: AllowAllAuthenticator in scylla.yaml
     * If username and password need to be used, then authenticator should be set as PasswordAuthenticator
     * (through custom Scylla configuration) and through CQL with default scylla-scylla credentials
     * user management should be modified
     */
    public String getPassword() {
        return PASSWORD;
    }

    /**
     * Get configured Cluster
     * <p>
     * Can be used to obtain connections to Scylla in the container
     */
    public Cluster getCluster() {
        return getCluster(this, enableJmxReporting);
    }

    public static Cluster getCluster(ContainerState containerState, boolean enableJmxReporting) {
        final Cluster.Builder builder = Cluster.builder()
                .addContactPoint(containerState.getHost())
                .withPort(containerState.getMappedPort(CQL_PORT));
        if (!enableJmxReporting) {
            builder.withoutJMXReporting();
        }
        return builder.build();
    }

    public static Cluster getCluster(ContainerState containerState) {
        return getCluster(containerState, false);
    }

    private DatabaseDelegate getDatabaseDelegate() {
        return new ScyllaDatabaseDelegate(this);
    }
}
