package org.testcontainers.scylla.delegate;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.scylla.ScyllaContainer;
import org.testcontainers.delegate.AbstractDatabaseDelegate;
import org.testcontainers.exception.ConnectionCreationException;
import org.testcontainers.ext.ScriptUtils.ScriptStatementFailedException;

/**
 * Scylla database delegate
 *
 * @author Eugeny Karpov
 * @author Jochen Schalanda
 */
public class ScyllaDatabaseDelegate extends AbstractDatabaseDelegate<Session> {
    private static final Logger log = LoggerFactory.getLogger(ScyllaDatabaseDelegate.class);

    private final ContainerState container;

    public ScyllaDatabaseDelegate(ContainerState container) {
        this.container = container;
    }

    @Override
    protected Session createNewConnection() {
        try {
            return ScyllaContainer.getCluster(container)
                    .newSession();
        } catch (DriverException e) {
            log.error("Could not obtain Scylla connection");
            throw new ConnectionCreationException("Could not obtain Scylla connection", e);
        }
    }

    @Override
    public void execute(String statement, String scriptPath, int lineNumber, boolean continueOnError, boolean ignoreFailedDrops) {
        try {
            ResultSet result = getConnection().execute(statement);
            if (result.wasApplied()) {
                log.debug("Statement {} was applied", statement);
            } else {
                throw new ScriptStatementFailedException(statement, lineNumber, scriptPath);
            }
        } catch (DriverException e) {
            throw new ScriptStatementFailedException(statement, lineNumber, scriptPath, e);
        }
    }

    @Override
    protected void closeConnectionQuietly(Session session) {
        try {
            session.getCluster().close();
        } catch (Exception e) {
            log.error("Could not close Scylla connection", e);
        }
    }
}
