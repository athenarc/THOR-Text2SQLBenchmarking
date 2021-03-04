package shared.database.connectivity;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.apache.commons.dbcp.BasicDataSource;

import shared.database.config.DatabaseConfigurations;
import shared.database.config.PropertiesSingleton;
import shared.database.model.DatabaseType;

/**
 * This class is the main Connection provider for our Systems.
 * First loadDBConfigurations and then getConnection from the pool.
 */
public class DataSourceFactory {
    private static final Logger LOGGER = Logger.getLogger(DataSourceFactory.class.getName());  // The LOGGER
    private static BasicDataSource ds = null;                                                  // The DataSource Object
    private static DatabaseType type = null;                                                   // The database type {psql, mysql}


    /**
     * Loads the connection properties and instantiates a {@link BasicDataSource} object.
     * After calling this method, using 'getConnection' will yield a {@link Connection} object.
     *
     * @NOTE: The database connection configurations are not necessary because this method will use the ones
     * stored in the {@link PropertiesSingleton} class.
     *
     * @param databaseName The name of the database
     * @param DatabaseType The type of the underlying database (see {@link DatabaseType})
     */
    public static void loadConnectionProperties(String databaseName, DatabaseType type) {
        if (PropertiesSingleton.getBundle() != null) {
            DatabaseConfigurations configs = new DatabaseConfigurations( PropertiesSingleton.getBundle(), databaseName, type);
            DataSourceFactory.instantiateDataSource(configs);
        }
        else {
            throw new RuntimeException("[ERR] Uninitialized Configurations. Please call PropertiesSingleton.loadProperties(<file_name>) to initialize them");
        }
    }

    /**
     * Loads the connection properties and instantiates a {@link BasicDataSource} object.
     * After calling this method, using 'getConnection' will yield a {@link Connection} object.
     *
     * @param configs An instantiated {@link DatabaseConfigurations} object containing all info about the database.
     *
     */
    public static void loadConnectionProperties(DatabaseConfigurations configs) {
        DataSourceFactory.instantiateDataSource(configs);
    }


     /**
     * Creates a {@link BasicDataSource} instance using the an instance of the class {@link DatabaseConfigurations}
     */
    private static void instantiateDataSource(DatabaseConfigurations config) {
        // Get the connection parameters.
        if (config.isAssigned()) {
            ds = new BasicDataSource();
            ds.setUrl(config.getFormattedURL());
            ds.setDriverClassName(config.getDriver());
            ds.setUsername(config.getUserName());
            ds.setPassword(config.getPassword());
            ds.setMinIdle(5);
            ds.setMaxIdle(10);
            ds.setMaxOpenPreparedStatements(100);
            type = config.getType();
        }
        else {
            LOGGER.info("[ERR] Configuration Object not assigned");
        }
    }

    // ----------------------

    /**
     * Return a connection with the database specified by the configurations loaded.
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    /**
     * @return the Database type {psql, mysql}
     */
    public static DatabaseType getType() {
        return type;
    }
}
