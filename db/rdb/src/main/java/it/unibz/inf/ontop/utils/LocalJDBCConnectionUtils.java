package it.unibz.inf.ontop.utils;

import it.unibz.inf.ontop.injection.OntopSQLCredentialSettings;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class LocalJDBCConnectionUtils {

    /**
     * Brings robustness to some Tomcat classloading issues.
     */
    public static Connection createConnection(OntopSQLCredentialSettings settings) throws SQLException {

        try {
            // This should work in most cases (e.g. from CLI, Protege, or Jetty)
            return DriverManager.getConnection(settings.getJdbcUrl(), settings.getJdbcUser(), settings.getJdbcPassword());
        } catch (SQLException ex) {
            // HACKY(xiao): This part is still necessary for Tomcat.
            // Otherwise, JDBC drivers are not initialized by default.
            try {
                Class.forName(settings.getJdbcDriver());

                // Kafka (ksql) Driver needs "special" registering
                if((settings.getJdbcDriver().equals("com.github.mmolimar.ksql.jdbc.KsqlDriver"))){
                    //Driver ksqlDriver = new com.github.mmolimar.ksql.jdbc.KsqlDriver();
                    //DriverManager.registerDriver(ksqlDriver);
                }
            } catch (ClassNotFoundException e) {
                throw new SQLException("Cannot load the driver: " + e.getMessage());
            }
            return DriverManager.getConnection(settings.getJdbcUrl(), settings.getJdbcUser(), settings.getJdbcPassword());
        }
    }
}
