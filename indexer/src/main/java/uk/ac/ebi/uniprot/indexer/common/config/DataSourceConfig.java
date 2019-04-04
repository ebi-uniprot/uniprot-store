package uk.ac.ebi.uniprot.indexer.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;

/**
 *
 * @author lgonzales
 */
@Configuration
public class DataSourceConfig {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceConfig.class);

    @Value(("${database.url}"))
    private String databaseURL;

    @Value(("${database.user.name}"))
    private String databaseUserName;

    @Value(("${database.password}"))
    private String databasePassword;

    @Value(("${database.driver.class.name}"))
    private String databaseDriverClassName;

    @Primary
    @Bean(destroyMethod = "shutdown")
    public EmbeddedDatabase dataSourceH2() {
        logger.info("Initializing embedded H2 database");
        return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql").build();
    }

    @Bean(name = "readDataSource")
    public DataSource readDataSource(){
        DriverManagerDataSource ds = new DriverManagerDataSource(databaseURL, databaseUserName, databasePassword);
        ds.setDriverClassName(databaseDriverClassName);
        logger.info("Initializing readDataSource for "+databaseURL);
        return ds;
    }
}
