package org.uniprot.store.indexer.common.config;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/** @author lgonzales */
@Configuration
@Slf4j
@Profile("online")
public class DataSourceConfig {
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
        log.info("Initializing embedded H2 database");
        return new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                .build();
    }

    @Bean(name = "readDataSource")
    public DataSource readDataSource() {
        DriverManagerDataSource ds =
                new DriverManagerDataSource(databaseURL, databaseUserName, databasePassword);
        ds.setDriverClassName(databaseDriverClassName);
        log.info("Initializing readDataSource for " + databaseURL);
        return ds;
    }
}
