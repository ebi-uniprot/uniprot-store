package org.uniprot.store.indexer.test.config;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import lombok.extern.slf4j.Slf4j;

@TestConfiguration
@Slf4j
@Profile("offline")
public class FakeReadDatabaseConfig {

    @Bean(name = "readDataSource", destroyMethod = "shutdown")
    public EmbeddedDatabase readDataSource() {
        log.info(
                "Initializing EmbeddedDatabase Database with necessary table and fake data for tests");
        return new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                .addScript("classpath:database/Schema.sql")
                .addScript("classpath:database/TestData.sql")
                .build();
    }
}
