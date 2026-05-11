# UniProt Store

UniProt Store is a Java 17 multi-module Maven project for building and using UniProt
search indexes, Solr collection configuration, Spark indexing jobs, and Voldemort-backed
data-store clients.

## Modules

| Module | Purpose |
| --- | --- |
| `index-config` | Assembles Solr collection configuration artifacts. |
| `uniprot-config` | Search-field and return-field configuration JSON and schema support. |
| `uniprot-search` | Shared search model, field mapping, and Solr-related search utilities. |
| `common-job` | Shared Spring Batch job infrastructure used by indexer jobs. |
| `indexer` | Spring Boot/Spring Batch indexer for Solr-backed UniProt collections. |
| `spark-indexer` | Apache Spark jobs for building, validating, and writing index documents. |
| `uniprot-datastore` | Voldemort data-store clients, builders, validators, and related utilities. |
| `integration-test` | Integration test support and resources. |
| `jacoco-aggregate-report` | Aggregates JaCoCo coverage across modules. |

## Requirements

- JDK 17
- Maven 3.8+
- Access to UniProt Maven Artifactory repositories for private dependencies
- Brotli installed locally when running the same full build path as CI
- Solr 8.11.x for local indexing/search workflows
- Apache Spark 3.3.x for `spark-indexer` jobs

The root `pom.xml` declares the UniProt Artifactory repositories used by the build.
If your environment requires credentials, configure them in your Maven `settings.xml`
using the repository ids from `pom.xml`, such as:

- `uniprot-artifactory-release`
- `uniprot-artifactory-snapshots`
- `uniprot-artifactory-private-thirdparty`

## Build

Build all modules and run tests:

```bash
mvn clean install
```

Build without tests:

```bash
mvn clean install -DskipTests
```

Build a single module and the modules it depends on:

```bash
mvn -pl indexer -am clean install
```

Run the CodeQL-style package build used by GitHub Actions:

```bash
mvn -B -T 1C package -DskipTests
```

The build runs Spotless formatting during `compile`. To disable that behavior for a
local build, use the `no-spotless` profile:

```bash
mvn clean install -Pno-spotless
```

## Tests

Run the unit/integration test lifecycle:

```bash
mvn verify
```

Run tests for a specific module:

```bash
mvn -pl uniprot-search test
```

CI runs Maven with additional UniProt resource configuration and then publishes the
aggregate JaCoCo report from:

```text
jacoco-aggregate-report/target/site/jacoco-aggregate/jacoco.xml
```

## Configuration

Runtime defaults are kept in module resource files:

- `indexer/src/main/resources/application.properties`
- `spark-indexer/src/main/resources/application.properties`
- `uniprot-datastore/src/main/resources/application.properties`

Several values are placeholders or point at developer-local test files. Before running
jobs against real data, provide environment-specific overrides for database connection
details, Solr hosts or ZooKeeper hosts, Voldemort store hosts, input directories, and
release file locations.

For Spring Boot jobs, standard Spring property override mechanisms apply, for example:

```bash
java -jar indexer/target/uniprot-indexer-*.jar \
  --spring.data.solr.httphost=http://localhost:8983/solr/ \
  --uniprot.job.name=uniprotkb
```

## Running Jobs

The `indexer` module provides the Spring Boot entry point:

```text
org.uniprot.store.indexer.IndexerSpringBootApplication
```

Select the job with `uniprot.job.name` and provide the required input, database, and
Solr properties for the target collection.

The `spark-indexer` module contains Spark entry points for full indexing, HPS document
generation, Solr indexing, and validation. Common main classes include:

- `org.uniprot.store.spark.indexer.main.IndexDataStoreMain`
- `org.uniprot.store.spark.indexer.main.WriteIndexDocumentsToHPSMain`
- `org.uniprot.store.spark.indexer.main.IndexHPSDocumentsInSolrMain`
- `org.uniprot.store.spark.indexer.main.SolrIndexValidatorMain`
- `org.uniprot.store.spark.indexer.validator.ValidateHPSDocumentsMain`

Most Spark entry points expect release name, collection names, Spark master URL, and
taxonomy database arguments. Check the relevant `main` method before running a job,
because argument counts differ by entry point.

The `uniprot-datastore` module provides Voldemort client and builder utilities,
including:

- `org.uniprot.store.datastore.voldemort.client.impl.UniProtClientMain`
- `org.uniprot.store.datastore.voldemort.data.validator.UniprotKBEntryRetrieveParseVerifier`

## CI

The GitLab pipeline uses `maven:3.8.5-openjdk-17`, installs Brotli, retrieves Maven
settings from the UniProt configuration project, runs `mvn install`, publishes JUnit
reports, runs SonarCloud analysis, and deploys snapshots/releases from `main`.

GitHub Actions runs CodeQL for Java/Kotlin with JDK 17 and a manual Maven package
build.

## Repository Notes

- The checked-in JavaScript files are Solr update scripts, not a Node.js application.
- Generated build outputs belong under module `target/` directories.
- Keep secrets and environment-specific Maven settings outside the repository.
