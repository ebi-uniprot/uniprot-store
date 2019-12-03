package org.uniprot.store.indexer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.uniprot.store.job.common.listener.LogJobListener;

/**
 * Created 11/04/19
 *
 * @author Edd
 */
class IndexerSpringBootApplicationTest {
    private static final int SUCCESS_EXIT_CODE = 0;

    static {
        // ensure default auto-configuration for spring batch, where a runner is created
        // for all jobs in the context. NB. This is disabled for other tests by default
        // because we do not want accidental scanning to cause all jobs found to startup
        System.setProperty("spring.batch.job.enabled", "true");
    }

    @Test
    void successfulSpringBootApplicationHasCorrectExitStatus() {
        assertThat(
                IndexerSpringBootApplication.run(
                        IndexerSpringBootApplicationTest.SuccessfulTestApp.class, new String[] {}),
                is(SUCCESS_EXIT_CODE));
    }

    @Test
    void failedSpringBootApplicationHasCorrectExitStatus() {
        assertThat(
                IndexerSpringBootApplication.run(
                        IndexerSpringBootApplicationTest.FailingTestApp.class, new String[] {}),
                is(not(SUCCESS_EXIT_CODE)));
    }

    @ActiveProfiles(profiles = {"job"})
    @Configuration
    @EnableAutoConfiguration
    @EnableBatchProcessing
    abstract static class TestApp {
        static final int ITEM_COUNT = 10;
        int itemCount = 0;

        @Autowired private JobBuilderFactory jobBuilderFactory;

        @Autowired private StepBuilderFactory steps;

        @Bean
        public Job testJob() throws Exception {
            return this.jobBuilderFactory
                    .get("test job")
                    .start(testStep())
                    .listener(new LogJobListener())
                    .build();
        }

        @Bean
        protected Step testStep() throws Exception {
            return this.steps
                    .get("test step")
                    .<String, String>chunk(1)
                    .reader(getStringItemReader())
                    .writer(createWriter())
                    .build();
        }

        private ItemReader<String> getStringItemReader() {
            return () -> {
                if (itemCount++ < ITEM_COUNT) {
                    return "source item " + itemCount;
                } else {
                    return null;
                }
            };
        }

        abstract ItemWriter<String> createWriter();
    }

    /** This test application reads and writes successfully all items encountered */
    static class SuccessfulTestApp extends TestApp {
        @Override
        ItemWriter<String> createWriter() {
            return System.out::println;
        }

        public static void main(String[] args) {
            System.exit(IndexerSpringBootApplication.run(SuccessfulTestApp.class, args));
        }
    }

    /**
     * This test application reads all items successfully, but fails to write some of the items due
     * to a deliberately thrown {@link Exception}. This simulates an erroneous batch job.
     */
    static class FailingTestApp extends TestApp {
        static final int WHEN_TO_FAIL = 9;

        ItemWriter<String> createWriter() {
            return list -> {
                if (itemCount >= WHEN_TO_FAIL) {
                    throw new HttpSolrClient.RemoteSolrException(
                            "localhost", 999, "No registered leader", new RuntimeException());
                } else {
                    System.out.println(list);
                }
            };
        }

        public static void main(String[] args) {
            System.exit(IndexerSpringBootApplication.run(FailingTestApp.class, args));
        }
    }
}
