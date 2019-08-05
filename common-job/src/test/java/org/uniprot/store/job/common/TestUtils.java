package org.uniprot.store.job.common;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/**
 * Used in integration tests to run Spring Batch jobs
 * Created 29/07/19
 *
 * @author Edd
 */
@TestConfiguration
public class TestUtils {
    @Bean
    @Profile("job")
    public JobLauncherTestUtils utils() {
        return new JobLauncherTestUtils();
    }
}
