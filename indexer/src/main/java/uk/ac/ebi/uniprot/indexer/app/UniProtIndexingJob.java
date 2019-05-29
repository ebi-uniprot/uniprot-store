package uk.ac.ebi.uniprot.indexer.app;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created 28/05/19
 *
 * @author Edd
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
// Add extra packages below if all beans in it are required for all jobs
@ComponentScan(basePackages = {
        // job/step listeners that are useful for all jobs
        "uk.ac.ebi.uniprot.indexer.common.listener",

        // this package defines all job configs, but only 1 is enabled
        // via @ConditionalOnProperty annotation
        "uk.ac.ebi.uniprot.indexer.app"
})
@Configuration
public @interface UniProtIndexingJob {
}
