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
@ComponentScan(basePackages = "uk.ac.ebi.uniprot.indexer.app")
@Configuration
public @interface UniProtIndexingJob {
}
