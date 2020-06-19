package org.uniprot.store.datastore.voldemort.data.performance;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Created 19/06/2020
 *
 * @author Edd
 */
class StatisticsSummaryTest {
    @Test
    void canPrintSummaries() {
        StatisticsSummary summary = new StatisticsSummary(Collections.singletonList("uniprotkb"));
        summary.getTotalRequested().set(20);
        summary.getTotalFailed("uniprotkb").set(5);
        summary.getTotalFetchDuration("uniprotkb").set(150L);
        summary.getTotalRetrieved("uniprotkb").set(15);

        String message = summary.getMessage();

        assertThat(message, containsString("uniprotkb"));
        assertThat(message, containsString("\t\tTotal retrieved successfully = 15"));
        assertThat(message, containsString("\t\tTotal failed = 5 (25.0 %)"));
        assertThat(message, containsString("\t\tAve successful request duration (ms) = 10"));
    }
}
