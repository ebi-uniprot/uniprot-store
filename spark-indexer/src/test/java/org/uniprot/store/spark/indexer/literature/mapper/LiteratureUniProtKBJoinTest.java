package org.uniprot.store.spark.indexer.literature.mapper;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.JournalArticle;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.Submission;
import org.uniprot.core.citation.impl.JournalArticleBuilder;
import org.uniprot.core.citation.impl.LiteratureBuilder;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 31/03/2021
 */
class LiteratureUniProtKBJoinTest {

    @Test
    void mapWithLiteratureEntryAndLiterature() throws Exception {
        LiteratureUniProtKBJoin mapper = new LiteratureUniProtKBJoin();

        LiteratureStatistics statistics = new LiteratureStatisticsBuilder()
                .reviewedProteinCount(10)
                .build();
        JournalArticle entryCitation = new JournalArticleBuilder().build();
        Optional<LiteratureEntry> entry1 = Optional.of(new LiteratureEntryBuilder()
                .statistics(statistics)
                .citation(entryCitation)
                .build());


        Literature literature = new LiteratureBuilder()
                .title("Tittle")
                .build();
        Optional<Literature> entry2 = Optional.of(literature);

        Tuple2<Optional<LiteratureEntry>, Optional<Literature>> tuple = new Tuple2<>(entry1, entry2);

        LiteratureEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(literature, result.getCitation());
        assertEquals(statistics, result.getStatistics());
    }


    @Test
    void mapOnlyLiteratureEntry() throws Exception {
        LiteratureUniProtKBJoin mapper = new LiteratureUniProtKBJoin();

        LiteratureStatistics statistics = new LiteratureStatisticsBuilder()
                .reviewedProteinCount(10)
                .build();
        JournalArticle entryCitation = new JournalArticleBuilder().build();
        LiteratureEntry litEntry = new LiteratureEntryBuilder()
                .statistics(statistics)
                .citation(entryCitation)
                .build();
        Optional<LiteratureEntry> entry1 = Optional.of(litEntry);

        Tuple2<Optional<LiteratureEntry>, Optional<Literature>> tuple = new Tuple2<>(entry1, Optional.empty());

        LiteratureEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(litEntry, result);
    }

    @Test
    void mapOnlyLiterature() throws Exception {
        LiteratureUniProtKBJoin mapper = new LiteratureUniProtKBJoin();

        Literature literature = new LiteratureBuilder()
                .title("Tittle")
                .build();
        Optional<Literature> entry2 = Optional.of(literature);

        Tuple2<Optional<LiteratureEntry>, Optional<Literature>> tuple = new Tuple2<>(Optional.empty(), entry2);

        LiteratureEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(literature, result.getCitation());
        assertNull(result.getStatistics());
    }

}