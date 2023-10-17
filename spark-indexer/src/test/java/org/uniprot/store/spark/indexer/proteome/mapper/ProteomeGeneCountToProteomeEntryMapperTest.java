package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import scala.Tuple2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ProteomeGeneCountToProteomeEntryMapperTest {
    public static final String PROTEOME_ID = "1234";
    public static final int GENE_COUNT = 984;
    private final ProteomeEntry proteomeEntry = new ProteomeEntryBuilder().proteomeId(PROTEOME_ID).build();
    private final ProteomeGeneCountToProteomeEntryMapper proteomeGeneCountToProteomeEntryMapper = new ProteomeGeneCountToProteomeEntryMapper();

    @Test
    void call() throws Exception {
        Tuple2<ProteomeEntry, Optional<Integer>> proteomeEntryProteomeGeneCountTuple2 = new Tuple2<>(proteomeEntry, Optional.of(GENE_COUNT));

        Tuple2<String, ProteomeEntry> result = proteomeGeneCountToProteomeEntryMapper.call(proteomeEntryProteomeGeneCountTuple2);

        assertResult(GENE_COUNT, result);
    }

    private void assertResult(int geneCount, Tuple2<String, ProteomeEntry> result) {
        assertEquals(geneCount, result._2.getGeneCount());
        assertEquals(PROTEOME_ID, result._1);
        assertThat(result._2, samePropertyValuesAs(proteomeEntry, "geneCount"));
    }

    @Test
    void call_whenGeneCountIsEmpty() throws Exception {
        Tuple2<ProteomeEntry, Optional<Integer>> proteomeEntryProteomeGeneCountTuple2 = new Tuple2<>(proteomeEntry, Optional.empty());

        Tuple2<String, ProteomeEntry> result = proteomeGeneCountToProteomeEntryMapper.call(proteomeEntryProteomeGeneCountTuple2);

        assertResult(0, result);
    }
}