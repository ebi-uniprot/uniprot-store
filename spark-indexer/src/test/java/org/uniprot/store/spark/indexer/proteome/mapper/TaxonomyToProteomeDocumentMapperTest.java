package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

class TaxonomyToProteomeDocumentMapperTest {
    private static final long TAXON_ID_0 = 0L;
    private static final long TAXON_ID_1 = 1L;
    private static final long TAXON_ID_2 = 2L;
    private static final String SCIENTIFIC_NAME_0 = "scientificName0";
    private static final String SCIENTIFIC_NAME_1 = "scientificName1";
    private static final String SCIENTIFIC_NAME_2 = "scientificName2";
    private static final String COMMON_NAME_0 = "commonName0";
    private static final String COMMON_NAME_1 = "commonName1";
    private static final String COMMON_NAME_2 = "commonName2";
    private static final String MNEMONIC_1 = "mnemonic1";
    private final List<String> synonyms0 = List.of("someSynonym0");
    private final List<String> synonyms1 = List.of("someSynonym1");
    private final List<String> synonyms2 = List.of("someSynonym2");
    private final TaxonomyLineage taxLineage0 =
            new TaxonomyLineageBuilder()
                    .taxonId(TAXON_ID_0)
                    .scientificName(SCIENTIFIC_NAME_0)
                    .commonName(COMMON_NAME_0)
                    .build();
    private final TaxonomyEntry taxEntry0 =
            new TaxonomyEntryBuilder()
                    .taxonId(TAXON_ID_0)
                    .scientificName(SCIENTIFIC_NAME_0)
                    .commonName(COMMON_NAME_0)
                    .synonymsSet(synonyms0)
                    .build();
    private final TaxonomyEntry taxEntry1 =
            new TaxonomyEntryBuilder()
                    .taxonId(TAXON_ID_1)
                    .scientificName(SCIENTIFIC_NAME_1)
                    .commonName(COMMON_NAME_1)
                    .synonymsSet(synonyms1)
                    .mnemonic(MNEMONIC_1)
                    .lineagesSet(List.of(taxLineage0))
                    .build();
    private final TaxonomyEntry taxEntry2 =
            new TaxonomyEntryBuilder()
                    .taxonId(TAXON_ID_2)
                    .scientificName(SCIENTIFIC_NAME_2)
                    .commonName(COMMON_NAME_2)
                    .synonymsSet(synonyms2)
                    .build();
    private final Map<String, TaxonomyEntry> taxonomyEntries =
            Stream.of(taxEntry0, taxEntry1, taxEntry2)
                    .collect(
                            Collectors.toMap(
                                    t -> String.valueOf(t.getTaxonId()), Function.identity()));
    private final ProteomeDocument proteomeDocument = new ProteomeDocument();
    private final TaxonomyToProteomeDocumentMapper taxonomyToProteomeDocumentMapper =
            new TaxonomyToProteomeDocumentMapper(taxonomyEntries);

    @Test
    void call() throws Exception {
        proteomeDocument.organismTaxId = (int) TAXON_ID_1;

        ProteomeDocument proteomeDocumentResult =
                taxonomyToProteomeDocumentMapper.call(proteomeDocument);

        assertEquals("scientificName1 commonName1 so", proteomeDocumentResult.organismSort);
        assertThat(
                proteomeDocumentResult.organismName,
                contains("scientificName1", "commonName1", "someSynonym1", "mnemonic1"));
        assertThat(
                proteomeDocumentResult.organismTaxon,
                contains(
                        "scientificName1",
                        "commonName1",
                        "someSynonym1",
                        "mnemonic1",
                        "scientificName0",
                        "commonName0"));
        assertThat(
                proteomeDocumentResult.taxLineageIds, contains((int) TAXON_ID_1, (int) TAXON_ID_0));
    }

    @Test
    void call_whenOrganismHasNoLineage() throws Exception {
        proteomeDocument.organismTaxId = (int) TAXON_ID_0;

        ProteomeDocument proteomeDocumentResult =
                taxonomyToProteomeDocumentMapper.call(proteomeDocument);

        assertEquals("scientificName0 commonName0 so", proteomeDocumentResult.organismSort);
        assertThat(
                proteomeDocumentResult.organismName,
                contains("scientificName0", "commonName0", "someSynonym0"));
        assertThat(
                proteomeDocumentResult.organismTaxon,
                contains("scientificName0", "commonName0", "someSynonym0"));
        assertThat(proteomeDocumentResult.taxLineageIds, contains((int) TAXON_ID_0));
    }
}
