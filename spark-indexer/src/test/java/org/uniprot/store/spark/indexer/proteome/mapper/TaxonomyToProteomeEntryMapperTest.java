package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import scala.Tuple2;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TaxonomyToProteomeEntryMapperTest {
    public static final long TAXON_ID = 67L;
    public static final long LINEAGE_ID_0 = 333L;
    public static final long LINEAGE_ID_1 = 1L;
    public static final String SCIENTIFIC_NAME = "scientific_name";
    public static final String SCIENTIFIC_NAME_SUPER_KINGDOM = Superkingdom.VIRUSES.getName();
    private final ProteomeEntry proteomeEntry = new ProteomeEntryBuilder().build();
    private final List<TaxonomyLineage> lineages = List.of(
            new TaxonomyLineageBuilder().taxonId(LINEAGE_ID_0).scientificName(SCIENTIFIC_NAME).build(),
            new TaxonomyLineageBuilder().taxonId(LINEAGE_ID_1).scientificName(SCIENTIFIC_NAME_SUPER_KINGDOM).build()
    );
    private final TaxonomyEntry taxonomyEntry = new TaxonomyEntryBuilder().taxonId(TAXON_ID).lineagesSet(lineages).build();
    private final TaxonomyToProteomeEntryMapper taxonomyToProteomeEntryMapper = new TaxonomyToProteomeEntryMapper();

    @Test
    void call() throws Exception {
        Tuple2<ProteomeEntry, Optional<TaxonomyEntry>> proteomeEntryTaxonomyTuple2 = new Tuple2<>(proteomeEntry, Optional.of(taxonomyEntry));

        ProteomeEntry result = taxonomyToProteomeEntryMapper.call(proteomeEntryTaxonomyTuple2);

        assertThat(result, samePropertyValuesAs(ProteomeEntryBuilder.from(proteomeEntry).taxonomy(taxonomyEntry)
                .taxonLineagesSet(taxonomyEntry.getLineages()).superkingdom(Superkingdom.typeOf(SCIENTIFIC_NAME_SUPER_KINGDOM)).build()));
    }

    @Test
    void call_whenLineagesHaveNoSuperKingdom() throws Exception {
        TaxonomyEntry taxonomyEntryWithNoSuperKingdom = TaxonomyEntryBuilder.from(taxonomyEntry).lineagesSet(List.of(new TaxonomyLineageBuilder().taxonId(LINEAGE_ID_0).scientificName(SCIENTIFIC_NAME).build())).build();
        Tuple2<ProteomeEntry, Optional<TaxonomyEntry>> proteomeEntryTaxonomyTuple2 = new Tuple2<>(proteomeEntry, Optional.of(taxonomyEntryWithNoSuperKingdom));

        ProteomeEntry result = taxonomyToProteomeEntryMapper.call(proteomeEntryTaxonomyTuple2);

        assertThat(result, samePropertyValuesAs(ProteomeEntryBuilder.from(proteomeEntry).taxonomy(taxonomyEntryWithNoSuperKingdom)
                .taxonLineagesSet(taxonomyEntryWithNoSuperKingdom.getLineages()).superkingdom(null).build()));
    }

    @Test
    void call_whenTaxonomyIsEmpty() throws Exception {
        Tuple2<ProteomeEntry, Optional<TaxonomyEntry>> proteomeEntryTaxonomyTuple2 = new Tuple2<>(proteomeEntry, Optional.empty());

        ProteomeEntry result = taxonomyToProteomeEntryMapper.call(proteomeEntryTaxonomyTuple2);

        assertEquals(proteomeEntryTaxonomyTuple2._1, result);
    }
}