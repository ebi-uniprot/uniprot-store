package org.uniprot.store.spark.indexer.uniprot.converter;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.taxonomy.Organism;
import org.uniprot.core.uniprot.taxonomy.OrganismHost;
import org.uniprot.core.uniprot.taxonomy.builder.OrganismBuilder;
import org.uniprot.core.uniprot.taxonomy.builder.OrganismHostBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-06
 */
class UniProtEntryTaxonomyConverterTest {

    @Test
    void convertOrganism() {
        // given
        Organism organism = new OrganismBuilder().taxonId(9606L).build();
        UniProtDocument uniProtDocument = new UniProtDocument();

        UniProtEntryTaxonomyConverter converter = new UniProtEntryTaxonomyConverter();

        // when
        converter.convertOrganism(organism, uniProtDocument);

        // then
        assertEquals(9606, uniProtDocument.organismTaxId);

        // content for default search
        assertEquals(singleton("9606"), uniProtDocument.content);
    }

    @Test
    void convertSingleOrganismHost() {
        OrganismHost organismHost = new OrganismHostBuilder().taxonId(9606L).build();

        // objects that will be updated in the convertion method
        UniProtDocument uniProtDocument = new UniProtDocument();

        UniProtEntryTaxonomyConverter converter = new UniProtEntryTaxonomyConverter();

        // when
        converter.convertOrganismHosts(singletonList(organismHost), uniProtDocument);

        // then
        assertEquals(singletonList(9606), uniProtDocument.organismHostIds);

        // content for default search
        assertEquals(singleton("9606"), uniProtDocument.content);
    }

    @Test
    void convertMultipleOrganismHosts() {
        OrganismHost organismHost = new OrganismHostBuilder().taxonId(9606L).build();
        OrganismHost otherOrganismHost = new OrganismHostBuilder().taxonId(9000L).build();

        // objects that will be updated in the convertion method
        UniProtDocument uniProtDocument = new UniProtDocument();

        UniProtEntryTaxonomyConverter converter = new UniProtEntryTaxonomyConverter();

        // when
        converter.convertOrganismHosts(asList(organismHost, otherOrganismHost), uniProtDocument);

        // then
        assertEquals(asList(9606, 9000), uniProtDocument.organismHostIds);

        // content for default search
        assertEquals(new HashSet<>(asList("9606", "9000")), uniProtDocument.content);
    }
}
