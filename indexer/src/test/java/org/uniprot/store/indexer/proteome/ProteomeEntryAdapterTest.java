package org.uniprot.store.indexer.proteome;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.xml.jaxb.proteome.ObjectFactory;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;

/**
 * @author lgonzales
 * @since 19/11/2020
 */
@ExtendWith(SpringExtension.class)
@TestPropertySource(locations = "classpath:application.properties")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProteomeEntryAdapterTest {

    @Value(("${uniprotkb.indexing.taxonomyFile}"))
    private String taxonomyFile;

    @Value(("${proteome.genecentric.canonical.dir.path}"))
    private String geneCentricDir;

    @Value(("${proteome.genecentric.canonical.file.suffix}"))
    private String geneCentricFileSuffix;

    private TaxonomyMapRepo taxonomyRepo;

    @BeforeAll
    void setupTaxonomyRepo() {
        taxonomyRepo = new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }

    @Test
    void adaptValidEntry() {
        Proteome proteomeType = getProteome();
        ProteomeEntryAdapter entryAdapter =
                new ProteomeEntryAdapter(taxonomyRepo, geneCentricDir, geneCentricFileSuffix);
        ProteomeEntry entry = entryAdapter.adaptEntry(proteomeType);
        assertNotNull(entry);
        assertEquals(Superkingdom.BACTERIA, entry.getSuperkingdom());
        assertEquals(4, entry.getGeneCount());
        assertNotNull(entry.getTaxonomy());
        Taxonomy organism = entry.getTaxonomy();
        assertEquals(289376, organism.getTaxonId());
        assertEquals(
                "Thermodesulfovibrio yellowstonii (strain ATCC 51303 / DSM 11347 / YP87)",
                organism.getScientificName());
        assertEquals("THEYD", organism.getMnemonic());
        assertTrue(organism.getCommonName().isEmpty());

        assertNotNull(entry.getTaxonLineages());
        List<TaxonomyLineage> lineages = entry.getTaxonLineages();
        assertEquals(1, lineages.size());

        TaxonomyLineage lineage = lineages.get(0);
        assertEquals(2, lineage.getTaxonId());
        assertEquals("Bacteria", lineage.getScientificName());
        assertEquals("eubacteria", lineage.getCommonName());
        assertEquals(TaxonomyRank.SUPERKINGDOM, lineage.getRank());
        assertFalse(lineage.isHidden());
    }

    @Test
    void adaptWithoutGeneEntry() {
        Proteome proteomeType = getProteome();
        ProteomeEntryAdapter entryAdapter =
                new ProteomeEntryAdapter(taxonomyRepo, "theDir", "suffix.fasta");
        ProteomeEntry entry = entryAdapter.adaptEntry(proteomeType);
        assertEquals(0, entry.getGeneCount());
    }

    private Proteome getProteome() {
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteomeType = xmlFactory.createProteome();
        proteomeType.setUpid("UP000000718");
        proteomeType.setDescription("Proteome Description");
        proteomeType.setTaxonomy(289376L);
        return proteomeType;
    }
}
