package org.uniprot.store.indexer.taxonomy.processor;

import static org.uniprot.store.indexer.taxonomy.TaxonomySQLConstants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.*;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryImpl;
import org.uniprot.core.taxonomy.impl.TaxonomyStrainBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.taxonomy.readers.*;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocumentConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxonomyProcessor implements ItemProcessor<TaxonomyEntry, TaxonomyDocument> {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper jsonMapper;
    private final UniProtSolrClient uniProtSolrClient;
    private final TaxonomyDocumentConverter documentConverter;

    public TaxonomyProcessor(DataSource readDataSource, UniProtSolrClient uniProtSolrClient) {
        this.jdbcTemplate = new JdbcTemplate(readDataSource);
        this.uniProtSolrClient = uniProtSolrClient;
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
        documentConverter = new TaxonomyDocumentConverter(jsonMapper);
    }

    @Override
    public TaxonomyDocument process(TaxonomyEntry entry) throws Exception {
        long taxonId = entry.getTaxonId();
        if (taxonId == 1L) {
            return null;
        }
        TaxonomyEntryBuilder entryBuilder = TaxonomyEntryBuilder.from(entry);
        SolrQuery query = new SolrQuery("id:" + taxonId);
        Optional<TaxonomyDocument> optionalDocument =
                uniProtSolrClient.queryForObject(
                        SolrCollection.taxonomy, query, TaxonomyDocument.class);
        if (optionalDocument.isPresent()) {
            TaxonomyDocument document = optionalDocument.get();

            byte[] taxonomyObj = document.getTaxonomyObj();
            TaxonomyEntry statisticsEntry =
                    jsonMapper.readValue(taxonomyObj, TaxonomyEntryImpl.class);
            entryBuilder.statistics(statisticsEntry.getStatistics());
        }
        entryBuilder.hostsSet(loadVirusHosts(taxonId));
        entryBuilder.otherNamesSet(loadOtherNames(taxonId));
        List<TaxonomyLineage> lineage = loadLineage(taxonId);
        entryBuilder.lineagesSet(lineage);
        entryBuilder.strainsSet(loadStrains(taxonId));
        entryBuilder.linksSet(loadLinks(taxonId));

        long parentTaxId = entry.getParent().getTaxonId();
        if (entry.hasParent() && lineage != null) {
            lineage.stream()
                    .filter(ln -> ln.getTaxonId() == parentTaxId)
                    .findFirst()
                    .map(this::getParentFromLineage)
                    .ifPresent(entryBuilder::parent);
        }
        entry = entryBuilder.build();
        TaxonomyDocument.TaxonomyDocumentBuilder docBuilder = documentConverter.convert(entry);
        if (entry.hasStatistics()) {
            List<String> taxonomiesWith = new ArrayList<>();
            TaxonomyStatistics statistics = entry.getStatistics();
            if (statistics.hasReviewedProteinCount()) {
                taxonomiesWith.add("1_uniprotkb");
                taxonomiesWith.add("2_reviewed");
            }
            if (statistics.hasUnreviewedProteinCount() && !statistics.hasReviewedProteinCount()) {
                taxonomiesWith.add("1_uniprotkb");
                taxonomiesWith.add("3_unreviewed");
            }
            if (statistics.hasReferenceProteomeCount()) {
                taxonomiesWith.add("4_reference");
                taxonomiesWith.add("5_proteome");
            }
            if (statistics.hasProteomeCount() && !statistics.hasReferenceProteomeCount()) {
                taxonomiesWith.add("5_proteome");
            }
            docBuilder.taxonomiesWith(taxonomiesWith);
        }
        TaxonomyDocument result = null;
        if (docBuilder != null) {
            result = docBuilder.build();
        }
        return result;
    }

    protected String getTaxonomyLineageSQL() {
        return SELECT_TAXONOMY_LINEAGE_SQL;
    }

    private Taxonomy getParentFromLineage(TaxonomyLineage parentLineage) {
        return new TaxonomyBuilder()
                .taxonId(parentLineage.getTaxonId())
                .scientificName(parentLineage.getScientificName())
                .commonName(parentLineage.getCommonName())
                .synonymsSet(parentLineage.getSynonyms())
                .build();
    }

    private List<String> loadLinks(long taxonId) {
        return jdbcTemplate.query(SELECT_TAXONOMY_LINKS_SQL, new TaxonomyURLReader(), taxonId);
    }

    private List<String> loadOtherNames(long taxonId) {
        return jdbcTemplate.query(
                SELECT_TAXONOMY_OTHER_NAMES_SQL, new TaxonomyNamesReader(), taxonId);
    }

    private List<Taxonomy> loadVirusHosts(long taxonId) {
        return jdbcTemplate.query(
                SELECT_TAXONOMY_HOSTS_SQL, new TaxonomyVirusHostReader(), taxonId);
    }

    private List<TaxonomyLineage> loadLineage(long taxonId) {
        List<List<TaxonomyLineage>> result =
                jdbcTemplate.query(getTaxonomyLineageSQL(), new TaxonomyLineageReader(), taxonId);
        if (Utils.notNullNotEmpty(result)) {
            return result.get(0);
        }
        return null;
    }

    private List<TaxonomyStrain> loadStrains(long taxonId) {
        List<TaxonomyStrain> result = new ArrayList<>();
        List<TaxonomyStrainReader.Strain> strains =
                jdbcTemplate.query(
                        SELECT_TAXONOMY_STRAINS_SQL, new TaxonomyStrainReader(), taxonId);

        strains.stream()
                .collect(Collectors.groupingBy(TaxonomyStrainReader.Strain::getId))
                .values()
                .forEach(
                        strainList -> {
                            TaxonomyStrainBuilder builder = new TaxonomyStrainBuilder();
                            for (TaxonomyStrainReader.Strain strain : strainList) {
                                if (strain.getNameClass()
                                        .equals(
                                                TaxonomyStrainReader.StrainNameClass
                                                        .scientific_name)) {
                                    builder.name(strain.getName());
                                } else {
                                    builder.synonymsAdd(strain.getName());
                                }
                            }
                            result.add(builder.build());
                        });
        return result;
    }
}
