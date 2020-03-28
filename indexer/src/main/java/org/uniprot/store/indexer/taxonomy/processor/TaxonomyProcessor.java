package org.uniprot.store.indexer.taxonomy.processor;

import static org.uniprot.store.indexer.taxonomy.TaxonomySQLConstants.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.jdbc.core.JdbcTemplate;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.TaxonomyStrain;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryImpl;
import org.uniprot.core.taxonomy.impl.TaxonomyStrainBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.taxonomy.readers.*;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxonomyProcessor implements ItemProcessor<TaxonomyEntry, TaxonomyDocument> {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper jsonMapper;
    private final UniProtSolrOperations solrOperations;

    public TaxonomyProcessor(DataSource readDataSource, UniProtSolrOperations solrOperations) {
        this.jdbcTemplate = new JdbcTemplate(readDataSource);
        this.solrOperations = solrOperations;
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument process(TaxonomyEntry entry) throws Exception {
        long taxonId = entry.getTaxonId();
        TaxonomyEntryBuilder entryBuilder = TaxonomyEntryBuilder.from(entry);
        Query query = new SimpleQuery().addCriteria(Criteria.where("id").is(taxonId));
        Optional<TaxonomyDocument> optionalDocument =
                solrOperations.queryForObject(
                        SolrCollection.taxonomy.name(), query, TaxonomyDocument.class);
        if (optionalDocument.isPresent()) {
            TaxonomyDocument document = optionalDocument.get();

            byte[] taxonomyObj = document.getTaxonomyObj().array();
            TaxonomyEntry statisticsEntry =
                    jsonMapper.readValue(taxonomyObj, TaxonomyEntryImpl.class);
            entryBuilder.statistics(statisticsEntry.getStatistics());
        }
        entryBuilder.hostsSet(loadVirusHosts(taxonId));
        entryBuilder.otherNamesSet(loadOtherNames(taxonId));
        entryBuilder.lineagesSet(loadLineage(taxonId));
        entryBuilder.strainsSet(loadStrains(taxonId));
        entryBuilder.linksSet(loadLinks(taxonId));

        return buildTaxonomyDocument(entryBuilder.build());
    }

    protected String getTaxonomyLineageSQL() {
        return SELECT_TAXONOMY_LINEAGE_SQL;
    }

    private TaxonomyDocument buildTaxonomyDocument(TaxonomyEntry entry) {
        TaxonomyDocument.TaxonomyDocumentBuilder documentBuilder = TaxonomyDocument.builder();
        documentBuilder.id(String.valueOf(entry.getTaxonId()));
        documentBuilder.taxId(entry.getTaxonId());
        documentBuilder.ancestor(entry.getParentId());

        documentBuilder.scientific(entry.getScientificName());
        documentBuilder.common(entry.getCommonName());
        documentBuilder.mnemonic(entry.getMnemonic());
        documentBuilder.synonym(String.join(", ", entry.getSynonyms()));
        documentBuilder.rank(entry.getRank().toDisplayName());

        documentBuilder.active(entry.isActive());
        documentBuilder.hidden(entry.isHidden());
        documentBuilder.linked(entry.getLinks().size() > 0);

        if (entry.hasStatistics()) {
            TaxonomyStatistics statistics = entry.getStatistics();
            if (statistics.hasReviewedProteinCount()) {
                documentBuilder.reviewed(true);
            }
            if (statistics.hasUnreviewedProteinCount()) {
                documentBuilder.annotated(true);
            }
            if (statistics.hasReferenceProteomeCount()) {
                documentBuilder.reference(true);
            }
            if (statistics.hasProteomeCount()) {
                documentBuilder.proteome(true);
            }
        }

        documentBuilder.host(
                entry.getHosts().stream().map(Taxonomy::getTaxonId).collect(Collectors.toList()));
        documentBuilder.lineage(
                entry.getLineages().stream()
                        .map(TaxonomyLineage::getTaxonId)
                        .collect(Collectors.toList()));
        documentBuilder.strain(buildStrainList(entry.getStrains()));
        documentBuilder.taxonomyObj(getTaxonomyBinary(entry));

        return documentBuilder.build();
    }

    private List<String> buildStrainList(List<TaxonomyStrain> strainList) {
        return strainList.stream()
                .map(
                        strain -> {
                            return strain.getName()
                                    + " ; "
                                    + String.join(" , ", strain.getSynonyms());
                        })
                .collect(Collectors.toList());
    }

    private ByteBuffer getTaxonomyBinary(TaxonomyEntry entry) {
        try {
            return ByteBuffer.wrap(jsonMapper.writeValueAsBytes(entry));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse TaxonomyEntry to binary json: ", e);
        }
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
                        (strainList) -> {
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
