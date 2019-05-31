package uk.ac.ebi.uniprot.indexer.taxonomy.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import uk.ac.ebi.uniprot.common.Utils;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyLineage;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyStrain;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyEntryBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyStrainBuilder;
import uk.ac.ebi.uniprot.domain.uniprot.taxonomy.Taxonomy;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.*;
import uk.ac.ebi.uniprot.json.parser.taxonomy.TaxonomyJsonConfig;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomySQLConstants.*;

public class TaxonomyProcessor implements ItemProcessor<TaxonomyEntryBuilder, TaxonomyDocument> {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper jsonMapper;

    public TaxonomyProcessor(DataSource readDataSource) {
        this.jdbcTemplate = new JdbcTemplate(readDataSource);
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument process(TaxonomyEntryBuilder entryBuilder) throws Exception {
        long taxonId = entryBuilder.build().getTaxonId();
        entryBuilder.hosts(loadVirusHosts(taxonId));
        entryBuilder.otherNames(loadOtherNames(taxonId));
        entryBuilder.lineage(loadLineage(taxonId));
        entryBuilder.strains(loadStrains(taxonId));
        entryBuilder.links(loadLinks(taxonId));

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

        documentBuilder.host(entry.getHosts().stream().map(Taxonomy::getTaxonId).collect(Collectors.toList()));
        documentBuilder
                .lineage(entry.getLineage().stream().map(TaxonomyLineage::getTaxonId).collect(Collectors.toList()));
        documentBuilder.strain(buildStrainList(entry.getStrains()));
        documentBuilder.taxonomyObj(getTaxonomyBinary(entry));

        return documentBuilder.build();
    }

    private List<String> buildStrainList(List<TaxonomyStrain> strainList) {
        return strainList.stream().map(strain -> {
            return strain.getName() + " ; " + String.join(" , ", strain.getSynonyms());
        }).collect(Collectors.toList());
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
        return jdbcTemplate.query(SELECT_TAXONOMY_OTHER_NAMES_SQL, new TaxonomyNamesReader(), taxonId);
    }


    private List<Taxonomy> loadVirusHosts(long taxonId) {
        return jdbcTemplate.query(SELECT_TAXONOMY_HOSTS_SQL, new TaxonomyVirusHostReader(), taxonId);
    }

    private List<TaxonomyLineage> loadLineage(long taxonId) {
        List<List<TaxonomyLineage>> result = jdbcTemplate
                .query(getTaxonomyLineageSQL(), new TaxonomyLineageReader(), taxonId);
        if (Utils.notEmpty(result)) {
            return result.get(0);
        }
        return null;
    }

    private List<TaxonomyStrain> loadStrains(long taxonId) {
        List<TaxonomyStrain> result = new ArrayList<>();
        List<TaxonomyStrainReader.Strain> strains = jdbcTemplate
                .query(SELECT_TAXONOMY_STRAINS_SQL, new TaxonomyStrainReader(), taxonId);

        strains.stream().collect(Collectors.groupingBy(TaxonomyStrainReader.Strain::getId)).values()
                .forEach((strainList) -> {
                    TaxonomyStrainBuilder builder = new TaxonomyStrainBuilder();
                    for (TaxonomyStrainReader.Strain strain : strainList) {
                        if (strain.getNameClass().equals(TaxonomyStrainReader.StrainNameClass.scientific_name)) {
                            builder.name(strain.getName());
                        } else {
                            builder.addSynonym(strain.getName());
                        }
                    }
                    result.add(builder.build());
                });
        return result;
    }

}
