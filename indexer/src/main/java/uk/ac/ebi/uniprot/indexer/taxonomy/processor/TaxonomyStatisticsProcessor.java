package uk.ac.ebi.uniprot.indexer.taxonomy.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyEntryBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyStatisticsBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.impl.TaxonomyEntryImpl;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyStatisticsReader;
import uk.ac.ebi.uniprot.json.parser.taxonomy.TaxonomyJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.util.Optional;

/**
 *
 * @author lgonzales
 */
public class TaxonomyStatisticsProcessor implements ItemProcessor<TaxonomyStatisticsReader.TaxonomyCount, TaxonomyEntry> {

    private final SolrTemplate solrTemplate;
    private final ObjectMapper jsonMapper;

    public TaxonomyStatisticsProcessor(SolrTemplate solrTemplate){
        this.solrTemplate = solrTemplate;
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyEntry process(TaxonomyStatisticsReader.TaxonomyCount taxonomyCount) throws Exception {
        TaxonomyEntry entry = null;
        Query query = new SimpleQuery().addCriteria(Criteria.where("id").is(taxonomyCount.getTaxId()));
        Optional<TaxonomyDocument> optionalDocument = solrTemplate.queryForObject(SolrCollection.taxonomy.name(), query, TaxonomyDocument.class);
        if(optionalDocument.isPresent()) {
            TaxonomyDocument document = optionalDocument.get();

            byte[] taxonomyObj = document.getTaxonomyObj().array();
            entry = jsonMapper.readValue(taxonomyObj, TaxonomyEntryImpl.class);

            TaxonomyStatisticsBuilder statisticsBuilder = new TaxonomyStatisticsBuilder().from(entry.getStatistics());
            statisticsBuilder.reviewedProteinCount(taxonomyCount.getReviewedProteinCount());
            statisticsBuilder.unreviewedProteinCount(taxonomyCount.getUnreviewedProteinCount());
//            statisticsBuilder.proteomeCount(taxonomyCount.getProteomeCount());

            TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder().from(entry);
            builder.statistics(statisticsBuilder.build());

            entry = builder.build();
        }
        return entry;
    }
}
