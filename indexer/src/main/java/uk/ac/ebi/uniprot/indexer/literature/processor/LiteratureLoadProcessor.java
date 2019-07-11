package uk.ac.ebi.uniprot.indexer.literature.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import uk.ac.ebi.uniprot.domain.citation.Author;
import uk.ac.ebi.uniprot.domain.literature.LiteratureEntry;
import uk.ac.ebi.uniprot.domain.literature.LiteratureMappedReference;
import uk.ac.ebi.uniprot.domain.literature.LiteratureStatistics;
import uk.ac.ebi.uniprot.domain.literature.builder.LiteratureEntryBuilder;
import uk.ac.ebi.uniprot.domain.literature.impl.LiteratureEntryImpl;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtAccession;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.json.parser.literature.LiteratureJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.literature.LiteratureDocument;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 */
@Slf4j
public class LiteratureLoadProcessor implements ItemProcessor<LiteratureEntry, LiteratureDocument> {

    private final UniProtSolrOperations solrOperations;
    private final ObjectMapper literatureObjectMapper;

    public LiteratureLoadProcessor(UniProtSolrOperations solrOperations) {
        this.solrOperations = solrOperations;
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureEntry entry) throws Exception {
        LiteratureEntryBuilder entryBuilder = new LiteratureEntryBuilder().from(entry);
        Query query = new SimpleQuery().addCriteria(Criteria.where("id").is(entry.getPubmedId()));
        Optional<LiteratureDocument> optionalDocument = solrOperations.queryForObject(SolrCollection.literature.name(), query, LiteratureDocument.class);
        if (optionalDocument.isPresent()) {
            LiteratureDocument document = optionalDocument.get();

            //Get statistics and mapped references from previous steps and copy it to entry builder
            byte[] literatureObj = document.getLiteratureObj().array();
            LiteratureEntry statisticsEntry = literatureObjectMapper.readValue(literatureObj, LiteratureEntryImpl.class);
            entryBuilder.statistics(statisticsEntry.getStatistics());
            entryBuilder.literatureMappedReference(statisticsEntry.getLiteratureMappedReferences());

            solrOperations.delete(SolrCollection.literature.name(), query);
            solrOperations.softCommit(SolrCollection.literature.name());
        }
        return createLiteratureDocument(entryBuilder.build());
    }

    private LiteratureDocument createLiteratureDocument(LiteratureEntry entry) {
        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        Set<String> content = new HashSet<>();
        builder.id(entry.getPubmedId());
        content.add(entry.getPubmedId());

        builder.doi(entry.getDoiId());
        content.add(entry.getDoiId());

        builder.title(entry.getTitle());
        content.add(entry.getTitle());

        if (entry.hasAuthors()) {
            Set<String> authors = entry.getAuthors().stream().map(Author::getValue).collect(Collectors.toSet());
            builder.author(authors);
            content.addAll(authors);
        }
        if (entry.hasJournal()) {
            builder.journal(entry.getJournal().getName());
            content.add(entry.getJournal().getName());
        }
        if (entry.hasPublicationDate()) {
            builder.published(entry.getPublicationDate().getValue());
        }
        if (entry.hasStatistics()) {
            LiteratureStatistics statistics = entry.getStatistics();
            builder.mappedin(statistics.hasMappedProteinCount());
            builder.citedin(statistics.hasReviewedProteinCount() || statistics.hasUnreviewedProteinCount());
        }

        if (entry.hasLiteratureAbstract()) {
            content.add(entry.getLiteratureAbstract());
        }
        if (entry.hasAuthoringGroup()) {
            content.addAll(entry.getAuthoringGroup());
        }
        builder.content(content);

        if (entry.hasLiteratureMappedReferences()) {
            Set<String> uniprotAccessions = entry.getLiteratureMappedReferences().stream()
                    .filter(LiteratureMappedReference::hasUniprotAccession)
                    .map(LiteratureMappedReference::getUniprotAccession)
                    .map(UniProtAccession::getValue)
                    .collect(Collectors.toSet());
            builder.mappedProteins(uniprotAccessions);
        }

        byte[] literatureByte = getLiteratureObjectBinary(entry);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));

        log.debug("LiteratureLoadProcessor entry: " + entry);
        return builder.build();
    }

    private byte[] getLiteratureObjectBinary(LiteratureEntry literature) {
        try {
            return this.literatureObjectMapper.writeValueAsBytes(literature);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse Literature to binary json: ", e);
        }
    }

}
