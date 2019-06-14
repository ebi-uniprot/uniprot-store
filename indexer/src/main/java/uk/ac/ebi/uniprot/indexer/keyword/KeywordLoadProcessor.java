package uk.ac.ebi.uniprot.indexer.keyword;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.jdbc.core.JdbcTemplate;
import uk.ac.ebi.uniprot.common.Utils;
import uk.ac.ebi.uniprot.cv.keyword.KeywordEntry;
import uk.ac.ebi.uniprot.cv.keyword.impl.KeywordEntryImpl;
import uk.ac.ebi.uniprot.json.parser.keyword.KeywordJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.keyword.KeywordDocument;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lgonzales
 */
public class KeywordLoadProcessor implements ItemProcessor<KeywordEntry, KeywordDocument> {

    private final ObjectMapper keywordObjectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final SolrTemplate solrTemplate;

    public KeywordLoadProcessor(DataSource readDataSource, SolrTemplate solrTemplate) throws SQLException {
        this.jdbcTemplate = new JdbcTemplate(readDataSource);
        this.solrTemplate = solrTemplate;
        this.keywordObjectMapper = KeywordJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public KeywordDocument process(KeywordEntry entry) throws Exception {
        KeywordEntryImpl keywordEntry = (KeywordEntryImpl) entry;

        Query query = new SimpleQuery().addCriteria(Criteria.where("id").is(keywordEntry.getKeyword().getAccession()));
        Optional<KeywordDocument> optionalDocument = solrTemplate.queryForObject(SolrCollection.keyword.name(), query, KeywordDocument.class);
        if (optionalDocument.isPresent()) {
            KeywordDocument document = optionalDocument.get();

            byte[] keywordObj = document.getKeywordObj().array();
            KeywordEntry statisticsEntry = keywordObjectMapper.readValue(keywordObj, KeywordEntryImpl.class);
            keywordEntry.setStatistics(statisticsEntry.getStatistics());

            solrTemplate.delete(SolrCollection.keyword.name(), query);
            solrTemplate.softCommit(SolrCollection.keyword.name());
        }
        return createKeywordDocument(keywordEntry);

    }

    private KeywordDocument createKeywordDocument(KeywordEntryImpl keywordEntry) {
        // content is keyword id + keyword name + definition + synonyms
        List<String> content = new ArrayList<>();
        content.add(keywordEntry.getKeyword().getId());
        content.add(keywordEntry.getKeyword().getAccession());
        content.add(keywordEntry.getDefinition());
        if (Utils.notEmpty(keywordEntry.getSynonyms())) {
            content.addAll(keywordEntry.getSynonyms());
        }

        // create Keyword document
        KeywordDocument.KeywordDocumentBuilder builder = KeywordDocument.builder();
        builder.id(keywordEntry.getKeyword().getAccession());
        builder.name(keywordEntry.getKeyword().getId());
        builder.content(content);

        if (!keywordEntry.getParents().isEmpty()) {
            List<String> parents = keywordEntry.getParents().stream()
                    .flatMap(this::getParentStream)
                    .collect(Collectors.toList());
            builder.parent(parents); // only one level

            List<String> ancestors = keywordEntry.getParents().stream()
                    .flatMap(this::getAncestorsStrem)
                    .collect(Collectors.toList());
            builder.ancestor(ancestors); //recursively navigate over parents...
        }

        byte[] keywordByte = getKeywordObjectBinary(keywordEntry);
        builder.keywordObj(ByteBuffer.wrap(keywordByte));

        return builder.build();
    }

    private Stream<String> getParentStream(KeywordEntry kwEntry) {
        return Stream.of(kwEntry.getKeyword().getId(), kwEntry.getKeyword().getAccession());
    }

    private Stream<String> getAncestorsStrem(KeywordEntry kwEntry) {
        Stream<String> currentParent = getParentStream(kwEntry);
        Stream<String> parentsStream = kwEntry.getParents().stream().flatMap(this::getAncestorsStrem); //recursive call
        return Stream.concat(currentParent, parentsStream);
    }

    private byte[] getKeywordObjectBinary(KeywordEntry keyword) {
        try {
            return this.keywordObjectMapper.writeValueAsBytes(keyword);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse Keyword to binary json: ", e);
        }
    }

}
