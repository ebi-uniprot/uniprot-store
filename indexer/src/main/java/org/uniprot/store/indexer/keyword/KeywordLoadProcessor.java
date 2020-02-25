package org.uniprot.store.indexer.keyword;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.builder.KeywordEntryBuilder;
import org.uniprot.core.cv.keyword.builder.KeywordEntryImpl;
import org.uniprot.core.json.parser.keyword.KeywordJsonConfig;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.keyword.KeywordDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
public class KeywordLoadProcessor implements ItemProcessor<KeywordEntry, KeywordDocument> {

    private final ObjectMapper keywordObjectMapper;
    private final UniProtSolrOperations solrOperations;

    public KeywordLoadProcessor(UniProtSolrOperations solrOperations) throws SQLException {
        this.solrOperations = solrOperations;
        this.keywordObjectMapper = KeywordJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public KeywordDocument process(KeywordEntry entry) throws Exception {
        Query query =
                new SimpleQuery().addCriteria(Criteria.where("id").is(entry.getKeyword().getId()));
        Optional<KeywordDocument> optionalDocument =
                solrOperations.queryForObject(
                        SolrCollection.keyword.name(), query, KeywordDocument.class);
        if (optionalDocument.isPresent()) {
            KeywordDocument document = optionalDocument.get();

            byte[] keywordObj = document.getKeywordObj().array();
            KeywordEntry statisticsEntry =
                    keywordObjectMapper.readValue(keywordObj, KeywordEntryImpl.class);
            entry =
                    KeywordEntryBuilder.from(entry)
                            .statistics(statisticsEntry.getStatistics())
                            .build();
        }
        return createKeywordDocument(entry);
    }

    private KeywordDocument createKeywordDocument(KeywordEntry keywordEntry) {
        // content is keyword id + keyword name + definition + synonyms
        List<String> content = new ArrayList<>();
        content.add(keywordEntry.getKeyword().getName());
        content.add(keywordEntry.getKeyword().getId());
        content.add(keywordEntry.getDefinition());
        if (Utils.notNullNotEmpty(keywordEntry.getSynonyms())) {
            content.addAll(keywordEntry.getSynonyms());
        }

        // create Keyword document
        KeywordDocument.KeywordDocumentBuilder builder = KeywordDocument.builder();
        builder.id(keywordEntry.getKeyword().getId());
        builder.name(keywordEntry.getKeyword().getName());
        builder.content(content);

        if (!keywordEntry.getParents().isEmpty()) {
            List<String> parents =
                    keywordEntry.getParents().stream()
                            .flatMap(this::getParentStream)
                            .collect(Collectors.toList());
            builder.parent(parents); // only one level

            List<String> ancestors =
                    keywordEntry.getParents().stream()
                            .flatMap(this::getAncestorsStrem)
                            .collect(Collectors.toList());
            builder.ancestor(ancestors); // recursively navigate over parents...
        }

        byte[] keywordByte = getKeywordObjectBinary(keywordEntry);
        builder.keywordObj(ByteBuffer.wrap(keywordByte));

        return builder.build();
    }

    private Stream<String> getParentStream(KeywordEntry kwEntry) {
        return Stream.of(kwEntry.getKeyword().getName(), kwEntry.getKeyword().getId());
    }

    private Stream<String> getAncestorsStrem(KeywordEntry kwEntry) {
        Stream<String> currentParent = getParentStream(kwEntry);
        Stream<String> parentsStream =
                kwEntry.getParents().stream().flatMap(this::getAncestorsStrem); // recursive call
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
