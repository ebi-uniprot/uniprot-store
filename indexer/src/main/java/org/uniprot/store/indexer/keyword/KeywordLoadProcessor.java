package org.uniprot.store.indexer.keyword;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryBuilder;
import org.uniprot.core.cv.keyword.impl.KeywordEntryImpl;
import org.uniprot.core.json.parser.keyword.KeywordJsonConfig;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.keyword.KeywordDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
public class KeywordLoadProcessor implements ItemProcessor<KeywordEntry, KeywordDocument> {

    private final ObjectMapper keywordObjectMapper;
    private final UniProtSolrClient uniProtSolrClient;

    public KeywordLoadProcessor(UniProtSolrClient uniProtSolrClient) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.keywordObjectMapper = KeywordJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public KeywordDocument process(KeywordEntry entry) throws Exception {
        SolrQuery query = new SolrQuery("id:" + entry.getKeyword().getId());
        Optional<KeywordDocument> optionalDocument =
                uniProtSolrClient.queryForObject(
                        SolrCollection.keyword, query, KeywordDocument.class);
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
        // create Keyword document
        KeywordDocument.KeywordDocumentBuilder builder = KeywordDocument.builder();
        builder.id(keywordEntry.getKeyword().getId());
        builder.name(keywordEntry.getKeyword().getName());
        builder.definition(keywordEntry.getDefinition());
        if (Utils.notNullNotEmpty(keywordEntry.getSynonyms())) {
            builder.synonyms(keywordEntry.getSynonyms());
        }
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
        if (Utils.notNull(keywordEntry.getCategory())) {
            String category = keywordEntry.getCategory().name().toLowerCase();
            builder.category(category);
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
            throw new DocumentConversionException("Unable to parse Keyword to binary json: ", e);
        }
    }
}
