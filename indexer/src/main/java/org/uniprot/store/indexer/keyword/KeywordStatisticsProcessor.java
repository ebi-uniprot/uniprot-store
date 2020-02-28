package org.uniprot.store.indexer.keyword;

import java.nio.ByteBuffer;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.Statistics;
import org.uniprot.core.builder.StatisticsBuilder;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.builder.KeywordEntryBuilder;
import org.uniprot.core.json.parser.keyword.KeywordJsonConfig;
import org.uniprot.store.search.document.keyword.KeywordDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
public class KeywordStatisticsProcessor
        implements ItemProcessor<KeywordStatisticsReader.KeywordCount, KeywordDocument> {

    private final ObjectMapper keywordObjectMapper;

    public KeywordStatisticsProcessor() {
        this.keywordObjectMapper = KeywordJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public KeywordDocument process(KeywordStatisticsReader.KeywordCount keywordCount)
            throws Exception {
        Statistics statistics =
                new StatisticsBuilder()
                        .reviewedProteinCount(keywordCount.getReviewedProteinCount())
                        .unreviewedProteinCount(keywordCount.getUnreviewedProteinCount())
                        .build();
        KeywordEntry keywordEntry = new KeywordEntryBuilder().statistics(statistics).build();

        KeywordDocument.KeywordDocumentBuilder builder = KeywordDocument.builder();
        builder.id(keywordCount.getKeywordId());

        byte[] keywordByte = getKeywordObjectBinary(keywordEntry);
        builder.keywordObj(ByteBuffer.wrap(keywordByte));

        return builder.build();
    }

    private byte[] getKeywordObjectBinary(KeywordEntry keyword) {
        try {
            return this.keywordObjectMapper.writeValueAsBytes(keyword);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse Keyword to binary json: ", e);
        }
    }
}
