package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.uniprotkb.Keyword;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.KeywordBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

public class GoogleProtNLMEntryUpdater
        implements Function<Tuple2<UniProtKBEntry, UniProtKBEntry>, UniProtKBEntry>, Serializable {

    private static final long serialVersionUID = -3375925835880954913L;
    private final Map<String, KeywordEntry> keywordAccEntryMap;

    public GoogleProtNLMEntryUpdater(Map<String, KeywordEntry> keywordAccEntryMap) {
        this.keywordAccEntryMap = keywordAccEntryMap;
    }

    @Override
    public UniProtKBEntry call(Tuple2<UniProtKBEntry, UniProtKBEntry> tuple) {
        UniProtKBEntry protNLMEntry = tuple._1();
        UniProtKBEntry uniProtEntry = tuple._2();
        // inject keyword category
        List<Keyword> keywords = populateKeywordCategory(protNLMEntry.getKeywords());
        return UniProtKBEntryBuilder.from(protNLMEntry)
                .uniProtId(uniProtEntry.getUniProtkbId())
                .keywordsSet(keywords)
                .build();
    }

    private List<Keyword> populateKeywordCategory(List<Keyword> keywords) {
        return keywords.stream().map(this::injectCateory).toList();
    }

    private Keyword injectCateory(Keyword keyword) {
        KeywordEntry actualKeyword = this.keywordAccEntryMap.get(keyword.getId());
        KeywordCategory keywordCategory = KeywordCategory.UNKNOWN;
        if (actualKeyword != null && actualKeyword.getCategory() != null) {
            keywordCategory = actualKeyword.getCategory();
        }
        return KeywordBuilder.from(keyword).category(keywordCategory).build();
    }
}
