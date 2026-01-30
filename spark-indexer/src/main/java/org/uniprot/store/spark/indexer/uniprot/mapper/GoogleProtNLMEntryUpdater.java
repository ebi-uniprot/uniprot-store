package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.uniprot.core.uniprotkb.comment.CommentType.SUBCELLULAR_LOCATION;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.api.java.function.Function;
import org.jetbrains.annotations.NotNull;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.uniprotkb.Keyword;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.comment.SubcellularLocation;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationComment;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationValue;
import org.uniprot.core.uniprotkb.comment.impl.SubcellularLocationBuilder;
import org.uniprot.core.uniprotkb.comment.impl.SubcellularLocationCommentBuilder;
import org.uniprot.core.uniprotkb.comment.impl.SubcellularLocationValueBuilder;
import org.uniprot.core.uniprotkb.impl.KeywordBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class GoogleProtNLMEntryUpdater
        implements Function<Tuple2<UniProtKBEntry, UniProtKBEntry>, UniProtKBEntry>, Serializable {

    private static final long serialVersionUID = -3375925835880954913L;
    private final Map<String, KeywordEntry> keywordAccEntryMap;
    private final Map<String, SubcellularLocationEntry> subcellNameEntryMap;

    public GoogleProtNLMEntryUpdater(
            Map<String, KeywordEntry> keywordAccEntryMap,
            Map<String, SubcellularLocationEntry> SubcellNameEntryMap) {
        this.keywordAccEntryMap = keywordAccEntryMap;
        this.subcellNameEntryMap = SubcellNameEntryMap;
    }

    @Override
    public UniProtKBEntry call(Tuple2<UniProtKBEntry, UniProtKBEntry> tuple) {
        UniProtKBEntry protNLMEntry = tuple._1();
        UniProtKBEntry uniProtEntry = tuple._2();
        // inject keyword category
        List<Keyword> keywords = populateKeywordCategory(protNLMEntry.getKeywords());
        List<Comment> comments = addSubcellularLocationId(protNLMEntry.getComments());
        return UniProtKBEntryBuilder.from(protNLMEntry)
                .uniProtId(uniProtEntry.getUniProtkbId())
                .keywordsSet(keywords)
                .commentsSet(comments)
                .build();
    }

    private List<Comment> addSubcellularLocationId(List<Comment> comments) {
        return comments.stream()
                .map(
                        comment ->
                                SUBCELLULAR_LOCATION.equals(comment.getCommentType())
                                        ? getSubcellCommentWithId(comment)
                                        : comment)
                .toList();
    }

    @NotNull
    private SubcellularLocationComment getSubcellCommentWithId(Comment comment) {
        SubcellularLocationComment subcellularLocationComment =
                (SubcellularLocationComment) comment;
        List<SubcellularLocation> subcellularLocationsWithId =
                subcellularLocationComment.getSubcellularLocations().stream()
                        .map(this::enrichWithSubcellId)
                        .toList();
        return SubcellularLocationCommentBuilder.from(subcellularLocationComment)
                .subcellularLocationsSet(subcellularLocationsWithId)
                .build();
    }

    private SubcellularLocation enrichWithSubcellId(SubcellularLocation subcellularLocation) {
        SubcellularLocationValue subcellLocationValue = subcellularLocation.getLocation();
        String subcellLocationName = subcellLocationValue.getValue();
        SubcellularLocationValue subcellLocationValueWithId =
                SubcellularLocationValueBuilder.from(subcellLocationValue)
                        .id(
                                Optional.ofNullable(subcellNameEntryMap.get(subcellLocationName))
                                        .map(SubcellularLocationEntry::getId)
                                        .orElseGet(
                                                () -> {
                                                    log.warn(
                                                            "Invalid subcellular location name %s"
                                                                    .formatted(
                                                                            subcellLocationName));
                                                    return null;
                                                }))
                        .build();
        return SubcellularLocationBuilder.from(subcellularLocation)
                .location(subcellLocationValueWithId)
                .build();
    }

    private List<Keyword> populateKeywordCategory(List<Keyword> keywords) {
        return keywords.stream().map(this::injectCategory).toList();
    }

    private Keyword injectCategory(Keyword keyword) {
        KeywordEntry actualKeyword = this.keywordAccEntryMap.get(keyword.getId());
        KeywordCategory keywordCategory = KeywordCategory.UNKNOWN;

        if (actualKeyword != null && actualKeyword.getCategory() != null) {
            keywordCategory = actualKeyword.getCategory();
        }

        return KeywordBuilder.from(keyword).category(keywordCategory).build();
    }
}
