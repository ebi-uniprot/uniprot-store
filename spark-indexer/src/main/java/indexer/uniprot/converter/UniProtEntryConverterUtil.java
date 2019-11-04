package indexer.uniprot.converter;

import org.uniprot.core.Value;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.comment.AlternativeProductsComment;
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.core.uniprot.comment.IsoformSequenceStatus;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.store.search.document.suggest.SuggestDictionary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
class UniProtEntryConverterUtil {

    private static final int SORT_FIELD_MAX_LENGTH = 30;

    static Set<String> extractEvidence(List<Evidence> evidences) {
        Set<String> extractedEvidence = evidences.stream()
                .flatMap(UniProtEntryConverterUtil::addExtractedEvidenceItem)
                .collect(Collectors.toSet());
        return extractedEvidence;
    }

    private static Stream<String> addExtractedEvidenceItem(Evidence evidence) {
        List<String> result = new ArrayList<>();
        result.add(evidence.getEvidenceCode().name());

        result.addAll(evidence.getEvidenceCode().getCategories().stream()
                .map(category -> category.name().toLowerCase())
                .collect(Collectors.toList()));

        return result.stream();
    }

    static String createSuggestionMapKey(SuggestDictionary dict, String id) {
        return dict.name() + ":" + id;
    }

    static List<String> getXrefId(String id, String dbname) {
        List<String> values = new ArrayList<>();
        values.add(id);
        values.add(dbname + "-" + id);
        if (id.indexOf(".") > 0) {
            String idMain = id.substring(0, id.indexOf("."));
            values.add(idMain);
            values.add(dbname + "-" + idMain);
        }
        return values;
    }

    static String truncatedSortValue(String value) {
        if (value != null && value.length() > SORT_FIELD_MAX_LENGTH) {
            return value.substring(0, SORT_FIELD_MAX_LENGTH);
        } else {
            return value;
        }
    }

    static void addValueListToStringList(Collection<String> list, List<? extends Value> values) {
        if (values != null) {
            for (Value v : values) {
                addValueToStringList(list, v);
            }
        }
    }

    static void addValueToStringList(Collection<String> list, Value value) {
        if ((value != null) && (!value.getValue().isEmpty())) {
            list.add(value.getValue());
        }
    }

    static boolean isCanonicalIsoform(UniProtEntry uniProtEntry) {
        return uniProtEntry.getCommentByType(CommentType.ALTERNATIVE_PRODUCTS)
                .stream()
                .map(comment -> (AlternativeProductsComment) comment)
                .flatMap(comment -> comment.getIsoforms().stream())
                .filter(isoform -> isoform.getIsoformSequenceStatus() == IsoformSequenceStatus.DISPLAYED)
                .flatMap(isoform -> isoform.getIsoformIds().stream())
                .filter(isoformId -> isoformId.getValue().equals(uniProtEntry.getPrimaryAccession().getValue()))
                .count() == 1L;
    }

}
