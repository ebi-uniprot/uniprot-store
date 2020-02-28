package org.uniprot.store.search.domain2.impl;

import java.util.Objects;
import java.util.Optional;

import lombok.Builder;
import lombok.Data;

import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.store.config.model.FieldItem;
import org.uniprot.store.config.model.FieldType;
import org.uniprot.store.search.domain2.SearchField;
import org.uniprot.store.search.domain2.SearchFieldType;

/**
 * Created 14/11/19
 *
 * @author Edd
 */
@Builder
@Data
public class SearchFieldImpl implements SearchField {
    public static final String XREF_COUNT_PREFIX = "xref_count_";
    private String name;
    private SearchFieldType type;
    private SearchField sortField;
    private String validRegex;

    @Override
    public Optional<SearchField> getSortField() {
        return Optional.ofNullable(sortField);
    }

    @Override
    public Optional<String> getValidRegex() {
        return Optional.ofNullable(validRegex);
    }

    public static SearchField from(FieldItem fieldItem) {
        SearchFieldImplBuilder builder = SearchFieldImpl.builder();
        builder.name(fieldItem.getFieldName()).validRegex(fieldItem.getValidRegex());
        if (isGeneralFieldItem(fieldItem)) {
            builder.type(SearchFieldType.GENERAL);
        } else if (isRangeFieldItem(fieldItem)) {
            builder.type(SearchFieldType.RANGE);
        }
        if (Objects.nonNull(fieldItem.getSortFieldId())) { // set the sortfield id
            builder.sortField(SearchFieldImpl.builder().name(fieldItem.getSortFieldId()).build());
        }
        return builder.build();
    }

    public static SearchField from(UniProtDatabaseDetail db) {
        SearchFieldImplBuilder builder = SearchFieldImpl.builder();
        builder.name(XREF_COUNT_PREFIX + db.getName().toLowerCase()).type(SearchFieldType.RANGE);
        return builder.build();
    }

    private static boolean isGeneralFieldItem(FieldItem fieldItem) {
        return Objects.nonNull(fieldItem.getFieldType())
                && (FieldType.evidence == fieldItem.getFieldType()
                        || FieldType.field == fieldItem.getFieldType());
    }

    private static boolean isRangeFieldItem(FieldItem fieldItem) {
        return Objects.nonNull(fieldItem.getFieldType())
                && FieldType.range == fieldItem.getFieldType();
    }
}
