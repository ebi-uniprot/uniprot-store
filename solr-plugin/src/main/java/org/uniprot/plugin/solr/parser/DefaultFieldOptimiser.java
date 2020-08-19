package org.uniprot.plugin.solr.parser;

import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Consumer;

import static org.uniprot.core.util.Utils.notNullNotEmpty;

/**
 * Created 18/08/2020
 *
 * @author Edd
 */
@Builder
@Slf4j
public class DefaultFieldOptimiser implements Consumer<Query> {
    @Singular
    private final List<SearchFieldItem> optimisedFields;
    private final Field reflectedTermText;
    private final Field reflectedTermField;
    private final boolean optimisePossible;

    public static DefaultFieldOptimiserBuilder builder() {
        return new DefaultFieldOptimiserBuilder() {
            @Override
            public DefaultFieldOptimiser build() {
                init();
                return super.build();
            }
        };
    }

    @Override
    public void accept(Query query) {
        if (query instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) query;
            optimiseIfNecessary(termQuery.getTerm());
        }
    }

    private void optimiseIfNecessary(Term term) {
        if (Utils.nullOrEmpty(term.field())) {
            for (SearchFieldItem field : optimisedFields) {
                if (notNullNotEmpty(field.getValidRegex())
                        && term.text().matches(field.getValidRegex())) {
                    try {
                        reflectedTermField.set(term, field.getFieldName());
                        reflectedTermText.set(term, new BytesRef(term.text()));
                    } catch (IllegalAccessException e) {
                        log.error("Could not access field", e);
                    }
                    break;
                }
            }
        }
    }

    public static class DefaultFieldOptimiserBuilder {
        private Field reflectedTermText;
        private Field reflectedTermField;
        private boolean optimisePossible;

        void init() {
            reflectedTermField = null;
            try {
                reflectedTermField = Term.class.getDeclaredField("field");
                reflectedTermField.setAccessible(true);
                reflectedTermText = Term.class.getDeclaredField("bytes");
                reflectedTermText.setAccessible(true);
                optimisePossible = true;
            } catch (NoSuchFieldException e) {
                log.error(
                        "Could not get Term.field for use when adding concrete fields to default fields, e.g., P12345 -> accession_id:P12345",
                        e);
                optimisePossible = false;
            }
        }
    }
}
