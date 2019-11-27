package org.uniprot.store.indexer.search;

import java.lang.reflect.Field;
import java.util.function.Consumer;

import org.uniprot.store.search.document.Document;

/**
 * Required to capture and modify the {@code value} of Solr {@code @Field("value")} annotations.
 * This is necessary for testing purposes. See {@link FullCIAnalysisSearchIT}.
 *
 * <p>Created 02/07/18
 *
 * @author Edd
 */
public class DocFieldTransformer implements Consumer<Document> {
    private final String field;
    private final Object value;

    private DocFieldTransformer(String field, Object value) {
        this.field = field;
        this.value = value;
    }

    public static DocFieldTransformer fieldTransformer(String field, Object value) {
        return new DocFieldTransformer(field, value);
    }

    @Override
    public void accept(Document doc) {
        Class<? extends Document> c = doc.getClass();
        try {
            boolean updated = false;
            for (Field declaredField : c.getDeclaredFields()) {
                if (declaredField.isAnnotationPresent(
                        org.apache.solr.client.solrj.beans.Field.class)) {
                    org.apache.solr.client.solrj.beans.Field declaredAnnotation =
                            declaredField.getDeclaredAnnotation(
                                    org.apache.solr.client.solrj.beans.Field.class);
                    if (declaredAnnotation.value().equals(this.field)) {
                        declaredField.set(doc, value);
                        updated = true;
                    }
                }
            }
            if (!updated) {
                throw new IllegalStateException(
                        "Cannot transform document, field does not exist: " + this.field);
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot transform document", e);
        }
    }
}
