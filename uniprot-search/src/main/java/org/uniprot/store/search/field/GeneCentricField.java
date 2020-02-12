package org.uniprot.store.search.field;

import java.util.Arrays;

/**
 * @author jluo
 * @date: 17 May 2019
 */
public interface GeneCentricField {
    enum Return {
        accession_id,
        genecentric_stored;
    }

    enum ResultFields implements ReturnField {
        accession_id("canonical protein", "canonicalProtein"),
        gene("canonical gene"),
        entry_type("entry type"),
        related_accession("related proteins", "relatedProteins");

        private String label;
        private String javaFieldName;

        ResultFields(String label) {
            this(label, null);
        }

        ResultFields(String label, String javaFieldName) {
            this.label = label;
            this.javaFieldName = javaFieldName;
        }

        public String getLabel() {
            return this.label;
        }

        @Override
        public String getJavaFieldName() {
            return this.javaFieldName;
        }

        @Override
        public boolean hasReturnField(String fieldName) {
            return Arrays.stream(ResultFields.values())
                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
        }
    }
}
