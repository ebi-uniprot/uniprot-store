package org.uniprot.store.search.field;

import java.util.Arrays;

public interface KeywordField {

    enum ResultFields implements ReturnField {
        id("Keyword ID"),
        keyword("Keyword", "keyword", true),
        name("Name"),
        description("Description", "definition"),
        category("Category", "category"),
        synonym("Synonyms", "synonyms"),
        gene_ontology("Gene Ontology", "geneOntologies"),
        sites("Sites", "sites"),
        children("Children", "children"),
        parent("Parents", "parents"),
        statistics("Statistics", "statistics");

        private String label;
        private String javaFieldName;
        private boolean
                isMandatoryJsonField; // flag to indicate field which will always be present in
        // Json response no matter what 'fields' are pass in request

        ResultFields(String label) {
            this(label, null);
        }

        ResultFields(String label, String javaFieldName) {
            this(label, javaFieldName, false);
        }

        ResultFields(String label, String javaFieldName, boolean isMandatoryJsonField) {
            this.label = label;
            this.javaFieldName = javaFieldName;
            this.isMandatoryJsonField = isMandatoryJsonField;
        }

        public String getLabel() {
            return this.label;
        }

        @Override
        public boolean hasReturnField(String fieldName) {
            return Arrays.stream(ResultFields.values())
                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
        }

        @Override
        public String getJavaFieldName() {
            return this.javaFieldName;
        }

        @Override
        public boolean isMandatoryJsonField() {
            return this.isMandatoryJsonField;
        }
    }

    enum Return {
        id,
        taxonomy_obj
    }
}
