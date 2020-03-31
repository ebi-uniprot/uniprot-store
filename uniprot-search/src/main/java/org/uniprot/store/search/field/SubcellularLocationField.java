package org.uniprot.store.search.field;

import java.util.Arrays;

/**
 * @author lgonzales
 * @since 2019-07-19
 */
public interface SubcellularLocationField {

    enum ResultFields implements ReturnField {
        name("Name", "name"),
        id("Subcellular location ID", "id", true),
        definition("Description", "definition"),
        category("Category", "category"),
        keyword("Keyword", "keyword"),
        synonyms("Synonyms", "synonyms"),
        content("Content", "content"),
        gene_ontologies("Gene Ontologies", "geneOntologies"),
        note("Note", "note"),
        references("References", "references"),
        links("Links", "links"),
        is_a("Is a", "isA"),
        part_of("Is part of", "partOf"),
        statistics("Statistics", "statistics");

        private String label;
        private String javaFieldName;
        private boolean isMandatoryJsonField;

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
        subcellularlocation_obj
    }
}
