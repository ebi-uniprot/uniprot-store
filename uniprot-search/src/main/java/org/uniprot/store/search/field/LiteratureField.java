package org.uniprot.store.search.field;

import java.util.Arrays;

/** @author lgonzales */
public interface LiteratureField {

    enum ResultFields implements ReturnField {
        id("PubMed ID", "pubmedId", true),
        doi("Doi", "doiId"),
        title("Title", "title"),
        authoring_group("Authoring Group", "authoringGroup"),
        author("Authors", "authors"),
        author_and_group("Authors/Groups"),
        journal("Journal", "journal"),
        publication("Publication", "publicationDate"),
        reference("Reference"),
        lit_abstract("Abstract/Summary", "literatureAbstract"),
        statistics("Statistics", "statistics"),
        first_page("First page", "firstPage"),
        last_page("Last page", "lastPage"),
        volume("Volume", "volume"),
        completeAuthorList("Complete Author List", "completeAuthorList", true);

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
        literature_obj
    }
}
