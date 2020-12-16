package org.uniprot.store.indexer.publication.community;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.uniprot.core.json.parser.publication.CommunityMappedReferenceJsonConfig;
import org.uniprot.core.publication.CommunityAnnotation;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.impl.CommunityAnnotationBuilder;
import org.uniprot.core.publication.impl.CommunityMappedReferenceBuilder;
import org.uniprot.core.publication.impl.MappedSourceBuilder;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

class CommunityPublicationProcessorTest {
    private static final String ID_COMPONENT_SEPARATOR = "__";
    private static final String FUNCTION = "f";
    private static final String DISEASE = "d";
    private static final CommunityAnnotation ANNOTATION =
            new CommunityAnnotationBuilder().function(FUNCTION).disease(DISEASE).build();
    private static final String ACCESSION = "acc";
    private static final String PUBMED_ID = "12345";
    private static final String SOURCE_ID = "sourceId1234";
    private static final String SOURCE_CATEGORY = "cat";
    private static final CommunityMappedReference REFERENCE =
            new CommunityMappedReferenceBuilder()
                    .uniProtKBAccession(ACCESSION)
                    .pubMedId(PUBMED_ID)
                    .source(new MappedSourceBuilder().id(SOURCE_ID).name("sourceName").build())
                    .sourceCategoriesAdd(SOURCE_CATEGORY)
                    .communityAnnotation(ANNOTATION)
                    .build();

    @Test
    void validateCorrectId() {
        CommunityPublicationProcessor processor = new CommunityPublicationProcessor();

        String id = PublicationUtils.computeDocumentId(REFERENCE);

        assertThat(
                id,
                is(
                        ACCESSION
                                + ID_COMPONENT_SEPARATOR
                                + PUBMED_ID
                                + ID_COMPONENT_SEPARATOR
                                + SOURCE_ID));
    }

    @Test
    void validateSerialisedObject() throws IOException {
        CommunityPublicationProcessor processor = new CommunityPublicationProcessor();

        PublicationDocument document = processor.process(REFERENCE);

        ObjectMapper mapper =
                CommunityMappedReferenceJsonConfig.getInstance().getFullObjectMapper();
        CommunityMappedReference reference =
                mapper.readValue(
                        document.getPublicationMappedReference(), CommunityMappedReference.class);

        assertThat(reference.getUniProtKBAccession().getValue(), is(ACCESSION));
        assertThat(reference.getPubMedId(), is(PUBMED_ID));
        assertThat(reference.getSourceCategories(), contains(SOURCE_CATEGORY));
        assertThat(reference.getCommunityAnnotation().getFunction(), is(FUNCTION));
        assertThat(reference.getCommunityAnnotation().getDisease(), is(DISEASE));
        assertThat(reference.getCommunityAnnotation().getProteinOrGene(), is(nullValue()));
        assertThat(reference.getCommunityAnnotation().getComment(), is(nullValue()));
    }
}
