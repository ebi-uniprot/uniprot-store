package org.uniprot.store.indexer.publication.community;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.CommunityAnnotation;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.impl.CommunityAnnotationBuilder;
import org.uniprot.core.publication.impl.CommunityMappedReferenceBuilder;
import org.uniprot.core.publication.impl.MappedSourceBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.publication.common.PublicationUtils;
import org.uniprot.store.search.document.publication.PublicationDocument;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

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
    private static final UniProtSolrClient SOLR_CLIENT = mock(UniProtSolrClient.class);

    @Test
    void validateCorrectId() {
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
        CommunityPublicationProcessor processor = new CommunityPublicationProcessor(SOLR_CLIENT);

        List<PublicationDocument> documents = processor.process(REFERENCE);

        assertThat(documents, hasSize(1));

        PublicationDocument document = documents.get(0);

        ObjectMapper mapper = MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();
        MappedPublications mappedPublications =
                mapper.readValue(
                        document.getPublicationMappedReferences(), MappedPublications.class);

        List<CommunityMappedReference> references =
                mappedPublications.getCommunityMappedReferences();
        assertThat(references, hasSize(1));

        CommunityMappedReference reference = references.get(0);

        assertThat(reference.getUniProtKBAccession().getValue(), is(ACCESSION));
        assertThat(reference.getPubMedId(), is(PUBMED_ID));
        assertThat(reference.getSourceCategories(), contains(SOURCE_CATEGORY));
        assertThat(reference.getCommunityAnnotation().getFunction(), is(FUNCTION));
        assertThat(reference.getCommunityAnnotation().getDisease(), is(DISEASE));
        assertThat(reference.getCommunityAnnotation().getProteinOrGene(), is(nullValue()));
        assertThat(reference.getCommunityAnnotation().getComment(), is(nullValue()));
    }
}
