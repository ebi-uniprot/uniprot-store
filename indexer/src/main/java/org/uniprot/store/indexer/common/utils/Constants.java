package org.uniprot.store.indexer.common.utils;

public class Constants {

    private Constants() {}

    public static final String SUPPORTING_DATA_INDEX_JOB = "Supporting data indexing job";

    public static final String UNIPROTKB_INDEX_JOB = "UniProtKB_Indexing_Job";
    public static final String UNIPROTKB_INDEX_STEP = "UniProtKB_Indexing_Step";

    public static final String INACTIVEENTRY_INDEX_JOB = "InactiveEntry_Indexing_Job";
    public static final String INACTIVEENTRY_INDEX_STEP = "InactiveEntry_Indexing_Step";

    public static final String CROSS_REF_LOAD_JOB = "Cross ref load job";
    public static final String CROSS_REF_INDEX_STEP = "Cross ref indexing step";
    public static final String CROSS_REF_UNIPROT_COUNT_STEP_NAME =
            "CROSS_REF_UNIPROT_COUNT_STEP_NAME";
    public static final String CROSS_REF_PROTEIN_COUNT_KEY = "Cross Ref Protein Context Key";

    public static final String TAXONOMY_LOAD_JOB_NAME = "TAXONOMY_LOAD_JOB_NAME";
    public static final String TAXONOMY_LOAD_NODE_STEP_NAME = "TAXONOMY_LOAD_NODE";
    public static final String TAXONOMY_LOAD_STATISTICS_STEP_NAME = "TAXONOMY_LOAD_STATISTICS";
    public static final String TAXONOMY_LOAD_MERGED_STEP_NAME = "TAXONOMY_LOAD_MERGED";
    public static final String TAXONOMY_LOAD_DELETED_STEP_NAME = "TAXONOMY_LOAD_DELETED";

    public static final String GENE_CENTRIC_INDEX_JOB = "Gene_Centric_Indexing_Job";
    public static final String PROTEOME_INDEX_JOB = "Proteome_Indexing_Job";
    public static final String DISEASE_LOAD_JOB_NAME = "DiseaseEntry indexing job";
    public static final String DISEASE_PROTEIN_COUNT_STEP = "DiseaseEntry protein count step";
    public static final String DISEASE_INDEX_STEP = "DiseaseEntry indexing step";
    public static final String DISEASE_PROTEIN_COUNT_KEY = "DiseaseEntry Protein Context Key";
    public static final String UNIPARC_INDEX_JOB = "UniParc_Indexing_Job";

    public static final String KEYWORD_LOAD_JOB_NAME = "KEYWORD_LOAD_JOB_NAME";
    public static final String KEYWORD_INDEX_STEP = "KEYWORD_INDEX_STEP";
    public static final String KEYWORD_LOAD_STATISTICS_STEP_NAME = "KEYWORD_LOAD_STATISTICS";

    public static final String LITERATURE_LOAD_JOB_NAME = "LITERATURE_LOAD_JOB_NAME";
    public static final String LITERATURE_INDEX_STEP = "LITERATURE_INDEX_STEP";
    public static final String LITERATURE_MAPPING_INDEX_STEP = "LITERATURE_MAPPING_INDEX_STEP";
    public static final String LITERATURE_LOAD_STATISTICS_STEP_NAME = "LITERATURE_LOAD_STATISTICS";

    public static final String PUBLICATION_LARGE_SCALE_KEY = "largeScalePubmedIds";
    public static final String COMMUNITY_PUBLICATION_JOB_NAME = "COMMUNITY_PUBLICATION_JOB_NAME";
    public static final String COMMUNITY_PUBLICATION_INDEX_STEP =
            "COMMUNITY_PUBLICATION_INDEX_STEP";

    public static final String UNIPROTKB_PUBLICATION_JOB_NAME = "UNIPROTKB_PUBLICATION_JOB_NAME";
    public static final String UNIPROTKB_PUBLICATION_INDEX_STEP =
            "UNIPROTKB_PUBLICATION_INDEX_STEP";

    public static final String PUBLICATION_LARGE_SCALE_STEP = "PUBLICATION_LARGE_SCALE_STEP";
    public static final String COMPUTATIONAL_PUBLICATION_JOB_NAME =
            "COMPUTATIONAL_PUBLICATION_JOB_NAME";
    public static final String COMPUTATIONAL_PUBLICATION_INDEX_STEP =
            "COMPUTATIONAL_PUBLICATION_INDEX_STEP";

    public static final String SUBCELLULAR_LOCATION_LOAD_JOB_NAME =
            "SUBCELLULAR_LOCATION_LOAD_JOB_NAME";
    public static final String SUBCELLULAR_LOCATION_INDEX_STEP = "SUBCELLULAR_LOCATION_INDEX_STEP";
    public static final String SUBCELLULAR_LOCATION_LOAD_STATISTICS_STEP_NAME =
            "SUBCELLULAR_LOCATION_LOAD_STATISTICS";
    public static final String SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY =
            "SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY";

    public static final String SUGGESTIONS_INDEX_STEP = "Suggestion_Indexing_Step";
    public static final String SUGGESTIONS_MAP = "Suggestions_Set";

    public static final String UNIREF_INDEX_JOB = "UniRef_Indexing_Job";
    public static final String UNIREF_INDEX_STEP = "UniRef_Index_Step";

    public static final String UNIRULE_INDEX_JOB = "UniRule Indexing Job";
    public static final String UNIRULE_INDEX_STEP = "UniRule Indexing Step";
    public static final String UNIRULE_PROTEIN_COUNT_STEP = "UniRule protein count step";
    public static final String UNIRULE_PROTEIN_COUNT_CACHE_KEY = "UniRule Protein Count Cache Key";

    public static final String ARBA_INDEX_JOB = "ARBA Indexing Job";
    public static final String ARBA_INDEX_STEP = "ARBA Indexing Step";
    public static final String ARBA_PROTEIN_COUNT_STEP = "ARBA protein count step";
    public static final String ARBA_PROTEIN_COUNT_CACHE_KEY = "ARBA Protein Count Cache Key";

    public static final String HELP_PAGE_INDEX_JOB_NAME = "Help page index job";
    public static final String HELP_PAGE_INDEX_STEP = "Help page index step";

    public static final String CHEBI_RDF_NODE_ID_ATTRIBBUTE = "_rdf:nodeID";
    public static final String CHEBI_RDF_ABOUT_ATTRIBUTE = "_rdf:about";
    public static final String CHEBI_RDF_TYPE_ATTRIBUTE = "rdf:type";
    public static final String CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE = "chebiStructuredName";
    public static final String CHEBI_RDFS_SUBCLASS_ATTRIBUTE = "rdfs:subClassOf";
    public static final String CHEBI_RDFS_LABEL_ATTRIBUTE = "rdfs:label";
    public static final String CHEBI_OWL_PROPERTY_ATTRIBUTE = "owl:onProperty";
    public static final String CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE = "owl:someValuesFrom";
    public static final String CHEBI_RDF_RESOURCE_ATTRIBUTE = "_rdf:resource";
}
