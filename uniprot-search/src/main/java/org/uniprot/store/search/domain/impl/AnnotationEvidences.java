package org.uniprot.store.search.domain.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.uniprot.store.search.domain.EvidenceGroup;
import org.uniprot.store.search.domain.SearchEvidences;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public enum AnnotationEvidences implements SearchEvidences {
    INSTANCE;
    private static final String FILENAME = "uniprot/annotation_evidence.json";
    private List<EvidenceGroup> evidences = new ArrayList<>();

    AnnotationEvidences() {
        init();
    }

    void init() {
        ObjectMapper objectMapper = JsonConfig.getJsonMapper();
        try (InputStream is =
                AnnotationEvidences.class.getClassLoader().getResourceAsStream(FILENAME)) {
            List<EvidenceGroupImpl> evidences =
                    objectMapper.readValue(is, new TypeReference<List<EvidenceGroupImpl>>() {});
            this.evidences.addAll(evidences);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<EvidenceGroup> getEvidences() {
        return evidences;
    }
}
