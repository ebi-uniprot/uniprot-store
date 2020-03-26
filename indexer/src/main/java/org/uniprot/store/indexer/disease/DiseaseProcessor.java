package org.uniprot.store.indexer.disease;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.core.cv.disease.impl.DiseaseEntryBuilder;
import org.uniprot.core.json.parser.disease.DiseaseJsonConfig;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.disease.DiseaseDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DiseaseProcessor implements ItemProcessor<DiseaseEntry, DiseaseDocument> {
    private ObjectMapper diseaseObjectMapper;
    // cache from DiseaseProteinCountWriter Step
    private Map<String, DiseaseProteinCountReader.DiseaseProteinCount> diseaseIdProteinCountMap;

    public DiseaseProcessor() {
        this.diseaseObjectMapper = DiseaseJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public DiseaseDocument process(DiseaseEntry disease) {
        List<String> kwIds = new ArrayList<>();
        if (disease.getKeywords() != null) {
            kwIds =
                    disease.getKeywords().stream()
                            .map(kw -> kw.getName())
                            .collect(Collectors.toList());
        }
        // name is a combination of id, acronym, definition, synonyms, keywords
        List<String> name = new ArrayList<>();
        name.add(disease.getName());
        name.add(disease.getAcronym());
        name.add(disease.getDefinition());
        name.addAll(kwIds);
        name.addAll(disease.getAlternativeNames());

        // content is name + accession
        List<String> content = new ArrayList<>();
        content.addAll(name);
        content.add(disease.getId());

        // create disease document
        DiseaseDocument.DiseaseDocumentBuilder builder = DiseaseDocument.builder();
        builder.id(disease.getId());
        builder.name(name).content(content);
        byte[] diseaseByte = getDiseaseObjectBinary(disease);
        builder.diseaseObj(ByteBuffer.wrap(diseaseByte));

        return builder.build();
    }

    private byte[] getDiseaseObjectBinary(DiseaseEntry disease) {
        try {
            DiseaseEntryBuilder diseaseBuilder = DiseaseEntryBuilder.from(disease);

            // get the protein count, reviewed and unreviewed
            DiseaseProteinCountReader.DiseaseProteinCount diseaseProteinCount =
                    this.diseaseIdProteinCountMap.get(disease.getName());

            if (diseaseProteinCount != null) {
                diseaseBuilder.reviewedProteinCount(diseaseProteinCount.getReviewedProteinCount());
            }

            return this.diseaseObjectMapper.writeValueAsBytes(diseaseBuilder.build());

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse disease to binary json: ", e);
        }
    }

    @BeforeStep
    public void getStepExecution(
            final StepExecution stepExecution) { // get the cached data from previous step

        this.diseaseIdProteinCountMap =
                (Map<String, DiseaseProteinCountReader.DiseaseProteinCount>)
                        stepExecution
                                .getJobExecution()
                                .getExecutionContext()
                                .get(Constants.DISEASE_PROTEIN_COUNT_KEY);
    }
}
