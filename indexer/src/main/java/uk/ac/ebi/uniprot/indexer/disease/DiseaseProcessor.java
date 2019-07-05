package uk.ac.ebi.uniprot.indexer.disease;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.cv.disease.Disease;
import uk.ac.ebi.uniprot.domain.builder.DiseaseBuilder;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.json.parser.disease.DiseaseJsonConfig;
import uk.ac.ebi.uniprot.search.document.disease.DiseaseDocument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DiseaseProcessor implements ItemProcessor<Disease, DiseaseDocument> {
    private ObjectMapper diseaseObjectMapper;
    // cache from DiseaseProteinCountWriter Step
    private Map<String, DiseaseProteinCountReader.DiseaseProteinCount> diseaseIdProteinCountMap;

    public DiseaseProcessor() {
        this.diseaseObjectMapper = DiseaseJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public DiseaseDocument process(Disease disease) {
        List<String> kwIds = new ArrayList<>();
        if (disease.getKeywords() != null) {
            kwIds = disease.getKeywords().stream().map(kw -> kw.getId()).collect(Collectors.toList());
        }
        // name is a combination of id, acronym, definition, synonyms, keywords
        List<String> name = new ArrayList<>();
        name.add(disease.getId());
        name.add(disease.getAcronym());
        name.add(disease.getDefinition());
        name.addAll(kwIds);
        name.addAll(disease.getAlternativeNames());

        // content is name + accession
        List<String> content = new ArrayList<>();
        content.addAll(name);
        content.add(disease.getAccession());

        // create disease document
        DiseaseDocument.DiseaseDocumentBuilder builder = DiseaseDocument.builder();
        builder.accession(disease.getAccession());
        builder.name(name).content(content);
        byte[] diseaseByte = getDiseaseObjectBinary(disease);
        builder.diseaseObj(ByteBuffer.wrap(diseaseByte));

        return builder.build();
    }

    private byte[] getDiseaseObjectBinary(Disease disease) {
        try {
            DiseaseBuilder diseaseBuilder = DiseaseBuilder.newInstance().from(disease);

            // get the protein count, reviewed and unreviewed
            DiseaseProteinCountReader.DiseaseProteinCount diseaseProteinCount = this.diseaseIdProteinCountMap.get(disease.getId());

            if(diseaseProteinCount != null) {
                diseaseBuilder.reviewedProteinCount(diseaseProteinCount.getReviewedProteinCount());
                diseaseBuilder.unreviewedProteinCount(diseaseProteinCount.getUnreviewedProteinCount());
            } 

            return this.diseaseObjectMapper.writeValueAsBytes(diseaseBuilder.build());

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse disease to binary json: ", e);
        }
    }

    @BeforeStep
    public void getStepExecution(final StepExecution stepExecution) {// get the cached data from previous step

        this.diseaseIdProteinCountMap = (Map<String, DiseaseProteinCountReader.DiseaseProteinCount>) stepExecution.getJobExecution()
                .getExecutionContext().get(Constants.DISEASE_PROTEIN_COUNT_KEY);
    }

}
