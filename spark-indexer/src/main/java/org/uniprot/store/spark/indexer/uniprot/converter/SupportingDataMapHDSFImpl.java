package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.readLines;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.util.Pair;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.disease.DiseaseFileReader;
import org.uniprot.cv.keyword.KeywordFileReader;
import org.uniprot.cv.subcell.SubcellularLocationFileReader;

/**
 * Class responsible to load SupportingData from Hadoop FileSystem.
 *
 * @author lgonzales
 * @since 2019-11-01
 */
@Slf4j
public class SupportingDataMapHDSFImpl implements SupportingDataMap {

    private static final long serialVersionUID = -1947869915492700469L;
    private static final String LOADED = "Loaded ";
    private final Map<String, Pair<String, KeywordCategory>> keywordMap = new HashMap<>();
    private final Map<String, String> diseaseMap = new HashMap<>();
    private final Map<String, String> subcellularLocationMap = new HashMap<>();

    public SupportingDataMapHDSFImpl(
            String keywordFile,
            String diseaseFile,
            String subcellularLocationFile,
            Configuration hadoopConfig) {
        loadKeywordMap(keywordFile, hadoopConfig);
        loadDiseaseMap(diseaseFile, hadoopConfig);
        loadSubcellularLocationMap(subcellularLocationFile, hadoopConfig);
    }

    private void loadSubcellularLocationMap(
            String subcellularLocationFile, Configuration hadoopConfig) {
        if (Utils.notNullNotEmpty(subcellularLocationFile)) {
            List<String> lines = readLines(subcellularLocationFile, hadoopConfig);
            List<SubcellularLocationEntry> entries =
                    new SubcellularLocationFileReader().parseLines(lines);
            subcellularLocationMap.putAll(
                    entries.stream()
                            .collect(
                                    Collectors.toMap(
                                            SubcellularLocationEntry::getContent,
                                            SubcellularLocationEntry::getId)));
            log.info(LOADED + subcellularLocationMap.size() + " Subcellular Location Map");
        } else {
            log.warn("Subcellular Location File was not loaded");
        }
    }

    private void loadKeywordMap(String keywordFile, Configuration hadoopConfig) {
        if (Utils.notNullNotEmpty(keywordFile)) {
            List<String> lines = readLines(keywordFile, hadoopConfig);
            List<KeywordEntry> entries = new KeywordFileReader().parseLines(lines);
            keywordMap.putAll(
                    entries.stream()
                            .collect(
                                    Collectors.toMap(
                                            KeywordFileReader::getId,
                                            KeywordFileReader::getAccessionCategoryPair)));
            log.info(LOADED + keywordMap.size() + " keyword Map");
        } else {
            log.warn("Keyword File was not loaded");
        }
    }

    private void loadDiseaseMap(String diseaseFile, Configuration hadoopConfig) {
        if (Utils.notNullNotEmpty(diseaseFile)) {
            List<String> lines = readLines(diseaseFile, hadoopConfig);
            List<DiseaseEntry> entries = new DiseaseFileReader().parseLines(lines);
            diseaseMap.putAll(
                    entries.stream()
                            .collect(Collectors.toMap(DiseaseEntry::getName, DiseaseEntry::getId)));
            log.info(LOADED + diseaseMap.size() + " disease Map");
        } else {
            log.warn("diseaseFile path must not be null or empty");
        }
    }

    @Override
    public Map<String, Pair<String, KeywordCategory>> getKeywordMap() {
        return keywordMap;
    }

    @Override
    public Map<String, String> getDiseaseMap() {
        return diseaseMap;
    }

    @Override
    public Map<String, Map<String, List<Evidence>>> getGoEvidencesMap() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getSubcellularLocationMap() {
        return subcellularLocationMap;
    }
}
