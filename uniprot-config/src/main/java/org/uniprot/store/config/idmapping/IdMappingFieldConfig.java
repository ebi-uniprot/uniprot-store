package org.uniprot.store.config.idmapping;

import org.uniprot.core.cv.xdb.UniProtDatabaseCategory;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author sahmad
 * @created 26/02/2021
 */
public class IdMappingFieldConfig {
    public static final String ACC_STR = convertDisplayNameToName("UniProtKB");
    public static final String ACC_ID_STR = convertDisplayNameToName("UniProtKB AC/ID");
    public static final String SWISSPROT_STR = convertDisplayNameToName("UniProtKB/Swiss-Prot");
    public static final String UPARC_STR = convertDisplayNameToName("UniParc");
    public static final String UNIREF50_STR = convertDisplayNameToName("UniRef50");
    public static final String UNIREF90_STR = convertDisplayNameToName("UniRef90");
    public static final String UNIREF100_STR = convertDisplayNameToName("UniRef100");
    public static final String GENENAME_STR = convertDisplayNameToName("Gene Name");

    private static final String PIR_CRC64 = "CRC64";
    public static final String PIR_ACC_STR = "ACC";
    public static final String PIR_ACC_ID_STR = "ACC,ID";
    public static final String PIR_SWISSPROT_STR = "SWISSPROT";
    public static final String PIR_UPARC_STR = "UPARC";
    public static final String PIR_UNIREF50_STR = "NF50";
    public static final String PIR_UNIREF90_STR = "NF90";
    public static final String PIR_UNIREF100_STR = "NF100";
    public static final String PIR_GENENAME_STR = "GENENAME";

    private static final UniProtDatabaseTypes ALL_DB_TYPES = UniProtDatabaseTypes.INSTANCE;
    private static final String CRC64 = "CRC64";
    private static List<UniProtDatabaseDetail> idMappingTypes = new ArrayList<>();
    private static Map<String, String> uniProtToPIRDbNameMap = new HashMap<>();

    public static List<UniProtDatabaseDetail> getAllIdMappingTypes() {
        if (Utils.nullOrEmpty(idMappingTypes)) {
            // get all the fields which has idmappingname set
            idMappingTypes =
                    ALL_DB_TYPES.getAllDbTypes().stream()
                            .filter(IdMappingFieldConfig::hasIdMappingName)
                            .collect(Collectors.toList());
            // add db names for UniProt category
            idMappingTypes.addAll(createUniProtCategoryIdMappingTypes());
            // add other missing types
            idMappingTypes.addAll(createMissingIdMappingTypes());

            // replace special chars in name
            idMappingTypes =
                    idMappingTypes.stream()
                            .map(
                                    detail ->
                                            new UniProtDatabaseDetail(
                                                    convertDisplayNameToName(
                                                            detail.getDisplayName()),
                                                    detail.getDisplayName(),
                                                    detail.getCategory(),
                                                    detail.getUriLink(),
                                                    detail.getAttributes(),
                                                    detail.isImplicit(),
                                                    detail.getLinkedReason(),
                                                    detail.getIdMappingName()))
                            .collect(Collectors.toList());
        }

        return idMappingTypes;
    }

    public static String convertDbNameToPIRDbName(String database) {
        if (Utils.nullOrEmpty(uniProtToPIRDbNameMap)) {
            uniProtToPIRDbNameMap =
                    getAllIdMappingTypes().stream()
                            .collect(
                                    Collectors.toMap(
                                            UniProtDatabaseDetail::getName,
                                            UniProtDatabaseDetail::getIdMappingName));
        }

        return uniProtToPIRDbNameMap.get(database);
    }

    public static String convertDisplayNameToName(String value) {
        value = value.replaceAll("[/,()]", "-");
        value = value.replace(" ", "_");
        return value;
    }

    private static List<UniProtDatabaseDetail> createUniProtCategoryIdMappingTypes() {
        UniProtDatabaseCategory category = UniProtDatabaseCategory.UNKNOWN;
        UniProtDatabaseDetail uniProtKBAcc =
                new UniProtDatabaseDetail(
                        "UniProtKB", "UniProtKB", category, null, null, false, null, PIR_ACC_STR);
        UniProtDatabaseDetail uniProtKBAccId =
                new UniProtDatabaseDetail(
                        "UniProtKB AC/ID",
                        "UniProtKB AC/ID",
                        category,
                        null,
                        null,
                        false,
                        null,
                        PIR_ACC_ID_STR);

        UniProtDatabaseDetail uniProtSwissProtKBId =
                new UniProtDatabaseDetail(
                        "UniProtKB/Swiss-Prot",
                        "UniProtKB/Swiss-Prot",
                        category,
                        null,
                        null,
                        false,
                        null,
                        PIR_SWISSPROT_STR);

        UniProtDatabaseDetail uniParc =
                new UniProtDatabaseDetail(
                        "UniParc", "UniParc", category, null, null, false, null, PIR_UPARC_STR);
        UniProtDatabaseDetail uniRef50 =
                new UniProtDatabaseDetail(
                        "UniRef50", "UniRef50", category, null, null, false, null, PIR_UNIREF50_STR);
        UniProtDatabaseDetail uniRef90 =
                new UniProtDatabaseDetail(
                        "UniRef90", "UniRef90", category, null, null, false, null, PIR_UNIREF90_STR);
        UniProtDatabaseDetail uniRef100 =
                new UniProtDatabaseDetail(
                        "UniRef100", "UniRef100", category, null, null, false, null, PIR_UNIREF100_STR);
        UniProtDatabaseDetail geneName =
                new UniProtDatabaseDetail(
                        "Gene Name", "Gene Name", category, null, null, false, null, PIR_GENENAME_STR);
        UniProtDatabaseDetail crc64 =
                new UniProtDatabaseDetail(CRC64, CRC64, category, null, null, false, null, PIR_CRC64);
        return List.of(
                uniProtKBAcc,
                uniProtKBAccId,
                uniProtSwissProtKBId,
                uniParc,
                uniRef50,
                uniRef90,
                uniRef100,
                geneName,
                crc64);
    }

    private static List<UniProtDatabaseDetail> createMissingIdMappingTypes() {
        UniProtDatabaseCategory sequence = UniProtDatabaseCategory.SEQUENCE_DATABASES;

        UniProtDatabaseDetail embl =
                new UniProtDatabaseDetail(
                        "EMBL/GenBank/DDBJ",
                        "EMBL/GenBank/DDBJ",
                        sequence,
                        null,
                        null,
                        false,
                        null,
                        "EMBL_ID");
        UniProtDatabaseDetail emblCds =
                new UniProtDatabaseDetail(
                        "EMBL/GenBank/DDBJ CDS",
                        "EMBL/GenBank/DDBJ CDS",
                        sequence,
                        null,
                        null,
                        false,
                        null,
                        "EMBL");

        UniProtDatabaseCategory gma = UniProtDatabaseCategory.GENOME_ANNOTATION_DATABASES;

        UniProtDatabaseDetail ensemblProtein =
                new UniProtDatabaseDetail(
                        "Ensembl Protein",
                        "Ensembl Protein",
                        gma,
                        null,
                        null,
                        false,
                        null,
                        "ENSEMBL_PRO_ID");
        UniProtDatabaseDetail ensemblTrans =
                new UniProtDatabaseDetail(
                        "Ensembl Transcript",
                        "Ensembl Transcript",
                        gma,
                        null,
                        null,
                        false,
                        null,
                        "ENSEMBL_TRS_ID");
        UniProtDatabaseDetail ensemblGenome =
                new UniProtDatabaseDetail(
                        "Ensembl Genomes",
                        "Ensembl Genomes",
                        gma,
                        null,
                        null,
                        false,
                        null,
                        "ENSEMBLGENOME_ID");
        UniProtDatabaseDetail ensemblGenomeProtein =
                new UniProtDatabaseDetail(
                        "Ensembl Genomes Protein",
                        "Ensembl Genomes Protein",
                        gma,
                        null,
                        null,
                        false,
                        null,
                        "ENSEMBLGENOME_PRO_ID");
        UniProtDatabaseDetail ensemblGenomeTrans =
                new UniProtDatabaseDetail(
                        "Ensembl Genomes Transcript",
                        "Ensembl Genomes Transcript",
                        gma,
                        null,
                        null,
                        false,
                        null,
                        "ENSEMBLGENOME_TRS_ID");

        UniProtDatabaseCategory org = UniProtDatabaseCategory.ORGANISM_SPECIFIC_DATABASES;
        UniProtDatabaseDetail wormBaseProtein =
                new UniProtDatabaseDetail(
                        "WormBase Protein",
                        "WormBase Protein",
                        org,
                        null,
                        null,
                        false,
                        null,
                        "WORMBASE_PRO_ID");
        UniProtDatabaseDetail wormBaseTranscript =
                new UniProtDatabaseDetail(
                        "WormBase Transcript",
                        "WormBase Transcript",
                        org,
                        null,
                        null,
                        false,
                        null,
                        "WORMBASE_TRS_ID");
        return List.of(
                embl,
                emblCds,
                ensemblProtein,
                ensemblTrans,
                ensemblGenome,
                ensemblGenomeProtein,
                ensemblGenomeTrans,
                wormBaseProtein,
                wormBaseTranscript);
    }

    private static boolean hasIdMappingName(UniProtDatabaseDetail type) {
        return Utils.notNullNotEmpty(type.getIdMappingName());
    }
}
