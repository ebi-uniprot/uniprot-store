package org.uniprot.store.config.idmapping;

import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtDatabaseCategory;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;

/**
 * @author sahmad
 * @created 26/02/2021 This class contains all the possible fields of "from" and "to" in id-mapping
 *     service. from-to fields = drlineconfiguration.json where "idMappingName" is set + objects of
 *     type UniProtDatabaseDetail created in this class. Each from-to field has 3 names -
 *     displayName, restAPIName and PIRName. displayName - User readable name. restAPIName - A name
 *     user will pass during REST call. PIR Name - Name passed to PIR service during internal
 *     call(Refer https://idmapping.uniprot.org/cgi-bin/idmapping_http_client for all supported
 *     names) e.g. (displayName, restAPIName, PIRName) = ("UniProtKB AC/ID", "UniProtKB_AC-ID",
 *     "ACC,ID"). restAPIName is basically derived from display name by replacing space with
 *     underscore(_) and, forward slash and bracket with dash(-)
 */
public class IdMappingFieldConfig {
    public static final String UNIPROTKB_STR = "UniProtKB";
    public static final String UNIPROTKB_AC_ID_STR = "UniProtKB AC/ID";
    public static final String UNIPROTKB_SWISS_STR = "UniProtKB/Swiss-Prot";
    public static final String UNIPARC_STR = "UniParc";
    public static final String UNIREF_50_STR = "UniRef50";
    public static final String UNIREF_90_STR = "UniRef90";
    public static final String UNIREF_100_STR = "UniRef100";
    public static final String GENE_NAME_STR = "Gene Name";
    public static final String ACC_STR = convertDisplayNameToName(UNIPROTKB_STR);
    public static final String ACC_ID_STR = convertDisplayNameToName(UNIPROTKB_AC_ID_STR);
    public static final String SWISSPROT_STR = convertDisplayNameToName(UNIPROTKB_SWISS_STR);
    public static final String UPARC_STR = convertDisplayNameToName(UNIPARC_STR);
    public static final String UNIREF50_STR = convertDisplayNameToName(UNIREF_50_STR);
    public static final String UNIREF90_STR = convertDisplayNameToName(UNIREF_90_STR);
    public static final String UNIREF100_STR = convertDisplayNameToName(UNIREF_100_STR);
    public static final String GENENAME_STR = convertDisplayNameToName(GENE_NAME_STR);

    public static final String PIR_CRC64 = "CRC64";
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
                    ALL_DB_TYPES.getUniProtKBDbTypes().stream()
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
            idMappingTypes =
                    new ArrayList<>(
                            new LinkedHashSet<>(
                                    idMappingTypes)); // to avoid duplicate due to race condition
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

    public static boolean isValidDbName(String database) {
        return Utils.notNull(convertDbNameToPIRDbName(database));
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
                        UNIPROTKB_STR,
                        UNIPROTKB_STR,
                        category,
                        "https://www.uniprot.org/uniprot/%id/entry",
                        null,
                        false,
                        null,
                        PIR_ACC_STR);
        UniProtDatabaseDetail uniProtKBAccId =
                new UniProtDatabaseDetail(
                        UNIPROTKB_AC_ID_STR,
                        UNIPROTKB_AC_ID_STR,
                        category,
                        "https://www.uniprot.org/uniprot/%id/entry",
                        null,
                        false,
                        null,
                        PIR_ACC_ID_STR);

        UniProtDatabaseDetail uniProtSwissProtKBId =
                new UniProtDatabaseDetail(
                        UNIPROTKB_SWISS_STR,
                        UNIPROTKB_SWISS_STR,
                        category,
                        "https://www.uniprot.org/uniprot/%id/entry",
                        null,
                        false,
                        null,
                        PIR_SWISSPROT_STR);

        UniProtDatabaseDetail uniParc =
                new UniProtDatabaseDetail(
                        UNIPARC_STR,
                        UNIPARC_STR,
                        category,
                        "https://www.uniprot.org/uniparc/%id/entry",
                        null,
                        false,
                        null,
                        PIR_UPARC_STR);
        UniProtDatabaseDetail uniRef50 =
                new UniProtDatabaseDetail(
                        UNIREF_50_STR,
                        UNIREF_50_STR,
                        category,
                        "https://www.uniprot.org/uniref/%id",
                        null,
                        false,
                        null,
                        PIR_UNIREF50_STR);
        UniProtDatabaseDetail uniRef90 =
                new UniProtDatabaseDetail(
                        UNIREF_90_STR,
                        UNIREF_90_STR,
                        category,
                        "https://www.uniprot.org/uniref/%id",
                        null,
                        false,
                        null,
                        PIR_UNIREF90_STR);
        UniProtDatabaseDetail uniRef100 =
                new UniProtDatabaseDetail(
                        UNIREF_100_STR,
                        UNIREF_100_STR,
                        category,
                        "https://www.uniprot.org/uniref/%id",
                        null,
                        false,
                        null,
                        PIR_UNIREF100_STR);
        UniProtDatabaseDetail geneName =
                new UniProtDatabaseDetail(
                        GENE_NAME_STR,
                        GENE_NAME_STR,
                        category,
                        null,
                        null,
                        false,
                        null,
                        PIR_GENENAME_STR);
        UniProtDatabaseDetail crc64 =
                new UniProtDatabaseDetail(
                        CRC64, CRC64, category, null, null, false, null, PIR_CRC64);
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

    static List<UniProtDatabaseDetail> createMissingIdMappingTypes() {
        UniProtDatabaseCategory sequence = UniProtDatabaseCategory.SEQUENCE_DATABASES;

        UniProtDatabaseDetail embl =
                new UniProtDatabaseDetail(
                        "EMBL/GenBank/DDBJ",
                        "EMBL/GenBank/DDBJ",
                        sequence,
                        "https://www.ebi.ac.uk/ena/data/view/%id",
                        null,
                        false,
                        null,
                        "EMBL_ID");
        UniProtDatabaseDetail emblCds =
                new UniProtDatabaseDetail(
                        "EMBL/GenBank/DDBJ CDS",
                        "EMBL/GenBank/DDBJ CDS",
                        sequence,
                        "https://www.ebi.ac.uk/ena/data/view/%id",
                        null,
                        false,
                        null,
                        "EMBL");
        UniProtDatabaseDetail giNumber =
                new UniProtDatabaseDetail(
                        "GI number", "GI number", sequence, null, null, false, null, "P_GI");
        UniProtDatabaseDetail refSeqNucleotide =
                new UniProtDatabaseDetail(
                        "RefSeq Nucleotide",
                        "RefSeq Nucleotide",
                        sequence,
                        "https://www.ncbi.nlm.nih.gov/nuccore/%id",
                        null,
                        false,
                        null,
                        "REFSEQ_NT_ID");

        UniProtDatabaseDetail refSeqProtein =
                new UniProtDatabaseDetail(
                        "RefSeq Protein",
                        "RefSeq Protein",
                        sequence,
                        "https://www.ncbi.nlm.nih.gov/protein/%id",
                        null,
                        false,
                        null,
                        "P_REFSEQ_AC");

        UniProtDatabaseCategory gma = UniProtDatabaseCategory.GENOME_ANNOTATION_DATABASES;

        UniProtDatabaseDetail ensemblProtein =
                new UniProtDatabaseDetail(
                        "Ensembl Protein",
                        "Ensembl Protein",
                        gma,
                        "https://www.ensembl.org/id/%id",
                        null,
                        false,
                        null,
                        "ENSEMBL_PRO_ID");
        UniProtDatabaseDetail ensemblTrans =
                new UniProtDatabaseDetail(
                        "Ensembl Transcript",
                        "Ensembl Transcript",
                        gma,
                        "https://www.ensembl.org/id/%id",
                        null,
                        false,
                        null,
                        "ENSEMBL_TRS_ID");
        UniProtDatabaseDetail ensemblGenome =
                new UniProtDatabaseDetail(
                        "Ensembl Genomes",
                        "Ensembl Genomes",
                        gma,
                        "http://www.ensemblgenomes.org/id/%id",
                        null,
                        false,
                        null,
                        "ENSEMBLGENOME_ID");
        UniProtDatabaseDetail ensemblGenomeProtein =
                new UniProtDatabaseDetail(
                        "Ensembl Genomes Protein",
                        "Ensembl Genomes Protein",
                        gma,
                        "http://www.ensemblgenomes.org/id/%id",
                        null,
                        false,
                        null,
                        "ENSEMBLGENOME_PRO_ID");
        UniProtDatabaseDetail ensemblGenomeTrans =
                new UniProtDatabaseDetail(
                        "Ensembl Genomes Transcript",
                        "Ensembl Genomes Transcript",
                        gma,
                        "http://www.ensemblgenomes.org/id/%id",
                        null,
                        false,
                        null,
                        "ENSEMBLGENOME_TRS_ID");

        UniProtDatabaseDetail wbParasiteTranscript =
                new UniProtDatabaseDetail(
                        "WBParaSite Transcript/Protein",
                        "WBParaSite Transcript/Protein",
                        gma,
                        "https://parasite.wormbase.org/id/%id",
                        null,
                        false,
                        null,
                        "WBPARASITE_TRS_PRO_ID");

        UniProtDatabaseCategory org = UniProtDatabaseCategory.ORGANISM_SPECIFIC_DATABASES;
        UniProtDatabaseDetail wormBaseProtein =
                new UniProtDatabaseDetail(
                        "WormBase Protein",
                        "WormBase Protein",
                        org,
                        "https://parasite.wormbase.org/id/%id",
                        null,
                        false,
                        null,
                        "WORMBASE_PRO_ID");
        UniProtDatabaseDetail wormBaseTranscript =
                new UniProtDatabaseDetail(
                        "WormBase Transcript",
                        "WormBase Transcript",
                        org,
                        "https://parasite.wormbase.org/id/%id",
                        null,
                        false,
                        null,
                        "WORMBASE_TRS_ID");
        return List.of(
                embl,
                emblCds,
                giNumber,
                refSeqNucleotide,
                refSeqProtein,
                ensemblProtein,
                ensemblTrans,
                ensemblGenome,
                ensemblGenomeProtein,
                ensemblGenomeTrans,
                wbParasiteTranscript,
                wormBaseProtein,
                wormBaseTranscript);
    }

    private static boolean hasIdMappingName(UniProtDatabaseDetail type) {
        return Utils.notNullNotEmpty(type.getIdMappingName());
    }

    private IdMappingFieldConfig() {}
}
