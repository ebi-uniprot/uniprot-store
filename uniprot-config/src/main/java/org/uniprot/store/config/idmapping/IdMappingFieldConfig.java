package org.uniprot.store.config.idmapping;

import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtDatabaseCategory;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;

/**
 * @author sahmad
 * @created 26/02/2021
 */
public class IdMappingFieldConfig {
    private static final UniProtDatabaseTypes ALL_DB_TYPES = UniProtDatabaseTypes.INSTANCE;

    public static List<UniProtDatabaseDetail> getAllIdMappingTypes() {
        // get all the fields which has idmappingname set
        List<UniProtDatabaseDetail> idMappingTypes =
                ALL_DB_TYPES.getAllDbTypes().stream()
                        .filter(IdMappingFieldConfig::test)
                        .collect(Collectors.toList());
        // add db names for UniProt category
        idMappingTypes.addAll(createUniProtCategoryIdMappingTypes());
        // add other missing types
        idMappingTypes.addAll(createMissingIdMappingTypes());

        return idMappingTypes;
    }

    private static List<UniProtDatabaseDetail> createUniProtCategoryIdMappingTypes() {
        UniProtDatabaseCategory category = UniProtDatabaseCategory.UNKNOWN;
        UniProtDatabaseDetail uniProtKBAcc =
                new UniProtDatabaseDetail(
                        "UniProtKB", "UniProtKB", category, null, null, false, null, "ACC");
        UniProtDatabaseDetail uniProtKBId =
                new UniProtDatabaseDetail(
                        "UniProtKB", "UniProtKB", category, null, null, false, null, "ID");
        UniProtDatabaseDetail uniParc =
                new UniProtDatabaseDetail(
                        "UniParc", "UniParc", category, null, null, false, null, "UPARC");
        UniProtDatabaseDetail uniRef50 =
                new UniProtDatabaseDetail(
                        "UniRef50", "UniRef50", category, null, null, false, null, "NF50");
        UniProtDatabaseDetail uniRef90 =
                new UniProtDatabaseDetail(
                        "UniRef90", "UniRef90", category, null, null, false, null, "NF90");
        UniProtDatabaseDetail uniRef100 =
                new UniProtDatabaseDetail(
                        "UniRef100", "UniRef100", category, null, null, false, null, "NF100");
        UniProtDatabaseDetail geneName =
                new UniProtDatabaseDetail(
                        "Gene Name", "Gene Name", category, null, null, false, null, "GENENAME");
        UniProtDatabaseDetail crc64 =
                new UniProtDatabaseDetail(
                        "CRC64", "CRC64", category, null, null, false, null, "CRC64");
        return List.of(
                uniProtKBAcc, uniProtKBId, uniParc, uniRef50, uniRef90, uniRef100, geneName, crc64);
    }

    private static List<UniProtDatabaseDetail> createMissingIdMappingTypes() {
        UniProtDatabaseCategory sequence = UniProtDatabaseCategory.SEQUENCE_DATABASES;
        UniProtDatabaseDetail emblCds =
                new UniProtDatabaseDetail(
                        "EMBL CDS", "EMBL CDS", sequence, null, null, false, null, "EMBL");
        UniProtDatabaseDetail geneBankCds =
                new UniProtDatabaseDetail(
                        "GeneBank CDS", "GeneBank CDS", sequence, null, null, false, null, "EMBL");
        UniProtDatabaseDetail ddbjCds =
                new UniProtDatabaseDetail(
                        "DDBJ CDS", "DDBJ CDS", sequence, null, null, false, null, "EMBL");

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
                emblCds,
                geneBankCds,
                ddbjCds,
                ensemblProtein,
                ensemblTrans,
                ensemblGenome,
                ensemblGenomeProtein,
                ensemblGenomeTrans,
                wormBaseProtein,
                wormBaseTranscript);
    }

    private static boolean test(UniProtDatabaseDetail type) {
        return Utils.notNullNotEmpty(type.getIdMappingName());
    }

    private IdMappingFieldConfig() {}
}
