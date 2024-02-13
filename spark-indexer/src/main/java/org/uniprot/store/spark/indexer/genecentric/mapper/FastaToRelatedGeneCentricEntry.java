package org.uniprot.store.spark.indexer.genecentric.mapper;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.uniprot.core.fasta.UniProtKBFasta;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.genecentric.impl.ProteinBuilder;
import org.uniprot.core.parser.fasta.uniprot.UniProtKBFastaParser;
import org.uniprot.core.uniprotkb.UniProtKBAccession;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
public class FastaToRelatedGeneCentricEntry extends FastaToGeneCentricEntry {

    private static final long serialVersionUID = -8317477346908862682L;

    @Override
    Tuple2<String, GeneCentricEntry> parseEntry(
            String proteomeId, Tuple2<LongWritable, Text> fastaTuple) {
        Tuple2<String, GeneCentricEntry> result = null;
        String fastaInput = fastaTuple._2.toString();

        UniProtKBFasta uniProtKBFasta;
        try {
            uniProtKBFasta = UniProtKBFastaParser.fromFastaString(fastaInput);
        } catch (Exception e) {
            throw new IndexHPSDocumentsException(
                    "In Proteome: " + proteomeId + ", unable to parse fastaInput: " + fastaInput,
                    e);
        }
        Protein protein = ProteinBuilder.from(uniProtKBFasta).build();

        // Related protein names contains the prefix: Isoform of P0CX05,
        String[] splitProteinName = protein.getProteinName().split(",");

        String prefix = splitProteinName[0];
        String canonicalAccession = prefix.substring(prefix.lastIndexOf(" ") + 1);

        if (!canonicalAccession.equalsIgnoreCase("readthrough")) {
            UniProtKBAccession accession =
                    new UniProtKBAccessionBuilder(canonicalAccession).build();
            if (!accession.isValidAccession()) {
                throw new IllegalArgumentException(
                        "Related protein fasta file must have valid prefix \"Isoform of <Accession>,\", proteomeId: "
                                + proteomeId
                                + ", id: "
                                + protein.getId()
                                + ", protein name: "
                                + protein.getProteinName());
            }
            Protein canonicalProtein = new ProteinBuilder().id(canonicalAccession).build();

            // creating related protein name without Protein Name Prefix
            Protein relatedProtein = getRelatedProteinWithoutPrefix(protein, splitProteinName);

            GeneCentricEntry entry =
                    new GeneCentricEntryBuilder()
                            .proteomeId(proteomeId)
                            .canonicalProtein(canonicalProtein)
                            .relatedProteinsAdd(relatedProtein)
                            .build();

            result = new Tuple2<>(canonicalAccession, entry);
        } else {
            result = new Tuple2<>(canonicalAccession, null);
        }
        return result;
    }

    private Protein getRelatedProteinWithoutPrefix(
            Protein fastaProtein, String[] splitProteinName) {
        String proteinName = Arrays.stream(splitProteinName).skip(1).collect(Collectors.joining());
        return ProteinBuilder.from(fastaProtein).proteinName(proteinName.trim()).build();
    }
}
