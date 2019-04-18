package uk.ac.ebi.uniprot.indexer.uniprot.mockers;


import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;

public class GoRelationsRepoMocker {

    public static GoRelationRepo getGoRelationRepo() {
        String gotermPath = Thread.currentThread().getContextClassLoader().getResource("goterm").getFile();
        return GoRelationFileRepo.create(new GoRelationFileReader(gotermPath),
                new GoTermFileReader(gotermPath));
    }
}
