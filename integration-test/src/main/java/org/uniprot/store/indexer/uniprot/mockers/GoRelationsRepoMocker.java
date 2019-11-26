package org.uniprot.store.indexer.uniprot.mockers;

import org.uniprot.store.indexer.uniprot.go.GoRelationFileReader;
import org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo;
import org.uniprot.store.indexer.uniprot.go.GoRelationRepo;
import org.uniprot.store.indexer.uniprot.go.GoTermFileReader;

public class GoRelationsRepoMocker {

    public static GoRelationRepo getGoRelationRepo() {
        String gotermPath =
                Thread.currentThread().getContextClassLoader().getResource("goterm").getFile();
        return GoRelationFileRepo.create(
                new GoRelationFileReader(gotermPath), new GoTermFileReader(gotermPath));
    }
}
