package org.uniprot.store.indexer.help;

import org.springframework.batch.item.ItemReader;
import org.uniprot.store.search.document.help.HelpDocument;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * @author sahmad
 * @created 06/07/2021
 */
public class HelpPageItemReader implements ItemReader<HelpDocument> {
    private final Iterator<Path> fileIterator;
    private final HelpPageReader reader;

    public HelpPageItemReader(String directoryPath) throws IOException {
        DirectoryStream.Filter<Path> filter = path -> path.toFile().isFile() && path.toString().endsWith(".md")
                && !path.toString().endsWith("Home.md");
        this.fileIterator = Files.newDirectoryStream(Paths.get(directoryPath), filter).iterator();
        this.reader = new HelpPageReader();
    }

    @Override
    public HelpDocument read() throws Exception {
        while(this.fileIterator.hasNext()){
            String absPath = this.fileIterator.next().toAbsolutePath().normalize().toString();
            HelpDocument helpDocument = this.reader.read(absPath);
            return helpDocument;
        }
        return null;
    }
}
