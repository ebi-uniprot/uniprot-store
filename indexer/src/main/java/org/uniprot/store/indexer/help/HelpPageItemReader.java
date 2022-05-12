package org.uniprot.store.indexer.help;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemReader;
import org.uniprot.store.search.document.help.HelpDocument;

/**
 * @author sahmad
 * @created 06/07/2021
 */
public class HelpPageItemReader implements ItemReader<HelpDocument> {
    private final Iterator<Path> fileIterator;
    private final HelpPageReader reader;

    @SuppressWarnings("squid:S2095")
    public HelpPageItemReader(List<String> directoryPaths) throws IOException {
        Predicate<Path> isValidMdFile =
                path ->
                        path.toFile().isFile()
                                && path.toString().endsWith(".md")
                                && !path.toString().endsWith("Home.md");

        Stream<Path> pathsStream = Stream.empty();
        for (String directoryPath : directoryPaths) {
            Stream<Path> pathStream = Files.list(Paths.get(directoryPath)).filter(isValidMdFile);
            pathsStream = Stream.concat(pathsStream, pathStream);
        }

        this.fileIterator = pathsStream.iterator();
        this.reader = new HelpPageReader();
    }

    @Override
    public HelpDocument read() throws Exception {
        while (this.fileIterator.hasNext()) {
            String absPath = this.fileIterator.next().toAbsolutePath().normalize().toString();
            return this.reader.read(absPath);
        }
        return null;
    }
}
