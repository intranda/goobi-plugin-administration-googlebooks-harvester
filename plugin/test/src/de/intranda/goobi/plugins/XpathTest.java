package de.intranda.goobi.plugins;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.jdom2.JDOMException;
import org.junit.Test;

public class XpathTest {
    @Test
    public void testIdentifierXpath() throws IOException, JDOMException {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get("test/resources/metsFiles"))) {
            for (Path p : dirStream) {
                System.out.println("reading " + p);
                List<CatalogueIdentifier> identifers = QuartzJob.readIdsFromMarc(p);
                assertTrue(identifers != null && !identifers.isEmpty() && !identifers.get(0).getSearchValue().isEmpty());
            }
        }
    }
}
