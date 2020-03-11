package de.intranda.goobi.plugins;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.jdom2.JDOMException;
import org.junit.Test;

public class XpathTest {
    @Test
    public void testIdentifierXpath() throws IOException, JDOMException {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get("test/resources/metsFiles"))) {
            for (Path p : dirStream) {
                System.out.println("reading " + p);
                CatalogueIdentifier identifer = QuartzJob.readIdFromMarc(p);
                assertTrue(identifer != null && !identifer.getSearchValue().isEmpty());
            }
        }
    }
}
