package de.intranda.goobi;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class PluginInfoTest {

    @Test
    public void testVersion() throws IOException {
        String bla = PluginInfo.convertStreamToString(PluginInfo.class.getResourceAsStream("plugins.txt"));

        assertNotNull(bla);
    }

}
