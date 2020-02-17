package de.intranda.goobi.plugins;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.goobi.beans.LogEntry;
import org.goobi.beans.Step;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.PluginLoader;
import org.goobi.production.plugin.interfaces.IOpacPlugin;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.CloseStepHelper;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.ImportPluginException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.StepManager;
import de.unigoettingen.sub.search.opac.ConfigOpac;
import de.unigoettingen.sub.search.opac.ConfigOpacCatalogue;
import lombok.extern.log4j.Log4j;
import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Prefs;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.TypeNotAllowedForParentException;
import ugh.exceptions.WriteException;

@Log4j
public class QuartzJob implements Job {
    private static XPathFactory xFactory = XPathFactory.instance();
    private static Namespace metsNs = Namespace.getNamespace("METS", "http://www.loc.gov/METS/");
    private static Namespace marcNs = Namespace.getNamespace("marc", "http://www.loc.gov/MARC21/slim");
    private static XPathExpression<Element> datafield955Xpath =
            xFactory.compile("//METS:xmlData/marc:record/marc:datafield[@tag='955']", Filters.element(), null, metsNs, marcNs);
    private static XPathExpression<Element> subfieldAXpath = xFactory.compile("./marc:subfield[@code='a']", Filters.element(), null, metsNs, marcNs);
    private static XPathExpression<Element> subfieldBXpath = xFactory.compile("./marc:subfield[@code='b']", Filters.element(), null, metsNs, marcNs);

    private static Path runningPath = Paths.get("/tmp/gbooksharvester_running");
    private static Path stopPath = Paths.get("/tmp/gbooksharvester_stop");
    private final static long G = 1073741824;
    private final static long M = 1048576;

    @Override
    public void execute(JobExecutionContext context) {
        log.debug("Execute job: " + context.getJobDetail().getName() + " - " + context.getRefireCount());

        int numberHarvested = 0;
        XMLConfiguration config = ConfigPlugins.getPluginConfig("intranda_administration_googlebooks-harvester");

        if (Files.exists(stopPath)) {
            log.warn("Googlebooks harvester: File '/tmp/gbooksharvester_stop' exists. Will not run.");
            return;
        }
        if (Files.exists(runningPath)) {
            log.warn("Googlebooks harvester: File '/tmp/gbooksharvester_running' exists. Will not run.");
            return;
        }
        if (!checkBufferFree(config)) {
            log.warn("Googlebooks harvester: not enough free space in metadata dir. Aborting.");
            return;
        }

        String[] convertedBooks;
        try {
            convertedBooks = getConvertedBooks(config);
        } catch (IOException | InterruptedException e) {
            log.error("Googlebooks harvester: error getting converted books", e);
            return;
        }

        try {
            Files.createFile(runningPath);
            for (String convertedBook : convertedBooks) {
                String id = convertedBook.replace("NLI_", "").replace(".tar.gz.gpg", "");
                String processTitle = "Google-" + id;
                int count = ProcessManager.countProcessTitle(processTitle);
                if (count != 0) {
                    continue;
                }
                try {
                    log.debug(String.format("Googlebooks harvester: Downloading %s", convertedBook));
                    org.goobi.beans.Process goobiProcess = downloadAndImportBook(convertedBook, processTitle, id, config);
                    if (goobiProcess == null) {
                        continue;
                    }
                    Step myStep = null;
                    for (Step step : goobiProcess.getSchritte()) {
                        if (step.getBearbeitungsstatusEnum() == StepStatus.OPEN) {
                            myStep = step;
                            break;
                        }
                    }
                    if (myStep == null) {
                        String message = "Could not find first open step. Aborting.";
                        log.error(message);
                        writeLogEntry(goobiProcess, message);
                        continue;
                    }
                    CloseStepHelper.closeStep(myStep, null);
                    numberHarvested++;
                } catch (IOException | InterruptedException | DAOException | SwapException e) {
                    log.error("Googlebooks harvester: error downloading book:", e);
                }

                //check for free space after each book
                if (!checkBufferFree(config)) {
                    log.warn("Googlebooks harvester: not enough free space in metadata dir. Aborting.");
                    return;
                }
                if (Files.exists(stopPath)) {
                    log.warn("Googlebooks harvester: File '/tmp/gbooksharvester_stop' exists. Will not run.");
                    return;
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error(e);
        } finally {
            if (Files.exists(runningPath)) {
                try {
                    Files.delete(runningPath);
                } catch (IOException e) {
                    log.error("trying to delete running file:", e);
                }
            }
        }

        try {
            convertBooks(config);
        } catch (IOException | InterruptedException e) {
            log.error("Googlebooks harvester: error processing books", e);
            return;
        }

        log.debug(String.format("Googlebooks harvester: %d processes were created.", numberHarvested));

    }

    private void convertBooks(XMLConfiguration config) throws IOException, InterruptedException {
        int maxNumberToConvert = config.getInt("numberToConvertHourly", 5);
        String scriptDir = config.getString("scriptDir", "/opt/digiverso/goobi/scripts/googlebooks/");
        ProcessBuilder pb =
                new ProcessBuilder("/usr/bin/env", "python", "grin_oath.py", "--directory", "NLI", "--resource", "_available?format=text");
        pb.directory(new File(scriptDir));
        Process p = pb.start();
        ProcessOutputReader stdoutReader = new ProcessOutputReader(p.getInputStream());
        Thread stdoutThread = new Thread(stdoutReader);
        stdoutThread.start();

        ProcessOutputReader stderrReader = new ProcessOutputReader(p.getErrorStream());
        Thread stderrThread = new Thread(stderrReader);
        stderrThread.start();

        int result = p.waitFor();
        stdoutThread.join(1000);
        stderrThread.join(1000);

        if (result != 0) {
            throw new IOException(String.format("GRIN available-script exited with code %d. Stderr was: %s", result, stderrReader.getOutput()));
        }
        log.debug("Googlebooks harvester: get available books error output:\n\n" + stderrReader.getOutput());

        if (StringUtils.isBlank(stdoutReader.getOutput())) {
            return;
        }

        String[] books = stdoutReader.getOutput().split("\n");
        log.debug("Googlebooks harvester: number of available books: " + books.length);
        if (books.length > 0) {
            log.debug("Googlebooks harvester: first available book: " + books[0]);
        }

        int numberToConvert = Math.min(maxNumberToConvert, books.length);
        String barcodes = Arrays.stream(books).limit(numberToConvert).collect(Collectors.joining(","));

        String[] command =
                new String[] { "/usr/bin/env", "python", "grin_oath.py", "--directory", "NLI", "--resource", "_process?barcodes=" + barcodes };

        log.debug("Googlebooks harvester: calling the shell to convert books:" + Arrays.asList(command));
        pb = new ProcessBuilder("/usr/bin/env", "python", "grin_oath.py", "--directory", "NLI", "--resource", "_process?barcodes=" + barcodes);
        pb.directory(new File(scriptDir));
        p = pb.start();

        stdoutReader = new ProcessOutputReader(p.getInputStream());
        stdoutThread = new Thread(stdoutReader);
        stdoutThread.start();

        stderrReader = new ProcessOutputReader(p.getErrorStream());
        stderrThread = new Thread(stderrReader);
        stderrThread.start();

        result = p.waitFor();
        stdoutThread.join(1000);
        stderrThread.join(1000);

        log.debug("Googlebooks: _process call stdout: " + stdoutReader.getOutput());
        log.debug("Googlebooks: _process call stderr: " + stderrReader.getOutput());
        if (result != 0) {
            throw new IOException(String.format("GRIN process-script exited with code %d. Stderr was: %s", result, stderrReader.getOutput()));
        }
    }

    public String[] getConvertedBooks(XMLConfiguration config) throws IOException, InterruptedException {
        String scriptDir = config.getString("scriptDir", "/opt/digiverso/goobi/scripts/googlebooks/");
        ProcessBuilder pb =
                new ProcessBuilder("/usr/bin/env", "python", "grin_oath.py", "--directory", "NLI", "--resource", "_converted?format=text");
        pb.directory(new File(scriptDir));
        Process p = pb.start();
        ProcessOutputReader stdoutReader = new ProcessOutputReader(p.getInputStream());
        Thread stdoutThread = new Thread(stdoutReader);
        stdoutThread.start();

        ProcessOutputReader stderrReader = new ProcessOutputReader(p.getErrorStream());
        Thread stderrThread = new Thread(stderrReader);
        stderrThread.start();

        int result = p.waitFor();
        stdoutThread.join(1000);
        stderrThread.join(1000);

        if (result != 0) {
            throw new IOException(String.format("GRIN script exited with code %d. Stderr was: %s", result, stderrReader.getOutput()));
        }

        if (StringUtils.isBlank(stdoutReader.getOutput())) {
            return new String[0];
        }

        return stdoutReader.getOutput().split("\n");

    }

    private org.goobi.beans.Process downloadAndImportBook(String convertedBook, String processTitle, String id, XMLConfiguration config)
            throws IOException, InterruptedException, DAOException, SwapException {
        org.goobi.beans.Process goobiProcess = createProcess(processTitle, config);
        String scriptDir = config.getString("scriptDir", "/opt/digiverso/goobi/scripts/googlebooks/");
        Path goobiImagesSourceDir = Paths.get(goobiProcess.getSourceDirectory());
        if (!Files.exists(goobiImagesSourceDir)) {
            Files.createDirectories(goobiImagesSourceDir);
        }
        Path downloadPath = goobiImagesSourceDir.resolve(convertedBook);
        ProcessBuilder pb = new ProcessBuilder("/usr/bin/env", "python", "grin_oath.py", "--directory", "NLI", "--resource", convertedBook, "-o",
                downloadPath.toAbsolutePath().toString());
        pb.directory(new File(scriptDir));
        Process p = pb.start();
        ProcessOutputReader stdoutReader = new ProcessOutputReader(p.getInputStream());
        Thread stdoutThread = new Thread(stdoutReader);
        stdoutThread.start();

        ProcessOutputReader stderrReader = new ProcessOutputReader(p.getErrorStream());
        Thread stderrThread = new Thread(stderrReader);
        stderrThread.start();

        int result = p.waitFor();
        stdoutThread.join(1000);
        stderrThread.join(1000);

        if (result != 0) {
            StorageProvider.getInstance().deleteDir(Paths.get(goobiProcess.getProcessDataDirectory()));
            ProcessManager.deleteProcess(goobiProcess);
            throw new IOException(String.format("GRIN script exited with code %d. Stderr was: %s", result, stderrReader.getOutput()));
        }

        //decrypt stuff...
        String outputName = convertedBook.replace(".gpg", "");
        Path decryptPath = goobiImagesSourceDir.resolve(outputName);
        Process gpgProcess = new ProcessBuilder("/usr/bin/gpg", "--no-use-agent", "--passphrase", config.getString("passphrase"), "--output",
                decryptPath.toAbsolutePath().toString(), "-d", downloadPath.toAbsolutePath().toString()).start();

        stderrReader = new ProcessOutputReader(gpgProcess.getErrorStream());
        stderrThread = new Thread(stderrReader);
        stderrThread.start();

        stdoutReader = new ProcessOutputReader(gpgProcess.getInputStream());
        stdoutThread = new Thread(stdoutReader);
        stdoutThread.start();

        gpgProcess.waitFor();
        if (gpgProcess.exitValue() != 0) {
            log.error("Googlebooks: gpg stdout: " + stdoutReader.getOutput());
            log.error("Googlebooks: gpg stderr: " + stderrReader.getOutput());
            throw new IOException("could not decrypt gpg file: " + downloadPath.toAbsolutePath().toString());
        }

        Files.delete(downloadPath);

        //extract stuff...
        Path masterFolder = Paths.get(goobiProcess.getImagesOrigDirectory(false));
        if (!Files.exists(masterFolder)) {
            Files.createDirectories(masterFolder);
        }
        Path hOCRFolder = Paths.get(goobiProcess.getOcrXmlDirectory().replace("_xml", "_hocr"));
        if (!Files.exists(hOCRFolder)) {
            Files.createDirectories(hOCRFolder);
        }
        Path ocrTxtFolder = Paths.get(goobiProcess.getOcrTxtDirectory());
        if (!Files.exists(ocrTxtFolder)) {
            Files.createDirectories(ocrTxtFolder);
        }
        Path googleMetsFile = null;
        try (GZIPInputStream gzIn = new GZIPInputStream(Files.newInputStream(decryptPath));
                TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn)) {
            TarArchiveEntry currEntry = null;
            while ((currEntry = tarIn.getNextTarEntry()) != null) {
                String name = currEntry.getName();
                if (name.endsWith("jp2")) {
                    //copy to master folder
                    Files.copy(tarIn, masterFolder.resolve(name));
                } else if (name.endsWith("html")) {
                    //copy to OCR-hOCR folder
                    Files.copy(tarIn, hOCRFolder.resolve(name));
                } else if (name.endsWith("txt")) {
                    //copy to OCR txt folder
                    Files.copy(tarIn, ocrTxtFolder.resolve(name));
                } else if (name.endsWith("xml")) {
                    googleMetsFile = goobiImagesSourceDir.resolve(name);
                    Files.copy(tarIn, googleMetsFile);
                }
            }
        }

        String idFromMarc = null;
        try {
            idFromMarc = readIdFromMarc(googleMetsFile);
        } catch (JDOMException e) {
            log.error(e);
            writeLogEntry(goobiProcess, "Could not read identifier from google METS file. See log for details");
            Step firstStep = goobiProcess.getSchritte().get(0);
            firstStep.setBearbeitungsstatusEnum(StepStatus.ERROR);
            StepManager.saveStep(firstStep);
            return null;
        }

        if (idFromMarc == null) {
            writeLogEntry(goobiProcess, "Could not read identifier from google METS file.");
            Step firstStep = goobiProcess.getSchritte().get(0);
            firstStep.setBearbeitungsstatusEnum(StepStatus.ERROR);
            StepManager.saveStep(firstStep);
            return null;
        }

        try {
            Prefs prefs = goobiProcess.getRegelsatz().getPreferences();
            Fileformat ff = getRecordFromCatalogue(prefs, idFromMarc, "NLI Alma", "1007");
            DigitalDocument digDoc = ff.getDigitalDocument();
            DocStruct physical = digDoc.createDocStruct(prefs.getDocStrctTypeByName("BoundBook"));
            digDoc.setPhysicalDocStruct(physical);
            goobiProcess.writeMetadataFile(ff);
        } catch (PreferencesException | WriteException | TypeNotAllowedForParentException | ImportPluginException e) {
            log.error(e);
            writeLogEntry(goobiProcess, "Could not import metadata from catalogue.");
            Step firstStep = goobiProcess.getSchritte().get(0);
            firstStep.setBearbeitungsstatusEnum(StepStatus.ERROR);
            StepManager.saveStep(firstStep);
            return null;
        }

        return goobiProcess;
        //TODO (maybe check checksums) 
    }

    public static String readIdFromMarc(Path googleMetsFile) throws IOException, JDOMException {
        String idFromMarc = null;
        try (InputStream metsIn = Files.newInputStream(googleMetsFile)) {
            Document doc = new SAXBuilder().build(metsIn);
            List<Element> idEls = datafield955Xpath.evaluate(doc);
            Element idEl = null;
            for (Element dataField : idEls) {
                Element subA = subfieldAXpath.evaluateFirst(dataField);
                Element subB = subfieldBXpath.evaluateFirst(dataField);
                boolean subAOK = subA.getTextTrim() != null && subA.getTextTrim().toLowerCase().contains("stacks");
                boolean subBOK = subB.getTextTrim() != null && subB.getTextTrim().contains("-");
                if (subAOK && subBOK) {
                    idEl = subB;
                    break;
                }
            }
            if (idEl != null) {
                idFromMarc = idEl.getText().trim();
            }
        }
        return idFromMarc;
    }

    public static void writeLogEntry(org.goobi.beans.Process goobiProcess, String message) {
        LogEntry entry = new LogEntry();
        entry.setContent(message);
        entry.setProcessId(goobiProcess.getId());
        entry.setType(LogType.ERROR);
        ProcessManager.saveLogEntry(entry);
    }

    private org.goobi.beans.Process createProcess(String processTitle, XMLConfiguration config) throws DAOException {
        org.goobi.beans.Process template = ProcessManager.getProcessByTitle(config.getString("templateTitle"));
        org.goobi.beans.Process processCopy = new org.goobi.beans.Process();
        processCopy.setTitel(processTitle);
        processCopy.setIstTemplate(false);
        processCopy.setInAuswahllisteAnzeigen(false);
        processCopy.setProjekt(template.getProjekt());
        processCopy.setRegelsatz(template.getRegelsatz());
        processCopy.setDocket(template.getDocket());

        /*
         *  Kopie der Processvorlage anlegen
         */
        BeanHelper bHelper = new BeanHelper();
        bHelper.SchritteKopieren(template, processCopy);
        bHelper.ScanvorlagenKopieren(template, processCopy);
        bHelper.WerkstueckeKopieren(template, processCopy);
        bHelper.EigenschaftenKopieren(template, processCopy);

        ProcessManager.saveProcess(processCopy);

        return processCopy;
    }

    public boolean checkBufferFree(XMLConfiguration config) {
        boolean bufferFree = false;
        String bufStr = config.getString("buffer");
        if (bufStr == null) {
            log.error("buffer not set. Using 150G");
            bufStr = "150G";
        }
        char unit = bufStr.charAt(bufStr.length() - 1);
        Long buffer;
        try {
            buffer = Long.parseLong(bufStr.substring(0, bufStr.length() - 1));
            if (unit == 'G') {
                buffer = buffer * G;
            } else if (unit == 'M') {
                buffer = buffer * M;
            } else {
                log.error("could not read unit. Using G");
                buffer = buffer * G;
            }
        } catch (NumberFormatException e) {
            log.error("could not parse buffer. Using 150G");
            buffer = 150l * G;
        }

        Path metadataDir = Paths.get(ConfigurationHelper.getInstance().getMetadataFolder());
        try {
            long free = Files.getFileStore(metadataDir.toRealPath()).getUsableSpace();
            if (buffer < free) {
                bufferFree = true;
            }
        } catch (IOException e) {
            log.error(e);
        }
        return bufferFree;
    }

    private Fileformat getRecordFromCatalogue(Prefs prefs, String identifier, String opacName, String searchField) throws ImportPluginException {
        ConfigOpacCatalogue coc = ConfigOpac.getInstance().getCatalogueByName(opacName);
        if (coc == null) {
            throw new ImportPluginException("Catalogue with name " + opacName + " not found. Please check goobi_opac.xml");
        }
        IOpacPlugin myImportOpac = (IOpacPlugin) PluginLoader.getPluginByTitle(PluginType.Opac, coc.getOpacType());
        if (myImportOpac == null) {
            throw new ImportPluginException("Opac plugin " + coc.getOpacType() + " not found. Abort.");
        }
        Fileformat myRdf = null;
        try {
            myRdf = myImportOpac.search(searchField, identifier, coc, prefs);
            if (myRdf == null) {
                throw new ImportPluginException("Could not import record " + identifier
                        + ". Usually this means a ruleset mapping is not correct or the record can not be found in the catalogue.");
            }
        } catch (Exception e1) {
            throw new ImportPluginException("Could not import record " + identifier
                    + ". Usually this means a ruleset mapping is not correct or the record can not be found in the catalogue.");
        }
        DocStruct ds = null;
        DocStruct anchor = null;
        try {
            ds = myRdf.getDigitalDocument().getLogicalDocStruct();
            if (ds.getType().isAnchor()) {
                anchor = ds;
                if (ds.getAllChildren() == null || ds.getAllChildren().isEmpty()) {
                    throw new ImportPluginException(
                            "Could not import record " + identifier + ". Found anchor file, but no children. Try to import the child record.");
                }
                ds = ds.getAllChildren().get(0);
            }
        } catch (PreferencesException e1) {
            throw new ImportPluginException("Could not import record " + identifier
                    + ". Usually this means a ruleset mapping is not correct or the record can not be found in the catalogue.");
        }

        return myRdf;
    }
}
