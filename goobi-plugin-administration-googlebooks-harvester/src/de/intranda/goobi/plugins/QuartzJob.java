package de.intranda.goobi.plugins;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.goobi.beans.LogEntry;
import org.goobi.beans.Step;
import org.goobi.production.enums.LogType;
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
import de.sub.goobi.helper.HelperSchritte;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.StepManager;
import lombok.extern.log4j.Log4j;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.Prefs;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.ReadException;
import ugh.exceptions.WriteException;

@Log4j
public class QuartzJob implements Job {
    private static XPathFactory xFactory = XPathFactory.instance();
    private static Namespace metsNs = Namespace.getNamespace("METS", "http://www.loc.gov/METS/");
    private static Namespace marcNs = Namespace.getNamespace("marc", "http://www.loc.gov/MARC21/slim");
    private static XPathExpression<Element> identifierXpath =
            xFactory.compile("//METS:xmlData/marc:record/marc:controlfield[@tag='001']", Filters.element(), null, metsNs, marcNs);

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
                String processTitle = "Google-" + convertedBook.replace("NLI_", "").replace(".tar.gz.gpg", "");
                int count = ProcessManager.countProcessTitle(processTitle);
                if (count != 0) {
                    continue;
                }
                try {
                    log.debug(String.format("Googlebooks harvester: Downloading %s", convertedBook));
                    org.goobi.beans.Process goobiProcess = downloadAndImportBook(convertedBook, processTitle, config);
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
                    new HelperSchritte().CloseStepObjectAutomatic(myStep);
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

        log.debug(String.format("Googlebooks harvester: %d processes were created.", numberHarvested));

    }

    private org.goobi.beans.Process downloadAndImportBook(String convertedBook, String processTitle, XMLConfiguration config)
            throws IOException, InterruptedException, DAOException, SwapException {
        org.goobi.beans.Process goobiProcess = createProcess(processTitle, config);
        String scriptDir = config.getString("scriptDir", "/opt/digiverso/goobi/scripts/googlebooks/");
        Path goobiImagesSourceDir = Paths.get(goobiProcess.getSourceDirectory());
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
            throw new IOException(String.format("GRIN script exited with code %d. Stderr was: %s", result, stderrReader.getOutput()));
        }

        //decrypt stuff...
        String outputName = convertedBook.replace(".gpg", "");
        Path decryptPath = goobiImagesSourceDir.resolve(outputName);
        Process gpgProcess = new ProcessBuilder("/usr/bin/gpg", "--passphrase", config.getString("passphrase"), "--output",
                decryptPath.toAbsolutePath().toString(), "-d", downloadPath.toAbsolutePath().toString()).start();
        gpgProcess.waitFor();
        if (gpgProcess.exitValue() != 0) {
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
        try (InputStream metsIn = Files.newInputStream(googleMetsFile)) {
            Document doc = new SAXBuilder().build(metsIn);
            Element idEl = identifierXpath.evaluateFirst(doc);
            if (idEl != null) {
                idFromMarc = idEl.getText().trim();
            }
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
            Metadata catalogidMd = new Metadata(prefs.getMetadataTypeByName("CatalogIDDigital"));
            catalogidMd.setValue(idFromMarc);
            Fileformat ff = goobiProcess.readMetadataFile();
            ff.getDigitalDocument().getLogicalDocStruct().addMetadata(catalogidMd);
            goobiProcess.writeMetadataFile(ff);
        } catch (MetadataTypeNotAllowedException | ReadException | PreferencesException | WriteException e) {
            log.error(e);
        }

        return goobiProcess;
        //TODO (maybe check checksums) 
    }

    public void writeLogEntry(org.goobi.beans.Process goobiProcess, String message) {
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

}
