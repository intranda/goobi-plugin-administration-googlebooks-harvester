package de.intranda.goobi.plugins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class ProcessOutputReader implements Runnable {

    private InputStream inputStream;
    private StringBuilder sb = new StringBuilder();
    private boolean keepOutput = true;

    public ProcessOutputReader(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public void run() {
        String line = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            while ((line = reader.readLine()) != null) {
                sb.append(line);
                sb.append('\n');
            }
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    public static char[] decodeWithCharset(byte[] origBytes, Charset charset) throws CharacterCodingException {
        CharsetDecoder decoder = charset.newDecoder();

        ByteBuffer byteBuffer = ByteBuffer.wrap(origBytes);
        CharBuffer charBuffer = decoder.decode(byteBuffer);

        return charBuffer.array();
    }

    public String getOutput() {
        return sb.toString();
    }

    public void writeToFile(File file) throws IOException {
        getFileFromString(getOutput(), file, false);
    }

    private File getFileFromString(String string, File file, boolean append) throws IOException {

        try (FileWriter writer = new FileWriter(file, append)) {
            writer.write(string);
        }
        return file;
    }

    public boolean isKeepOutput() {
        return keepOutput;
    }

    public void setKeepOutput(boolean keepOutput) {
        this.keepOutput = keepOutput;
    }

}
