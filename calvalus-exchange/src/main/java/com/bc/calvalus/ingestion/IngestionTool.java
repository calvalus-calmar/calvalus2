package com.bc.calvalus.ingestion;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A tool to archive all MER_RR__1P or MER_FRS_1P products found in a given directory
 * into a computed directory in the HDFS archive.
 * <pre>
 * Usage:
 *    hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.ingestion.IngestionTool ( ${sourceDir} | ${sourceFiles} ) [-producttype=${productType}] [-revision=${revision}] [-replication=${replication}] [-blocksize=${blocksize}]
 * </pre>
 */
public class IngestionTool extends Configured implements Tool {

    public static final String DEFAULT_PRODUCT_TYPE = "MER_RR__1P";
    public static final String DEFAULT_REVISION = "r03";
    //static final String DEFAULT_PATTERN = "<type>.*\.N1";

    private static Options options;
    public static final SimpleDateFormat YEAR_MONTH_DAY_FORMAT = new SimpleDateFormat("yyyy/MM/dd");
    public static final SimpleDateFormat YEAR_DAY_OF_YEAR_FORMAT = new SimpleDateFormat("yyyyDDD");
    public static final SimpleDateFormat YEAR2_DAY_OF_YEAR_FORMAT = new SimpleDateFormat("yyDDD");

    static {
        options = new Options();
        options.addOption("p", "producttype", true, "product type of uploaded files, defaults to " + DEFAULT_PRODUCT_TYPE);
        options.addOption("r", "revision", true, "revision of uploaded files, defaults to " + DEFAULT_REVISION);
        options.addOption("c", "replication", true, "replication factor of uploaded files, defaults to Hadoop default");
        options.addOption("b", "blocksize", true, "block size in MB for uploaded files, defaults to file size");
        options.addOption("f", "filenamepattern", true, "regular expression matching filenames, defaults to 'type.*\\.N1'");
        options.addOption("v", "verify", false, "verify existence and size to avoid double copying, defaults to false");

        YEAR_MONTH_DAY_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        YEAR_DAY_OF_YEAR_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        YEAR2_DAY_OF_YEAR_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new IngestionTool(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        // parse command line arguments
        CommandLineParser commandLineParser = new PosixParser();
        final CommandLine commandLine = commandLineParser.parse(options, args);
        try {
            FileSystem hdfs = FileSystem.get(getConf());
            return handleIngestionCommand(commandLine, commandLine.getArgs(), hdfs);

        } catch (Exception e) {
            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ingest.sh", options);
            return 1;
        }
    }

    public static int handleIngestionCommand(CommandLine commandLine, String[] files, FileSystem hdfs) throws IOException {
        String productType = DEFAULT_PRODUCT_TYPE;
        String revision = DEFAULT_REVISION;
        long blockSizeParameter = -1;


        if (commandLine.hasOption("producttype")) {
            productType = commandLine.getOptionValue("producttype");
        }
        if (commandLine.hasOption("revision")) {
            revision = commandLine.getOptionValue("revision");
        }
        if (commandLine.hasOption("blocksize")) {
            blockSizeParameter = Long.parseLong(commandLine.getOptionValue("blocksize"));
        }
        final String filenamePattern;
        if (commandLine.hasOption("filenamepattern")) {
            filenamePattern = commandLine.getOptionValue("filenamepattern");
        } else {
            filenamePattern = productType + ".*\\.N1";
        }
        Pattern pattern = Pattern.compile(filenamePattern);

        final short replication;
        if (commandLine.hasOption("replication")) {
            replication = Short.parseShort(commandLine.getOptionValue("replication"));
        } else {
            replication = hdfs.getDefaultReplication();
        }

        boolean verify = commandLine.hasOption("verify");

        // determine input files
        List<File> sourceFiles = new ArrayList<File>();
        for (String path : files) {
            File file = new File(path);
            collectInputFiles(file, pattern, sourceFiles);
        }
        if (sourceFiles.isEmpty()) {
            throw new FileNotFoundException("no files found");
        }
        System.out.format("%d files to be ingested\n", sourceFiles.size());


        return ingest(productType, revision, blockSizeParameter, pattern, hdfs, replication, verify, sourceFiles);
    }

    private static int ingest(String productType, String revision, long blockSizeParameter, Pattern pattern, FileSystem hdfs, short replication, boolean verify, List<File> sourceFiles) throws IOException {
        // cache HDFS parameters for block size
        final int bufferSize = hdfs.getConf().getInt("io.file.buffer.size", 4096);
        final int checksumSize = hdfs.getConf().getInt("io.bytes.per.checksum", 512);

        // loop over input files
        for (File sourceFile : sourceFiles) {
            String archivePath = getArchivePath(sourceFile, productType, revision, pattern);

            // calculate block size to cover complete N1
            // blocksize must be a multiple of checksum size
            long fileSize = sourceFile.length();
            long blockSize;
            if (blockSizeParameter == -1) {
                blockSize = ((fileSize + checksumSize - 1) / checksumSize) * checksumSize;
            } else {
                blockSize = ((blockSizeParameter + checksumSize - 1) / checksumSize) * checksumSize;
            }

            // construct HDFS output stream
            Path destPath = new Path(archivePath, sourceFile.getName());
            // copy if either verification is off or target does not exist or target has different size
            if (! verify || ! hdfs.exists(destPath) || hdfs.listStatus(destPath) == null || hdfs.listStatus(destPath)[0].getLen() < fileSize) {
                int attempt = 1;
                boolean finished = false;
                IOException exception = null;
                System.out.println(MessageFormat.format("archiving {0} in {1}", sourceFile, archivePath));
                while (attempt <= 3 && !finished) {
                    short actualReplication = attempt == 1 ? replication : 3;
                    OutputStream out = hdfs.create(destPath, true, bufferSize, actualReplication, blockSize);
                    FileInputStream in = new FileInputStream(sourceFile);
                    try  {
                        IOUtils.copyBytes(in, out, hdfs.getConf(), true);
                        finished = true;
                        if (actualReplication != replication) {
                            hdfs.setReplication(destPath, replication);
                        }
                    }catch (IOException ioe){
                        System.err.print("copying attempt " + attempt + " failed.");
                        ioe.printStackTrace();
                        exception = ioe;
                    } finally {
                        out.close();
                        in.close();
                    }
                    attempt++;
                }
                if (!finished) {
                    throw new IOException("Failed to copy: " + sourceFile, exception);
                }
            } else {
                System.out.println(MessageFormat.format("skipping {0} existing in {1}", sourceFile, archivePath));
            }
        }

        return 0;
    }

    static void collectInputFiles(File file, Pattern filter, List<File> accu) throws IOException {
        if (file.isDirectory()) {
            if ("lost+found".equals(file.getName())) {
                return;
            }
            final File[] files = file.listFiles();
            if (files == null) {
                throw new FileNotFoundException("cannot access directory " + file.getPath() + ".");
            }
            for (File f : files) {
                collectInputFiles(f, filter, accu);
            }
        } else if (file.isFile()) {
            if (filter.matcher(file.getName()).matches()) {
                accu.add(file);
            }
        } else {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
    }

    /**
     * Implements the archiving "rule"
     *
     *
     * @param sourceFile a file name
     * @param productType the product type
     * @param revision the revision
     * @param pattern
     * @return   an archive path
     */
    static String getArchivePath(File sourceFile, String productType, String revision, Pattern pattern) {
        String subPath = getDatePath(sourceFile, productType, pattern);
        return String.format("/calvalus/eodata/%s/%s/%s", productType, revision, subPath);
    }

    /**
     * Parses file name, determines acquisition date, and constructs subdirectoy path year/month/day,
     * either using the provided pattern if it contains groups or a product type specific pattern
     * if the product type is known
     * <pre>
     *  MER_RR__1PRACR20060530_130506_000026432048_00110_22208_0000.N1
     *  NSS.GHRR.NM.D06365.S1409.E1554.B2348788.WI.gz
     *  A2012280012500.L1A_LAC.bz2
     *  V2KRNP____20070501F083.ZIP
     * </pre>
     *
     * @param sourceFile     file name that contains the concrete date in some encoding
     * @param productType  product type for default pattern selection
     * @param pattern      pattern used if it contains groups in parenthesis for year, month and day or for year and day of year
     * @return             directory path year/mont/day, "." if neither type is known nor pattern contains groups
     * @throws IllegalArgumentException  if pattern does not match
     */
    static String getDatePath(File sourceFile, String productType, Pattern pattern) throws IllegalArgumentException {
        int numberOfParenthesis = countChars(pattern.pattern(), '(');
        if (numberOfParenthesis >= 2) {
            Matcher matcher = pattern.matcher(sourceFile.getName());
            if (! matcher.matches()) {
                throw new IllegalArgumentException("pattern " + pattern.pattern() + " does not match file name " + sourceFile.getName());
            }
            if (matcher.groupCount() == 3) {
                return String.format("%s/%s/%s", matcher.group(1), matcher.group(2), matcher.group(3));
            } else if (matcher.groupCount() == 2 && matcher.group(1).length() == 4 && matcher.group(2).length() == 3) {
                Date date = null;
                try {
                    date = YEAR_DAY_OF_YEAR_FORMAT.parse(matcher.group(1) + matcher.group(2));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("file name " + sourceFile.getName() + " does not contain expected year and dayofyear according to pattern " + pattern.pattern() + ": " + e.getMessage());
                }
                return YEAR_MONTH_DAY_FORMAT.format(date);
            } else if (matcher.groupCount() == 2 && matcher.group(1).length() == 2 && matcher.group(2).length() == 3) {
                Date date = null;
                try {
                    date = YEAR2_DAY_OF_YEAR_FORMAT.parse(matcher.group(1) + matcher.group(2));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("file name " + sourceFile.getName() + " does not contain expected year and dayofyear according to pattern " + pattern.pattern() + ": " + e.getMessage());
                }
                return YEAR_MONTH_DAY_FORMAT.format(date);
            } else {
                throw new IllegalArgumentException("pattern " + pattern.pattern() + " does not contain recognised date in file name " + sourceFile.getName());
            }
        } else if (productType != null && productType.startsWith("MODIS")) {
            try {
                return YEAR_MONTH_DAY_FORMAT.format(YEAR_DAY_OF_YEAR_FORMAT.parse(sourceFile.getName().substring(1, 8)));
            } catch (ParseException e) {
                throw new IllegalArgumentException("file name " + sourceFile + " does not contain recognised date for MODIS default pattern");
            }
        } else if (productType != null && productType.startsWith("SPOT_VGT")) {
            try {
                return YEAR_MONTH_DAY_FORMAT.format(YEAR_DAY_OF_YEAR_FORMAT.parse(sourceFile.getName().substring(10, 18)));
            } catch (ParseException e) {
                throw new IllegalArgumentException("file name " + sourceFile.getName() + " does not contain recognised date for SPOT_VGT default pattern");
            }
        } else if ("MER_RR__1P".equals(productType) || "MER_FRS_1P".equals(productType)) {
            return String.format("%s/%s/%s",
                                 sourceFile.getName().substring(14, 18),
                                 sourceFile.getName().substring(18, 20),
                                 sourceFile.getName().substring(20, 22));
        } else {
            File dayDir = sourceFile.getParentFile();
            if (dayDir == null || dayDir.getName().length() != 2) {
                return ".";
            }
            File monthDir = dayDir.getParentFile();
            if (monthDir == null || monthDir.getName().length() != 2) {
                return ".";
            }
            File yearDir = monthDir.getParentFile();
            if (yearDir == null || yearDir.getName().length() != 4) {
                return ".";
            }
            return String.format("%s/%s/%s", yearDir.getName(), monthDir.getName(), dayDir.getName());
        }
    }

    private static int countChars(String s, char c) {
        int count = 0;
        for (char i : s.toCharArray()) {
            if (i == c) {
                ++count;
            }
        }
        return count;
    }

//    static class ProductFilenameFilter implements FilenameFilter {
//        private final String productType;
//
//        public ProductFilenameFilter(String productType) {
//            this.productType = productType;
//        }
//
//        @Override
//        public boolean accept(File file, String s) {
//            return file.isDirectory() || (s.startsWith(productType) && s.endsWith(".N1"));
//        }
//    }
}
