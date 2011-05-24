/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.WpsConfig;
import com.bc.calvalus.processing.shellexec.FileUtil;
import com.bc.io.IOUtils;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Logger;

/**
 * Formatter for the outputs generated by the L3Tool.
 * <pre>
 *   Usage:
 *       <b>L3Formatter</b> <i>input-dir</i> <i>output-file</i> <b>RGB</b>  <i>r-band</i> <i>r-v-min</i> <i>r-v-max</i>  <i>g-band</i> <i>g-v-min</i> <i>g-v-max</i>  <i>b-band</i> <i>b-v-min</i> <i>b-v-max</i>
 *   or
 *       <b>L3Formatter</b> <i>input-dir</i> <i>output-file</i> <b>Grey</b>  <i>band</i> <i>v-min</i> <i>v-max</i>  [ <i>band</i> <i>v-min</i> <i>v-max</i> ... ]
 * </pre>
 *
 * @author Norman Fomferra
 */
public class L3FormatterTool extends Configured implements Tool {
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static Options options = new Options();


    public static void main(String[] args) {
        if (Boolean.getBoolean("hadoop.debug")) {
            waitForDebuggerToConnect();
        }
        int result;
        try {
            result = ToolRunner.run(new L3FormatterTool(), args);
        } catch (Exception ex) {
            System.err.println("Error: " + ex.getMessage());
            ex.printStackTrace(System.err);
            result = -1;
        }
        System.exit(result);
    }


    // todo: replace by configuration (e.g. in shell script) to start application suspended
    // like in :
    //     if [ ! -z  $CV_DEBUG ]; then
    //         CV_OPTIONS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8001,server=y,suspend=y"
    //     fi
    private static void waitForDebuggerToConnect() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        // Halt here so that Norman can start the IDE debugger
        System.out.print("Connect debugger to the JVM and press return!");
        try {
            reader.readLine();
        } catch (IOException e) {
            // ?
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // parse command line arguments
        CommandLineParser commandLineParser = new PosixParser();
        final CommandLine commandLine = commandLineParser.parse(options, args);
        String[] remainingArgs = commandLine.getArgs();
        if (remainingArgs.length == 0) {
            throw new IllegalStateException("No request file specified.");
        }
        String requestPath = remainingArgs[0];

        // parse request
        final String formattingWpsRequest = FileUtil.readFile(requestPath);

        WpsConfig formattingWpsConfig = new WpsConfig(formattingWpsRequest);
        L3FormatterConfig formatterConfig = L3FormatterConfig.create(formattingWpsConfig.getFormatterParameters());
        String hadoopJobOutputDir = formattingWpsConfig.getRequestOutputDir();

        String processingWps = loadProcessingWpsXml(hadoopJobOutputDir);
        WpsConfig level3Wpsconfig = new WpsConfig(processingWps);
        L3Config l3Config = L3Config.fromXml(level3Wpsconfig.getLevel3Parameters());

        L3Formatter formatter = new L3Formatter(getConf());

        Geometry roiGeometry = JobUtils.createGeometry(level3Wpsconfig.getGeometry());
        return formatter.format(formatterConfig, l3Config, hadoopJobOutputDir, roiGeometry);
    }

    private String loadProcessingWpsXml(String hadoopJobOutputDir) throws IOException {
        Path l3OutputDir = new Path(hadoopJobOutputDir);
        FileSystem fs = l3OutputDir.getFileSystem(getConf());
        InputStream is = fs.open(new Path(l3OutputDir, L3Config.L3_REQUEST_FILENAME));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(is, baos);
        return baos.toString();
    }
}

