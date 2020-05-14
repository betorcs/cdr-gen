package com.cdr.gen;

import com.cdr.gen.util.IOUtils;
import com.cdr.gen.util.JavaUtils;
import com.google.common.collect.Range;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * This class only loads the configuration file and handles the saving of the
 * population to a file.
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public final class CDRGen {
    private static final Logger LOG = Logger.getLogger(CDRGen.class);
    private static final String DEFAULT_CONFIG_FILE = "/config.json";
    private Map<String, Object> config;

    public CDRGen() {
        loadConfig(DEFAULT_CONFIG_FILE);
    }

    public CDRGen(String configFile) {
        loadConfig(configFile);
    }

    public void loadConfig(String file) {
        try {
            JSONParser parser = new JSONParser();
            String configStr;

            if (JavaUtils.isJar() && file.equals(DEFAULT_CONFIG_FILE)) {
                InputStream is = CDRGen.class.getResourceAsStream(file);
                configStr = IOUtils.convertStreamToString(is);
            } else {
                if (file.equals(DEFAULT_CONFIG_FILE))
                    file = "src/main/resources" + file;

                configStr = Files.toString(new File(file), Charset.defaultCharset());
            }

            config = (JSONObject) parser.parse(configStr);
        } catch (IOException ex) {
            LOG.error("Unable to read config file '" + file + "'.", ex);
        } catch (ParseException ex) {
            LOG.error("Error parsing the config file '" + file + "'.", ex);
        }
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void saveToFile(String outputFile, List<Person> customers) {
        DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("dd/MM/yyyy");
        DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("HH:mm:ss");

        try {
            FileWriter fw = new FileWriter(outputFile);
            String newLine = System.getProperty("line.separator");

            for (Person p : customers) {
                for (Call c : p.getCalls()) {
                    fw.append(c.getId() + "," + p.getPhoneNumber() + "," + c.getLine() + "," + c.getDestPhoneNumber()
                            + "," + c.getTime().getStart().toString(dateFormatter) + ","
                            + c.getTime().getEnd().toString(dateFormatter) + ","
                            + c.getTime().getStart().toString(timeFormatter) + ","
                            + c.getTime().getEnd().toString(timeFormatter) + "," + c.getType() + "," + c.getCost()
                            + newLine);
                }
            }

            fw.close();
        } catch (IOException ex) {
            LOG.error("Error while writing the output file.", ex);
        }
    }

    private static class CDRArgs {

        private Options options;
        private String[] args;
        private CommandLine cmd;

        CDRArgs(String[] args) {
            this.args = args;

            options = new Options();

            Option inputPrefix = new Option("n", "namePrefix", true, "Output file prefix, default: cdr");
            inputPrefix.setRequired(false);
            options.addOption(inputPrefix);

            Option inputConfig = new Option("c", "config", true, "Configuration file");
            inputConfig.setRequired(false);
            options.addOption(inputConfig);

            Option inputThreadPool = new Option("p", "threadPool", true, "Number of process in parallel, default 16");
            inputThreadPool.setRequired(false);
            options.addOption(inputThreadPool);

            Option inputThreadCount = new Option("l", "threadCount", true, "Number of process, default 1");
            inputThreadCount.setRequired(false);
            options.addOption(inputThreadCount);
        }

        void validate() {
            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();
            try {
                cmd = parser.parse(options, args);
            } catch (org.apache.commons.cli.ParseException e) {
                formatter.printHelp("cdr", options);
                System.exit(-1);
            }
        }

        String getConfig() {
            return cmd.getOptionValue("config", DEFAULT_CONFIG_FILE);
        }

        String getPrefix() {
            return cmd.getOptionValue("namePrefix", "cdr");
        }

        int getThreadPoolSize() {
            return Integer.parseInt(cmd.getOptionValue("threadPool", "16"));
        }

        int getThreadCount() {
            return Integer.parseInt(cmd.getOptionValue("threadCount", "1"));
        }

    }

    public static void main(String[] args) {
        CDRArgs cdrArgs = new CDRArgs(args);
        cdrArgs.validate();

        int threadCount = cdrArgs.getThreadCount();
        String configFile = cdrArgs.getConfig();

        ExecutorService executor = Executors.newFixedThreadPool(cdrArgs.getThreadPoolSize());

        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                String fileName = String.format("%s-%s.txt", cdrArgs.getPrefix(), Instant.now().toString());
                LOG.info(String.format("[%s] Starting: %s", Thread.currentThread().getName(), fileName));
                CDRGen generator = new CDRGen(configFile);

                Population population = new Population(generator.getConfig());
                population.create();

                List<Person> customers = population.getPopulation();

                LOG.info(String.format("[%s] Saving file: %s", Thread.currentThread().getName(), fileName));
                generator.saveToFile(fileName, customers);
            });
        }

        executor.shutdown();

        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            executor.shutdownNow();
            LOG.error(e);
        }

        LOG.info("Done.");
    }
}
