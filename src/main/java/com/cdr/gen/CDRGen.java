package com.cdr.gen;

import com.cdr.gen.util.IOUtils;
import com.cdr.gen.util.JavaUtils;
import com.google.common.io.Files;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("dd/MM/yyyy");
    private DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("HH:mm:ss");

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

    private String getHeaders() {
        Boolean includeLineId = (Boolean) config.get("includeLineId");

        List<String> headers = new ArrayList<>();
        headers.add("uuid");
        headers.add("cell_id");
        headers.add("lat");
        headers.add("lon");
        headers.add("calling_number");
        if (includeLineId != null && includeLineId) {
            headers.add("line_id");
        }
        headers.add("called_number");
        headers.add("start_date_dd_mm_yyyy");
        headers.add("end_date_dd_mm_yyyy");
        headers.add("start_time_hh_mm_ss");
        headers.add("end_time_hh_mm_ss");
        headers.add("call_type");
        headers.add("cost");
        headers.add("fraud");
        headers.add("fraud_2");
        return String.join(",", headers);
    }

    private String getRow(Person p, Call c) {
        Boolean includeLineId = (Boolean) config.get("includeLineId");

        List<String> row = new ArrayList<>();
        row.add(c.getId().toString());
        row.add(c.getCell().getId());
        row.add(Double.toString(c.getCell().getLat()));
        row.add(Double.toString(c.getCell().getLon()));
        row.add(p.getPhoneNumber());
        if (includeLineId != null && includeLineId) {
            row.add(Integer.toString(c.getLine()));
        }
        row.add(c.getDestPhoneNumber());
        row.add(c.getTime().getStart().toString(dateFormatter));
        row.add(c.getTime().getEnd().toString(dateFormatter));
        row.add(c.getTime().getStart().toString(timeFormatter));
        row.add(c.getTime().getEnd().toString(timeFormatter));
        row.add(c.getType());
        row.add(Double.toString(c.getCost()));
        row.add(Fraud.FAR.equals(c.getFraud()) ? "1" : "0");
        row.add(Fraud.UNUSUAL.equals(c.getFraud()) ? "1" : "0");
        return String.join(",", row);
    }

    public void saveToFile(String outputFile, List<Person> customers) {

        try {
            FileWriter fw = new FileWriter(outputFile);
            String newLine = System.getProperty("line.separator");


            boolean displayHeaders = Boolean.parseBoolean(config.getOrDefault("displayHeaders", false).toString());

            if (displayHeaders) {
                fw.append(getHeaders() + newLine);
            }

            for (Person p : customers) {
                for (Call c : p.getCalls()) {
                    fw.append(getRow(p, c) + newLine);
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

            Option inputS3Bucket = new Option("s3Bucket", true, "S3 bucket when AWS variables is setup, ");
            inputS3Bucket.setRequired(false);
            options.addOption(inputS3Bucket);
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

        String getS3Bucket() {
            return cmd.getOptionValue("s3Bucket");
        }

    }

    private static S3Client createS3Client(String bucket) {
        if (bucket == null) return null;

        try {
            S3Client s3 = S3Client.create();
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            return s3;
        } catch (Exception e) {
            LOG.error("S3 was not configured properly", e);
            return null;
        }
    }

    public static void main(String[] args) {
        CDRArgs cdrArgs = new CDRArgs(args);
        cdrArgs.validate();

        int threadCount = cdrArgs.getThreadCount();
        String configFile = cdrArgs.getConfig();

        ExecutorService executor = Executors.newFixedThreadPool(cdrArgs.getThreadPoolSize());

        String s3Bucket = cdrArgs.getS3Bucket();
        final S3Client s3 = createS3Client(s3Bucket);

        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                String fileName = String.format("%s-%s.csv", cdrArgs.getPrefix(), UUID.randomUUID());
                LOG.info(String.format("[%s] Starting: %s", Thread.currentThread().getName(), fileName));
                CDRGen generator = new CDRGen(configFile);

                Population population = new Population(generator.getConfig());
                population.create();

                List<Person> customers = population.getPopulation();

                LOG.info(String.format("[%s] Saving file: %s", Thread.currentThread().getName(), fileName));
                generator.saveToFile(fileName, customers);
                Path path = Paths.get(fileName);

                if (s3 != null) {
                    try {
                        PutObjectRequest req = PutObjectRequest.builder()
                                .bucket(s3Bucket)
                                .key(fileName)
                                .build();
                        s3.putObject(req, path);
                    } catch (Exception e) {
                        LOG.error("Error while sending file "+fileName+" to S3", e);
                    } finally {
                        try {
                            java.nio.file.Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            LOG.info("File was not deleted: " + path);
                        }
                    }
                }
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
