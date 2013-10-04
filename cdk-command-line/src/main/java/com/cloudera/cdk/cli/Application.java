/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.cli;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.Formats;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

public class Application implements Command {

  private static final Logger logger = LoggerFactory.getLogger(Application.class);

  private Options options;
  private CommandLine commandLine;

  public static void main(String[] args) {
    Application application = new Application();

    application.run(args);
  }

  @Override
  public void run(String[] args) {
    buildOptions();

    try {
      parseOptions(args);

      if (commandLine.hasOption("create")) {
        DatasetRepository repo = DatasetRepositories.connect(new URI(commandLine.getOptionValue("repo")));

        DatasetDescriptor.Builder descBuilder = new DatasetDescriptor.Builder()
          .schema(commandLine.getOptionValue("schema"));

        if (commandLine.hasOption("format")) {
          String format = commandLine.getOptionValue("format");

          // There's no support for Formats.parseFormat(String) yet. :(
          if (format.equals("avro")) {
            descBuilder.format(Formats.AVRO);
          } else if (format.equals("parquet")) {
            descBuilder.format(Formats.PARQUET);
          } else {
            throw new ParseException("Unknown dataset format:" + format + " specified");
          }
        }

        repo.create(commandLine.getOptionValue("name"), descBuilder.get());
      } else if (commandLine.hasOption("update")) {
        DatasetRepository repo = DatasetRepositories.connect(new URI(commandLine.getOptionValue("repo")));
        String name = commandLine.getOptionValue("name");

        repo.update(
          commandLine.getOptionValue("name"),
          new DatasetDescriptor.Builder(repo.load(name).getDescriptor())
            .schema(commandLine.getOptionValue("schema"))
            .get()
        );
      } else if (commandLine.hasOption("drop")) {
        DatasetRepository repo = DatasetRepositories.connect(new URI(commandLine.getOptionValue("repo")));

        repo.delete(commandLine.getOptionValue("name"));
      } else {
        HelpFormatter helpFormatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.out);

        helpFormatter.printHelp(pw, 74,
          "cdk-cli [-h|-c|-u|-d] [command-specific options]", "\nOptions:", options, 2, 3, null);

        pw.append(
          Joiner.on("\n").join(
            "\nOne of --help, --create, --delete, or --update must be provided.\n",
            "  --create requires: --repo, --name, --schema",
            "  --drop   requires: --repo, --name",
            "  --update requires: --repo, --name",
            "           supports: --schema\n"
          )
        );

        pw.flush();
      }
    } catch (ParseException e) {
      logger.error(e.getMessage());
      logger.debug("Exception follows", e);
    } catch (URISyntaxException e) {
      logger.error("Invalid dataset repository URI - {}", e.getMessage());
      logger.debug("Exception follows", e);
    }

  }

  private void buildOptions() {
    options = new Options();

    // Global options
    options.addOption(new OptionBuilder().shortOption("h").longOption("help")
      .description("display help").get());

    // Dataset global options
    options.addOption(new OptionBuilder().shortOption("r").longOption("repo")
      .argCount(1).description("dataset repository URI").argName("uri").get());
    options.addOption(new OptionBuilder().shortOption("n").longOption("name")
      .argCount(1).description("dataset name").argName("name").get());
    options.addOption(new OptionBuilder().shortOption("c").longOption("create")
      .description("create a dataset").get());
    options.addOption(new OptionBuilder().shortOption("u").longOption("update")
      .description("update a dataset").get());
    options.addOption(new OptionBuilder().shortOption("d").longOption("drop")
      .description("drop a dataset").get());

    // Dataset create options
    options.addOption(new OptionBuilder().shortOption("f").longOption("format")
      .argCount(1).argName("parquet|avro").description("dataset format").get());

    // Dataset create or update options
    options.addOption(new OptionBuilder().shortOption("s").longOption("schema")
      .argCount(1).argName("avro schema").description("dataset schema").get());
    options.addOption(new OptionBuilder().shortOption("p").longOption("permissions")
      .argCount(1).argName("perms").description("dataset permissions").get());
    options.addOption(new OptionBuilder().shortOption("L").longOption("level")
      .optionalArg(true).argCount(1).argName("level")
      .description("logging level").get());
  }

  private void parseOptions(String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    commandLine = parser.parse(options, args);

    // Override all log4j settings if the user specifies something.
    if (commandLine.hasOption("level")) {
      Level level = Level.toLevel(commandLine.getOptionValue("level", "INFO"));

      LogManager.getRootLogger().setLevel(level);
      logger.debug("Set log level on logger:{} to:{}", LogManager.getRootLogger().getName(), level);

      Enumeration currentLoggers = LogManager.getCurrentLoggers();
      while (currentLoggers.hasMoreElements()) {
        org.apache.log4j.Logger l = (org.apache.log4j.Logger) currentLoggers.nextElement();
        l.setLevel(level);
        logger.debug("Set log level on logger:{} to:{}", l.getName(), level);
      }
    }

      /*
       * Apache Commons CLI doesn't support fine grained control over option
       * compatibility so we build a pair of tables to track exclusivity and
       * dependencies.
       */

    Map<String, Set<String>> exclusiveOf = Maps.newHashMap();
    Map<String, Set<String>> requiredWith = Maps.newHashMap();

    exclusiveOf.put("create", Sets.newHashSet("drop", "update"));
    exclusiveOf.put("update", Sets.newHashSet("create", "drop", "format", "permissions"));
    exclusiveOf.put("drop", Sets.newHashSet("create", "update", "format", "permissions", "schema"));

    requiredWith.put("create", Sets.newHashSet("repo", "name", "schema"));
    requiredWith.put("update", Sets.newHashSet("repo", "name"));
    requiredWith.put("drop", Sets.newHashSet("repo", "name"));

    for (Option option : commandLine.getOptions()) {
      if (exclusiveOf.containsKey(option.getLongOpt())) {
        for (String incompat : exclusiveOf.get(option.getLongOpt())) {
          if (commandLine.hasOption(incompat)) {
            throw new ParseException("Options --" + option.getLongOpt() + " and --" + incompat + " are incompatible");
          }
        }
      }

      if (requiredWith.containsKey(option.getLongOpt())) {
        for (String required : requiredWith.get(option.getLongOpt())) {
          if (!commandLine.hasOption(required)) {
            throw new ParseException("Option --" + option.getLongOpt() + " requires option --" + required + " to be present");
          }
        }
      }
    }
  }

}