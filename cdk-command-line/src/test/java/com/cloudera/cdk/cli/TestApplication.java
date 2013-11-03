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

import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@RunWith(Parameterized.class)
public class TestApplication {

  private static final Logger logger = LoggerFactory.getLogger(TestApplication.class);

  private final List<String[]> commandLines;

  @Parameterized.Parameters
  public static List<Object[]> parameters() {
    String schemaStr = "{ \"type\": \"record\", \"name\": \"Message\", \"fields\": [ { \"type\": \"string\", \"name\": \"message\" } ] }";

    return Lists.newArrayList(
      new Object[] { Lists.<String[]>newArrayList(new String[] { "--help" }) },
      new Object[] { Lists.<String[]>newArrayList(new String[] { "--create" }) },
      new Object[] { Lists.<String[]>newArrayList(new String[] { "--create --drop" }) },
      new Object[] { Lists.<String[]>newArrayList(new String[] { "-c --drop" }) },
      new Object[] {
        Lists.newArrayList(
          new String[] { "--create", "--repo", "repo:file:///tmp/cli-test", "--name", "test", "--schema", schemaStr },
          new String[] { "--drop", "--repo", "repo:file:///tmp/cli-test", "--name", "test" }
        )
      },
      new Object[] {
        Lists.newArrayList(
          new String[] { "--create", "--repo", "repo:file:target/cli-test", "--name", "test", "--schema", schemaStr },
          new String[] { "--drop", "--repo", "repo:file:target/cli-test", "--name", "test" }
        )
      }
    );
  }

  public TestApplication(List<String[]> parameters) {
    this.commandLines = parameters;
  }

  @Test
  public void testOptions() {
    Application app = new Application();

    logger.debug("Command lines:{}", commandLines);

    for (String[] commandLine : commandLines) {
      logger.debug("Trying commandLines:{}", commandLine);
      app.run(commandLine);
    }

//    try {
//      logger.debug("Trying --create --repo repo:hdfs://localhost:8020/tmp/cli-test --name test --schema {}", schemaStr);
//      app.run(new String[] { "--create", "--repo", "repo:hdfs://localhost:8020/tmp/cli-test", "--name", "test", "--schema", schemaStr });
//      app.run(new String[] { "--drop", "--repo", "repo:hdfs://localhost:8020/tmp/cli-test", "--name", "test" });
//    } catch (DatasetRepositoryException e) {
//      logger.info("Failed to create an HDFS dataset; assuming HDFS is not running.", e);
//    }
  }

}
