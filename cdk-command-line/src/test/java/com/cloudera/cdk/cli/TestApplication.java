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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestApplication {

  private static final Logger logger = LoggerFactory.getLogger(TestApplication.class);

  @Test
  public void testOptions() {
    Application app = new Application();

    logger.debug("Trying --help");
    app.run(new String[] { "--help" });

    logger.debug("Trying --create");
    app.run(new String[] { "--create" });

    logger.debug("Trying --create --drop");
    app.run(new String[] { "--create", "--drop" });

    logger.debug("Trying -c --drop");
    app.run(new String[] { "-c", "--drop" });

    logger.debug("Trying --create --repo dsr:file:///tmp/cli-test --name test --schema bar");
    app.run(new String[] { "--create", "--repo", "dsr:file:///tmp/cli-test", "--name", "test", "--schema", "{ \"type\": \"record\", \"name\": \"Message\", \"fields\": [ { \"type\": \"string\", \"name\": \"message\" } ] }" });
    app.run(new String[] { "--drop", "--repo", "dsr:file:///tmp/cli-test", "--name", "test" });

    logger.debug("Trying --create --repo dsr:file:target/cli-test --name test --schema bar");
    app.run(new String[] { "--create", "--repo", "dsr:file:target/cli-test", "--name", "test", "--schema", "bar" });

    logger.debug("Trying --create --repo dsr:hdfs://localhost:8020/tmp/cli-test --name test --schema bar");
    app.run(new String[] { "--create", "--repo", "dsr:hdfs://localhost:8020/tmp/cli-test", "--name", "test", "--schema", "bar" });
  }

}
