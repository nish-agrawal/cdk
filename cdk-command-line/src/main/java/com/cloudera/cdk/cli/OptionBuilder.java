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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.commons.cli.Option;

public class OptionBuilder implements Supplier<Option> {

  private String shortOption;
  private String longOption;
  private String description;
  private boolean isRequired;
  private int argCount;
  private String argName;
  private boolean optionalArg;

  public OptionBuilder shortOption(String shortOption) {
    this.shortOption = shortOption;
    return this;
  }

  public OptionBuilder longOption(String longOption) {
    this.longOption = longOption;
    return this;
  }

  public OptionBuilder description(String description) {
    this.description = description;
    return this;
  }

  public OptionBuilder argName(String argName) {
    this.argName = argName;
    return this;
  }

  public OptionBuilder isRequired(boolean isRequired) {
    this.isRequired = isRequired;
    return this;
  }

  public OptionBuilder argCount(int argCount) {
    this.argCount = argCount;
    return this;
  }

  public OptionBuilder optionalArg(boolean optionalArg) {
    this.optionalArg = optionalArg;
    return this;
  }

  @Override
  public Option get() {
    Preconditions.checkState(shortOption != null, "Missing required shortOption");
    Preconditions.checkState(description != null, "Missing required description");

    Option option = new Option(shortOption, description);

    if (longOption != null) {
      option.setLongOpt(longOption);
    }

    if (argName != null) {
      option.setArgName(argName);
    }

    if (optionalArg) {
      option.setOptionalArg(true);
    }

    option.setArgs(argCount);
    option.setRequired(isRequired);

    return option;
  }

}
