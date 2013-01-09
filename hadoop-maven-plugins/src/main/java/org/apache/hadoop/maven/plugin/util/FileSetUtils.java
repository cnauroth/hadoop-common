/*
 * Copyright 2012 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.maven.plugin.util;

import org.apache.maven.model.FileSet;
import org.codehaus.plexus.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileSetUtils {

  private static String getCommaSeparatedList(List list) {
    StringBuilder buffer = new StringBuilder();
    String separator = "";
    for (Object e : list) {
      buffer.append(separator).append(e);
      separator = ",";
    }
    return buffer.toString();
  }

  @SuppressWarnings("unchecked")
  public static List<File> convertFileSetToFiles(FileSet source) throws IOException {
    String includes = getCommaSeparatedList(source.getIncludes());
    String excludes = getCommaSeparatedList(source.getExcludes());
    return FileUtils.getFiles(new File(source.getDirectory()), includes, excludes);
  }
}
