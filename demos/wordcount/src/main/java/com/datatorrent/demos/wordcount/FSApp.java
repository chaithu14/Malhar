/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.wordcount;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.FileSplitter;
import com.datatorrent.lib.io.fs.FixedBytesBlockReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;




@ApplicationAnnotation(name="FileSplitterDemo")
public class FSApp implements StreamingApplication
{

  public static class BlockReader extends FixedBytesBlockReader
  {

    private String directory; // Same as FileSpiltter directory.

    @Override
    public void setup(Context.OperatorContext context)
    {

      super.setup(context);
      // Overwriting fs
      try {
        fs = FileSystem.newInstance(new Path(directory).toUri(), configuration);
      } catch (IOException e) {
        throw new RuntimeException("Unable to create filesystem instance for " + directory, e);
      }

    }

    public String getDirectory()
    {
      return directory;
    }

    public void setDirectory(String directory)
    {
      this.directory = directory;
    }

  }
   @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileSplitter input = dag.addOperator("FileSplitter", new FileSplitter());
    BlockReader blockReader = dag.addOperator("BlockReader", new BlockReader());

    //UniqueCounter<String> wordCount = dag.addOperator("count", new UniqueCounter<String>());
    dag.addStream("wordinput-count", input.blocksMetadataOutput, blockReader.blocksMetadataInput);
    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("count-console",blockReader.messages, consoleOperator.input);
  }
}
