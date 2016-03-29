package com.datatorrent.lib.io.fs;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.netlet.util.Slice;

/**
 * FS file copy module can be used in conjunction with file input modules to
 * copy files from any file system. This module supports parallel write
 * to multiple blocks of the same file and then stitching those blocks in
 * original sequence.
 *
 * Essential operators are wrapped into single component using Module API.
 *
 */
public abstract class FSCopyOutputModule implements Module
{
  /**
   * Path of the output directory. Relative path of the files copied will be
   * maintained w.r.t. source directory and output directory
   */
  @NotNull
  protected String outputDirectoryPath;

  /**
   * Flag to control if existing file with same name should be overwritten
   */
  private boolean overwriteOnConflict;

  /**
   * Input port for files metadata.
   */
  public final transient ProxyInputPort<AbstractFileSplitter.FileMetadata> filesMetadataInput = new ProxyInputPort<AbstractFileSplitter.FileMetadata>();

  /**
   * Input port for blocks metadata
   */
  public final transient ProxyInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new ProxyInputPort<BlockMetadata.FileBlockMetadata>();

  /**
   * Input port for blocks data
   */
  public final transient ProxyInputPort<AbstractBlockReader.ReaderRecord<Slice>> blockData = new ProxyInputPort<AbstractBlockReader.ReaderRecord<Slice>>();

  // Abstract Methods
  public abstract BlockWriter createBlockWriter();

  public abstract Synchronizer createSynchronizer();

  public abstract FileMerger createFileMerger();

  public BlockWriter blockWriter;
  public Synchronizer synchronizer;
  public FileMerger merger;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Defining DAG
    blockWriter = dag.addOperator("BlockWriter", createBlockWriter());
    synchronizer = dag.addOperator("BlockSynchronizer", createSynchronizer());

    dag.setInputPortAttribute(blockWriter.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, Context.PortContext.PARTITION_PARALLEL, true);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);

    merger = dag.addOperator("FileMerger", createFileMerger());
    dag.addStream("MergeTrigger", synchronizer.trigger, merger.input);

    //Setting operator properties
    merger.setFilePath(outputDirectoryPath);
    merger.setOverwriteOnConflict(overwriteOnConflict);

    //Binding proxy ports
    filesMetadataInput.set(synchronizer.filesMetadataInput);
    blocksMetadataInput.set(blockWriter.blockMetadataInput);
    blockData.set(blockWriter.input);
  }

  public String getOutputDirectoryPath()
  {
    return outputDirectoryPath;
  }

  public void setOutputDirectoryPath(String outputDirectoryPath)
  {
    this.outputDirectoryPath = outputDirectoryPath;
  }

  public boolean isOverwriteOnConflict()
  {
    return overwriteOnConflict;
  }

  public void setOverwriteOnConflict(boolean overwriteOnConflict)
  {
    this.overwriteOnConflict = overwriteOnConflict;
  }

}
