package org.iota.rockstograph;

import com.beust.jcommander.Parameter;

/**
 * @author Andreas C. Osowski
 */
public class Config {
  @Parameter(names="-rocksDb", required = true, description = "Path to RocksDB database")
  public String rocksDbPath;

  @Parameter(names="-janusConfig" ,required=true, description = "Path to JanusGraph config file")
  public String janusConfig;
}
