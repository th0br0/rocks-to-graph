package org.iota.rockstograph;

import com.beust.jcommander.JCommander;

/**
 * @author Andreas C. Osowski
 */
public class Main {
  public static void main(String[] args) {
    Config config = new Config();
    JCommander.newBuilder().addObject(config).build().parse(args);

    RocksDBImporter importer = new RocksDBImporter(config);
    importer.run();
  }
}
