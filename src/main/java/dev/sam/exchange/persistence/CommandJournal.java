package dev.sam.exchange.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import dev.sam.exchange.engine.EngineCommand;

public class CommandJournal {

  private final Path path;
  private final CommandCodec codec;

  public CommandJournal(Path path) {
    this.path = path;
    this.codec = new CommandCodec();
  }

  public void append(EngineCommand command) throws IOException {
    var encoding = codec.encode(command) + "\n";
    Files.writeString(path, encoding, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  public List<EngineCommand> readAll() throws IOException {
    String contents = Files.readString(this.path);
    if (!contents.isEmpty() && !contents.endsWith("\n")) {
      throw new IOException("Journal ends with an incomplete command: " + this.path);
    }
    return contents.lines().map(codec::decode).toList();
  }
}
