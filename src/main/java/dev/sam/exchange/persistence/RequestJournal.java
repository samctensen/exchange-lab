package dev.sam.exchange.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandRequestCodec;

public class RequestJournal {
  private final Path path;
  private final CommandRequestCodec codec;

  public RequestJournal(Path path) {
    this.path = path;
    this.codec = new CommandRequestCodec();
  }

  public void append(CommandRequest request) throws IOException {
    var encoding = codec.encode(request) + "\n";
    Files.writeString(path, encoding, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  public List<CommandRequest> readAll() throws IOException {
    String contents = Files.readString(this.path);
    if (!contents.isEmpty() && !contents.endsWith("\n")) {
      throw new IOException("Journal ends with an incomplete command: " + this.path);
    }
    return contents.lines().map(codec::decode).toList();
  }
}
