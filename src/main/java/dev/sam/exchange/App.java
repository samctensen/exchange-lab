package dev.sam.exchange;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.ReplayResult;
import dev.sam.exchange.engine.ReplayRunner;
import dev.sam.exchange.persistence.CommandJournal;

/**
 * Hello world!
 */
public class App {

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: java -cp target/classes dev.sam.exchange.App <journal-file>");
    }

    Path path = Path.of(args[0]);
    CommandJournal journal = new CommandJournal(path);

    List<EngineCommand> commands = journal.readAll();

    ReplayRunner runner = new ReplayRunner();
    ReplayResult results = runner.replay(commands);

    for (CommandResult result : results.results()) {
      System.out.println(result);
    }
    System.out.println(results.bookSnapshot());
  }
}
