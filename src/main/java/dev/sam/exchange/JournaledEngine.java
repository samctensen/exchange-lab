package dev.sam.exchange;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.persistence.CommandJournal;

public class JournaledEngine {

  private final MatchingEngine engine;
  private final CommandJournal journal;

  public JournaledEngine(MatchingEngine engine, CommandJournal journal) {
    this.engine = engine;
    this.journal = journal;
  }

  public CommandResult process(EngineCommand command) throws IOException {
    this.engine.validate(command);
    this.journal.append(command);
    return this.engine.process(command);
  }

  public static JournaledEngine recover(Path path) throws IOException {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    CommandJournal journal = new CommandJournal(path);
    List<EngineCommand> commands = journal.readAll();
    for (EngineCommand command : commands) {
      engine.process(command);
    }
    return new JournaledEngine(engine, journal);
  }
}
