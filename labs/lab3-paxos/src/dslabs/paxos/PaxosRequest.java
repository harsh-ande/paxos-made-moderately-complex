package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
  // Your code here...
  public AMOCommand command;

  public PaxosRequest(AMOCommand command) {
    this.command = command;
  }

  public AMOCommand getCommand() {
    return this.command;
  }
}
