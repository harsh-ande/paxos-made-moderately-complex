package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
  // Your code here...
  public int sequenceNumber;
  public Address address;
  public final Command command;

  public AMOCommand(int sequenceNumber, Address address, Command command) {
    this.sequenceNumber = sequenceNumber;
    this.address = address;
    this.command = command;
  }

  public Address getAddress() {
    return address;
  }

  @Override
  public boolean readOnly() {
    return this.command.readOnly();
  }
}
