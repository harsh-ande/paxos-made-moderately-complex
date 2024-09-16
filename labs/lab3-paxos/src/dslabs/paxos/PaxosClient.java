package dslabs.paxos;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;

  // Your code here...
  public AMOCommand currentCommand;
  public int currentSequenceNum = 0;
  public Result currentResult;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command operation) {
    // Your code here...
    currentSequenceNum++;
    currentCommand = new AMOCommand(currentSequenceNum, address(), operation);
    currentResult = null;
    System.out.printf(
        "Sending current command seq %d from client %s\n",
        currentCommand.sequenceNumber, address());
    for (int i = 0; i < servers.length; i++) {
      send(new PaxosRequest(currentCommand), servers[i]);
    }
    set(new dslabs.paxos.ClientTimer(currentCommand), CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return currentResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (currentResult == null) {
      this.wait();
    }
    return currentResult;
  }

  /* -----------------------------------------------------------------------------------------------
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    // Your code here...
    if (m.getResult().sequenceNumber == currentSequenceNum) {
      System.out.printf(
          "Client %s got reply %s for %d from sender %s\n",
          address(), m, currentSequenceNum, sender);
      currentResult = m.getResult().result;
      currentCommand = null;
      this.notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (Objects.equals(currentCommand, t.getCommand()) && this.currentResult == null) {
      for (int i = 0; i < servers.length; i++) {
        send(new PaxosRequest(currentCommand), servers[i]);
      }
      set(t, CLIENT_RETRY_MILLIS);
    }
  }
}
