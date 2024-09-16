package dslabs.primarybackup;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.primarybackup.ClientViewTimer.CLIENT_VIEW_RETRY_MILLIS;
import static java.lang.Thread.sleep;

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
class PBClient extends Node implements Client {
  private final Address viewServer;
  private Address primaryServer;
  public int currentSequenceNum = 0;
  public Result currentResult;
  public AMOCommand currentCommand;
  private View currentView;
  private Command queuedCommand;

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PBClient(Address address, Address viewServer) {
    super(address);
    this.viewServer = viewServer;
    queuedCommand = null;
  }

  @Override
  public synchronized void init() {
    // Your code here...
    primaryServer = null;
    System.out.printf("** Requesting view from viewserver %s in client\n", viewServer);
    send(new GetView(), viewServer);
    set(new ClientViewTimer(), CLIENT_VIEW_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    while (primaryServer == null) {
      if (queuedCommand != null) {
        System.out.printf("Queued command != null");
        return;
      }
      queuedCommand = command;
      return;
    }
    while (currentCommand != null) {
      try {
        System.out.printf("Trying to sleep in client %s\n", address());
        sleep(CLIENT_RETRY_MILLIS / 10);
      } catch (InterruptedException e) {
        System.out.printf("Caught exception while trying to sleep in client %s\n", address());
        throw new RuntimeException(e);
      }
    }
    //    System.out.printf("Sending command from client %s to address %s\n", command,
    // primaryServer);
    currentSequenceNum = currentSequenceNum + 1;
    currentResult = null;
    currentCommand = new AMOCommand(currentSequenceNum, address(), command);
    System.out.printf(
        "Sending current command = %s to address %s from client %s\n",
        currentCommand, primaryServer, address());
    send(new Request(currentCommand, currentView), primaryServer);
    set(new ClientTimer(currentCommand), CLIENT_RETRY_MILLIS);
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
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    System.out.printf("In client %s\n", address());
    if (m.getResult().sequenceNumber == currentSequenceNum) {
      System.out.printf(
          "Client %s got reply for %d from sender %s\n", address(), currentSequenceNum, sender);
      currentResult = m.getResult().result;
      currentCommand = null;
      this.notify();
    }
  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    System.out.printf(
        "handling view reply in client. Primary %s *******\n", m.getView().getPrimary());
    if (currentView != null && m.getView().getViewNum() < currentView.getViewNum()) {
      System.out.printf(
          "^^^^^^^^^^ In client curr view %s and got view %s", currentView, m.getView());
      return;
    }
    currentView = m.getView();
    primaryServer = currentView.getPrimary();
    if (queuedCommand != null) {
      Command queuedCommandCopy = queuedCommand;
      queuedCommand = null;
      sendCommand(queuedCommandCopy);
    }
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (Objects.equals(currentCommand, t.getCommand()) && this.currentResult == null) {
      System.out.printf("** Requesting view from viewserver %s in client\n", viewServer);
      send(new GetView(), viewServer);
      set(new ClientViewTimer(), CLIENT_VIEW_RETRY_MILLIS);
      send(new Request(currentCommand, currentView), primaryServer);
      set(t, CLIENT_RETRY_MILLIS);
    }
  }

  private synchronized void onClientViewTimer(ClientViewTimer t) {
    if (primaryServer == null) {
      send(new GetView(), viewServer);
      set(t, CLIENT_VIEW_RETRY_MILLIS);
    }
  }
}
