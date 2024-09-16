package dslabs.primarybackup;

import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.ReplicationTimer.REPLICATION_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import java.util.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
  private final Address viewServer;
  private AMOApplication amoApplication;
  // Your code here...
  private int serverRole; // 0 -> Primary, 1 -> Backup, 2 -> Extra server
  private Ping mostRecentPing;
  private View mostRecentView;
  private Set<String> clientReplicationReplySequences;
  private Map<String, Request> outstandingPrimaryRequests;
  private Map<String, Address> sequenceClientMap;
  private boolean replicationCommandOnGoing;
  private ReplicationTimer activeReplicationTimer;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;
    // Your code here...
    this.amoApplication = new AMOApplication(app);
    serverRole = -1;
    mostRecentPing = null;
    mostRecentView = null;
    clientReplicationReplySequences = new HashSet<>();
    outstandingPrimaryRequests = new HashMap<>();
    sequenceClientMap = new HashMap<>();
    replicationCommandOnGoing = false;
    activeReplicationTimer = null;
  }

  @Override
  public void init() {
    // Your code here...
    set(new PingTimer(), PING_MILLIS);
    mostRecentPing = new Ping(STARTUP_VIEWNUM);
    send(mostRecentPing, viewServer);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleRequest(Request m, Address sender) {
    // Your code here...
    System.out.printf(
        "Handling request on server %s %s from sender %s \n", address(), m.command, sender);
    if (!Objects.equals(m.view, mostRecentView)) {
      return;
    }
    if (serverRole == 0) {
      //      System.out.printf("Role primary on %s\n", address());
      if (mostRecentView.getBackup() != null
          && !replicationCommandOnGoing
          && activeReplicationTimer == null) {
        ReplicationRequest replicationRequest =
            new ReplicationRequest(m.command, sender, mostRecentView);
        send(replicationRequest, mostRecentView.getBackup());
        activeReplicationTimer = new ReplicationTimer(replicationRequest);
        set(activeReplicationTimer, REPLICATION_MILLIS);
        replicationCommandOnGoing = true;
        System.out.printf(" ######### ongoing command set\n");
        outstandingPrimaryRequests.put(sender.toString() + m.command.sequenceNumber, m);
        sequenceClientMap.put(sender.toString() + m.command.sequenceNumber, sender);
      } else if (!replicationCommandOnGoing) {
        AMOResult result = amoApplication.execute(m.command);
        send(new Reply(result, mostRecentView), sender);
      }
    } else if (serverRole == 1) {
      System.out.printf(
          "<handRequest>Backup executing seq %d on server %s\n",
          m.command.sequenceNumber, address());
      //      System.out.printf("Backup's appl state %s", amoApplication.getState());
      AMOResult result = amoApplication.execute(m.command);
    }
  }

  private void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    //    System.out.printf("handling view reply %s in %s\n", m.getView(), address());
    if (mostRecentView != null && m.getView().getViewNum() < mostRecentView.getViewNum()) {
      System.out.printf(
          "^^^^^^^^^^ In server %s curr view %s and got view %s",
          address(), mostRecentView, m.getView());
      return;
    }
    View prevMostRecentView = mostRecentView;
    mostRecentView = m.getView();
    if (Objects.equals(m.getView().getPrimary(), address())) {
      serverRole = 0;
      if (prevMostRecentView != null
          && Objects.equals(prevMostRecentView.getPrimary(), address())
          && prevMostRecentView.getBackup() == null
          && m.getView().getBackup() != null) {
        System.out.printf("From primary, sending appl state - %s\n", amoApplication.getState());
        ApplicationStateRequest applicationStateRequest =
            new ApplicationStateRequest(amoApplication.getState(), mostRecentView);
        send(applicationStateRequest, m.getView().getBackup());
      }
    } else if (Objects.equals(m.getView().getBackup(), address())) {
      serverRole = 1;
    } else {
      serverRole = 2;
    }
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    // Your code here...
    //    System.out.printf("Ping timer triggered for %s. Most recent view %d\n", address(),
    // mostRecentView.getViewNum());
    mostRecentPing =
        new Ping(mostRecentView != null ? mostRecentView.getViewNum() : STARTUP_VIEWNUM);
    if (mostRecentView != null
        && !Objects.equals(mostRecentView.getPrimary(), address())
        && !Objects.equals(mostRecentView.getBackup(), address())) {
      mostRecentPing = new Ping(STARTUP_VIEWNUM);
    }
    send(mostRecentPing, viewServer);
    set(t, PING_MILLIS);
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  public void onReplicationTimer(ReplicationTimer t) {
    if (!clientReplicationReplySequences.contains(
        t.replicationRequest.client.toString() + t.replicationRequest.command.sequenceNumber)) {
      t.replicationRequest.view = mostRecentView;
      send(t.replicationRequest, mostRecentView.getBackup());
      set(t, REPLICATION_MILLIS);
      return;
    }
    System.out.printf(" ######### ongoing command unset in repl timer\n");
    replicationCommandOnGoing = false;
  }

  public void handleApplicationStateRequest(
      ApplicationStateRequest applicationStateRequest, Address sender) {
    System.out.printf("Got application state transfer request on %s\n", address());
    System.out.printf("In backup, ingesting state %s\n", applicationStateRequest.applicationState);
    this.amoApplication.ingestState(applicationStateRequest.applicationState);
  }

  public void handleReplicationRequest(ReplicationRequest replicationRequest, Address sender) {
    if (!Objects.equals(replicationRequest.view, mostRecentView)) {
      return;
    }
    System.out.printf(
        "Backup executing seq %d on server %s\n",
        replicationRequest.command.sequenceNumber, address());
    AMOResult amoResult = this.amoApplication.execute(replicationRequest.command);
    send(new ReplicationReply(amoResult, replicationRequest.client, mostRecentView), sender);
  }

  public void handleReplicationReply(ReplicationReply replicationReply, Address sender) {
    if (!Objects.equals(replicationReply.view, mostRecentView)) {
      return;
    }
    if (outstandingPrimaryRequests.containsKey(
        replicationReply.client.toString() + replicationReply.result.sequenceNumber)) {
      System.out.printf(
          "Primary executing seq %d on server %s\n",
          replicationReply.result.sequenceNumber, address());
      clientReplicationReplySequences.add(
          replicationReply.client.toString() + replicationReply.result.sequenceNumber);
      AMOResult result =
          amoApplication.execute(
              outstandingPrimaryRequests.get(
                      replicationReply.client.toString() + replicationReply.result.sequenceNumber)
                  .command);
      send(
          new Reply(result, mostRecentView),
          sequenceClientMap.get(
              replicationReply.client.toString() + replicationReply.result.sequenceNumber));
      outstandingPrimaryRequests.remove(
          replicationReply.client.toString() + replicationReply.result.sequenceNumber);
      replicationCommandOnGoing = false;
      activeReplicationTimer = null;
      System.out.printf(" ######### ongoing command unset in handle repl reply\n");
    }
  }
}
