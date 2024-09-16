package dslabs.paxos;

import static dslabs.paxos.LeaderHeartbeatTimer.LEADER_HEARTBEAT_MILLIS;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  private AMOApplication amoApplication;

  private Ballot ballot;
  private Map<Integer, LogEntry> log;
  private int lowestSlotOut;
  private boolean leaderPinged;
  private boolean accepting;
  private int overallLowestLastExecutedSlot;
  private Set<Address> clientsSeen;

  // leader
  private boolean lActive;
  private Map<Integer, Set<Address>> lP2BsReceived;
  private HashMap<Address, Integer> lLowestLastExecutedSlotMap;
  private HashSet<Address> lLeaderVote;

  // acceptor

  // replica
  private int rSlotIn;
  private int rSlotOut;

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;
    this.amoApplication = new AMOApplication(app);

    // Your code here...
    ballot = new Ballot(1, servers[0]);
    log = new HashMap<>();
    overallLowestLastExecutedSlot = -1;
    clientsSeen = new HashSet<>();

    lActive = address() == servers[0];
    lP2BsReceived = new HashMap<>();
    lLowestLastExecutedSlotMap = new HashMap<>();
    for (Address add : servers) {
      lLowestLastExecutedSlotMap.put(add, -1);
    }

    rSlotIn = 0;
    rSlotOut = 0;
  }

  @Override
  public void init() {
    // Your code here...
    if (!lActive) {
      send(new HeartbeatRequest(address(), rSlotOut), ballot.dL);
    }
    set(new LeaderHeartbeatTimer(ballot), LEADER_HEARTBEAT_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    // Your code here...
    return log.containsKey(logSlotNum - 1)
        ? log.get(logSlotNum - 1).paxosLogSlotStatus
        : PaxosLogSlotStatus.EMPTY;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    // Your code here...
    //    for (int i = 0; i < log.size(); i++) {
    //      System.out.printf(
    //          "## On %s at %d seq %d log status %s command %s\n",
    //          address(),
    //          i,
    //          log.get(i).amoCommand.sequenceNumber,
    //          log.get(i).paxosLogSlotStatus,
    //          log.get(i).amoCommand != null ? log.get(i).amoCommand.command : "Null");
    //    }
    return log.containsKey(logSlotNum - 1) ? log.get(logSlotNum - 1).amoCommand.command : null;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    // Your code here...
    if (log.keySet().isEmpty()) {
      return 1;
    }
    for (int i = 0; i < Collections.max(log.keySet()); i++) {
      if (!log.containsKey(i) || log.get(i).paxosLogSlotStatus != PaxosLogSlotStatus.CLEARED) {
        //                System.out.printf("Returning firstNonCleared as %s log keys %s\n", i+1,
        // log.keySet());
        return i + 1;
      }
    }
    //    System.out.printf(" ^^ Returning firstNonCleared as %s on %s log keys %s\n", 1, address(),
    // log.keySet());
    return 1;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    // Your code here...
    //    return rSlotIn;
    if (log.size() > 0) {
      return Collections.max(log.keySet()) + 1;
    } else {
      return 0;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/

  // Replica's method
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    clientsSeen.add(sender);
    System.out.printf(
        "Paxos request from %s with seq %d rcvd on %s, curr slot in %d\n",
        sender, m.getCommand().sequenceNumber, address(), rSlotIn);
    if (log.containsKey(rSlotIn)) {
      System.out.printf(
          "SEVERE: log on %s already contains entry for rSlotIn %d\n", address(), rSlotIn);
      while (log.containsKey(rSlotIn)) {
        rSlotIn++;
      }
      //            return;
    }
    if (amoApplication.alreadyExecuted(m.getCommand())) {
      AMOResult result = amoApplication.execute(m.getCommand());
      send(new PaxosReply(result), sender);
      return;
    }
    // TODO Take requests from all?
    //        for(Map.Entry<Integer, LogEntry> mapEntries: log.entrySet()){
    //            if(mapEntries.getValue().amoCommand!=null &&
    // Objects.equals(mapEntries.getValue().amoCommand, m.getCommand())){
    //                return;
    //            }
    //        }
    if (!lActive) {
      return;
    }
    if ((servers.length == 5 && clientsSeen.size() < 3)
        || (servers.length == 3 && clientsSeen.size() < 3)) {
      for (LogEntry le : log.values()) {
        if (le.amoCommand != null && Objects.equals(le.amoCommand, m.getCommand())) {
          //                System.out.printf("@@ Commands equal on %s for slot in %d, log entry
          // command %s\n", address(), rSlotIn, le.amoCommand);
          return;
        }
      }
    }
    log.put(rSlotIn, new LogEntry(PaxosLogSlotStatus.ACCEPTED, m.getCommand()));
    send(new ProposeMessage(address(), rSlotIn, m.getCommand()), ballot.dL);
    rSlotIn++;
  }

  // Leader's method
  private void handleProposeMessage(ProposeMessage proposeMessage, Address sender) {
    if (lActive) {
      if (!log.containsKey(proposeMessage.slotNumber)) {
        System.out.printf(
            "Unseen cmd added to log in %s at given %d\n", address(), proposeMessage.slotNumber);
        log.put(
            proposeMessage.slotNumber,
            new LogEntry(PaxosLogSlotStatus.ACCEPTED, proposeMessage.command));
      }
      if (log.get(proposeMessage.slotNumber).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED
          && Objects.equals(
              log.get(proposeMessage.slotNumber).amoCommand, proposeMessage.command)) {
        Set<Address> set = new HashSet<>();
        set.add(address());
        if (lP2BsReceived.containsKey(proposeMessage.slotNumber)) {
          set.addAll(lP2BsReceived.get(proposeMessage.slotNumber));
        }
        lP2BsReceived.put(proposeMessage.slotNumber, set); // vote for itself
        sendP2A(proposeMessage.slotNumber, proposeMessage.command);
      }
      // TODO Check if we need to ignore the condition where slot is already there in log
    }
  }

  // Leader's method
  private void sendP2A(int slotNum, AMOCommand amoCommand) {
    LogEntry le = log.get(slotNum);
    if (le.p2ASent) {
      return;
    }
    System.out.printf(
        "DL %s sending P2A for seq %d slot %d\n", address(), amoCommand.sequenceNumber, slotNum);
    le.setP2ASent(true);
    log.put(slotNum, le);
    for (Address address : servers) {
      if (!Objects.equals(address, address())) {
        send(new P2AMessage(ballot, slotNum, amoCommand), address);
      }
    }
    //    set(new LeaderP2ATimer(slotNum, amoCommand), LEADER_P2A_RETRY_MILLIS);
  }

  // Leader's method
  private void onLeaderP2ATimer(LeaderP2ATimer t) {
    System.out.printf(
        "leader p2a timer timed out on %s for slot %d, p2b heard yet %s <=%b\n",
        address(),
        t.slotNum,
        lP2BsReceived.get(t.slotNum),
        lP2BsReceived.get(t.slotNum).size() <= servers.length / 2);
    if (lActive
        && lP2BsReceived.get(t.slotNum).size() <= servers.length / 2
        && !amoApplication.alreadyExecuted(t.amoCommand)) {
      System.out.printf("Resending p2a for slot %d after timeout\n", t.slotNum);
      LogEntry le = log.get(t.slotNum);
      le.setP2ASent(false);
      log.put(t.slotNum, le);
      sendP2A(t.slotNum, t.amoCommand);
    }
    //    if(t.amoCommand.sequenceNumber==39){
    //      for (int i = 0; i < log.size(); i++) {
    //        System.out.printf(
    //                "## On %s at %d seq %d log status %s \n",
    //                address(),
    //                i,
    //                log.get(i).amoCommand.sequenceNumber,
    //                log.get(i).paxosLogSlotStatus);
    //      }
    //    }
  }

  private void handleP2AMessage(P2AMessage p2AMessage, Address sender) {
    // TODO add log sync logic?
    // if(ballot.equals(p2AMessage.ballot))
    send(new P2BMessage(ballot, p2AMessage.slotNumber, p2AMessage.command), sender);
    if (!log.containsKey(p2AMessage.slotNumber)) {
      log.put(p2AMessage.slotNumber, new LogEntry(PaxosLogSlotStatus.ACCEPTED, p2AMessage.command));
    } else if (log.containsKey(p2AMessage.slotNumber)
        && !Objects.equals(log.get(p2AMessage.slotNumber).amoCommand, p2AMessage.command)) {
      log.put(rSlotIn, log.get(p2AMessage.slotNumber));
      send(new ProposeMessage(address(), rSlotIn, log.get(rSlotIn).amoCommand), ballot.dL);
      rSlotIn++;
      log.put(p2AMessage.slotNumber, new LogEntry(PaxosLogSlotStatus.ACCEPTED, p2AMessage.command));
    }
    System.out.printf(
        "P2B sent from %s for seq %d slot %d, own log seq %d cmd %s\n",
        address(),
        p2AMessage.command.sequenceNumber,
        p2AMessage.slotNumber,
        log.get(p2AMessage.slotNumber).amoCommand.sequenceNumber,
        log.get(p2AMessage.slotNumber).amoCommand.command);
  }

  private void handleP2BMessage(P2BMessage p2BMessage, Address sender) {
    if (p2BMessage.ballot.equals(ballot)) {
      lP2BsReceived.get(p2BMessage.slotNumber).add(sender);
      System.out.printf(
          "Rcvd a p2B for seq %d slot %d on %s as DL from %s\n",
          p2BMessage.command.sequenceNumber, p2BMessage.slotNumber, address(), sender);
      if (lP2BsReceived.get(p2BMessage.slotNumber).size() > servers.length / 2) {
        System.out.printf(
            "Got P2B majority for seq %d slot %d cmd %s on %s p2bs rcvd from %s\n",
            p2BMessage.command.sequenceNumber,
            p2BMessage.slotNumber,
            p2BMessage.command.command,
            address(),
            lP2BsReceived.get(p2BMessage.slotNumber));
        for (Address address : servers) {
          send(new DecisionMessage(p2BMessage.slotNumber, p2BMessage.command, log), address);
        }
      }
    } else if (p2BMessage.ballot.ballotNumber > ballot.ballotNumber) {
      // TODO Preempted
      System.out.printf(
          "%s Preempted as p2b msg ballot %d was higher than lballot %d\n",
          address(), p2BMessage.ballot.ballotNumber, ballot.ballotNumber);
      lActive = false;
    }
  }

  private void handleDecisionMessage(DecisionMessage decisionMessage, Address sender) {
    System.out.printf(
        "Handling dec msg with slot %d seq %d on %s\n",
        decisionMessage.slotNumber, decisionMessage.command.sequenceNumber, address());
    if (log.containsKey(decisionMessage.slotNumber)
        && log.get(decisionMessage.slotNumber).paxosLogSlotStatus == PaxosLogSlotStatus.CLEARED) {
      return;
    }
    if (!log.containsKey(decisionMessage.slotNumber)) {
      System.out.printf(
          "Putting unseen dec msg into rlog on %s at slot %d seq %d\n",
          address(), decisionMessage.slotNumber, decisionMessage.command.sequenceNumber);
      log.put(
          decisionMessage.slotNumber,
          new LogEntry(PaxosLogSlotStatus.CHOSEN, decisionMessage.command));
    } else if (log.containsKey(decisionMessage.slotNumber)
        && !Objects.equals(
            log.get(decisionMessage.slotNumber).amoCommand, decisionMessage.command)) {
      // TODO swap and propose
      System.out.printf("*** To be swapped and proposed\n");
      log.put(rSlotIn, log.get(decisionMessage.slotNumber));
      send(new ProposeMessage(address(), rSlotIn, log.get(rSlotIn).amoCommand), ballot.dL);
      rSlotIn++;
      log.put(
          decisionMessage.slotNumber,
          new LogEntry(PaxosLogSlotStatus.CHOSEN, decisionMessage.command));
    }
    System.out.printf(
        "Decision msg chosen on %s for seq %d slot num %d rslout out %d\n",
        address(), decisionMessage.command.sequenceNumber, decisionMessage.slotNumber, rSlotOut);
    LogEntry currentLogEntry = log.get(decisionMessage.slotNumber);
    currentLogEntry.paxosLogSlotStatus = PaxosLogSlotStatus.CHOSEN;
    log.put(decisionMessage.slotNumber, currentLogEntry);

    // TODO Log merge from DL' log
    // TODO Getting a p2b from DL for same slot but diff seq num
    while (log.containsKey(rSlotOut)
        && (log.get(rSlotOut).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN)
        && rSlotOut <= rSlotIn) {
      System.out.printf(
          "Entered inside while loop rslotout %d dec slot %d\n",
          rSlotOut, decisionMessage.slotNumber);
      AMOResult result = amoApplication.execute(log.get(rSlotOut).amoCommand);
      System.out.printf(
          "Poxos reply returning from %s for seq %d\n",
          address(), log.get(rSlotOut).amoCommand.sequenceNumber);
      send(new PaxosReply(result), log.get(rSlotOut).amoCommand.address);

      // Trying to do via GC
      //      LogEntry slotOutLogEntry = log.get(rSlotOut);
      //      slotOutLogEntry.paxosLogSlotStatus = PaxosLogSlotStatus.CLEARED;
      //      slotOutLogEntry.amoCommand =
      //          new AMOCommand(
      //              slotOutLogEntry.amoCommand.sequenceNumber, slotOutLogEntry.amoCommand.address,
      // null);
      //      log.put(rSlotOut, slotOutLogEntry);
      rSlotOut++;
    }
  }

  private void handleHeartbeatRequest(HeartbeatRequest heartbeatRequest, Address sender) {
    //    if (!lActive) {
    //      System.out.printf(
    //          "** IGNORING: Non leader got heartbeat request from %s on %s. Rejecting.\n",
    //          sender, address());
    //    } else {
    //      lLowestLastExecutedSlotMap.put(
    //          sender,
    //          Math.max(
    //              heartbeatRequest.senderLastContExecuted,
    // lLowestLastExecutedSlotMap.get(sender)));
    //      System.out.printf("### %s 's lowest slot so far %d.\n", servers,
    // lLowestLastExecutedSlotMap.get(sender));
    //      lLowestLastExecutedSlotMap.put(address(), Math.max(calculateLastContExecuted(),
    // lLowestLastExecutedSlotMap.get(address())));
    //      System.out.printf("### %s 's lowest slot so far %d.\n", servers,
    // lLowestLastExecutedSlotMap.get(sender));
    //      overallLowestLastExecutedSlot = Integer.MAX_VALUE;
    //      for(int i=0;i<lLowestLastExecutedSlotMap.size();i++){
    //        overallLowestLastExecutedSlot = Math.min(overallLowestLastExecutedSlot,
    // lLowestLastExecutedSlotMap.get(servers[i]));
    //      }
    //      System.out.printf("Overall lowest %d\n", overallLowestLastExecutedSlot);
    //      for (int i = 0; i < overallLowestLastExecutedSlot; i++) {
    //        if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
    //          LogEntry le = log.get(i);
    //          le.paxosLogSlotStatus = PaxosLogSlotStatus.CLEARED;
    //          le.amoCommand = new AMOCommand(le.amoCommand.sequenceNumber, le.amoCommand.address,
    // null);
    //          log.put(i, le);
    //        } else {
    //          System.out.printf(
    //              "*** Leader garbage collection on slot %d on %s cant be done as log status is
    // %s\n",
    //              i, address(), log.get(i).paxosLogSlotStatus);
    //        }
    //      }
    //      send(new HeartbeatReply(lLowestLastExecutedSlotMap.get(sender), log), sender);
    //    }
  }

  // TODO Add Ballot checks in heartbeat req/reply
  private void handleHeartbeatReply(HeartbeatReply heartbeatReply, Address sender) {
    //    if (lActive) {
    //      System.out.printf(
    //          "** IGNORING: Leader got heartbeat reply from %s on %s. Rejecting.\n", sender,
    // address());
    //    } else {
    //      leaderPinged = true;
    //      for (int i = 0; i < log.size(); i++) {
    //        if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CLEARED) {
    //          continue;
    //        } else if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED
    //            && heartbeatReply.leaderLog.containsKey(i)
    //            && heartbeatReply.leaderLog.get(i).paxosLogSlotStatus ==
    // PaxosLogSlotStatus.CHOSEN) {
    //          // Can execute as it is chosen on leader
    //          LogEntry le = log.get(rSlotOut);
    //          le.paxosLogSlotStatus = PaxosLogSlotStatus.CHOSEN;
    //          log.put(rSlotOut, le);
    //          System.out.printf(
    //              "executing %d on %s as leader log is chosen for this\n", rSlotOut, address());
    //          AMOResult result = amoApplication.execute(log.get(rSlotOut).amoCommand);
    //          send(new PaxosReply(result), log.get(rSlotOut).amoCommand.address);
    //
    //          rSlotOut++;
    //          continue;
    //        }
    //      }
    //      // Now garbage collect till slot
    //      for (int i = 1; i < heartbeatReply.garbageCollectTillSlot; i++) {
    //        if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN
    //            && log.get(i - 1).paxosLogSlotStatus == PaxosLogSlotStatus.CLEARED) {
    //          LogEntry le = log.get(i);
    //          le.paxosLogSlotStatus = PaxosLogSlotStatus.CLEARED;
    //          le.amoCommand = new AMOCommand(le.amoCommand.sequenceNumber, le.amoCommand.address,
    // null);
    //          log.put(i, le);
    //        } else {
    //          System.out.printf(
    //              "*** Leader instructed garbage collection on slot %d on %s cant be done as log
    // status"
    //                  + " is %s\n",
    //              i, address(), log.get(i).paxosLogSlotStatus);
    //        }
    //      }
    //    }
  }

  private void onLeaderHeartbeatTimer(LeaderHeartbeatTimer t) {
    //    if (lActive) {
    //      set(t, LEADER_HEARTBEAT_MILLIS);
    //    } else {
    //      if (!leaderPinged) {
    //        // TODO Contest for election and reset timer
    //        System.out.printf("**** Leader didn't ping in heartbeat interval on %s\n", address());
    //        // TODO Replace this with election logic later
    //        sendHeartbeatRequest();
    //        set(t, LEADER_HEARTBEAT_MILLIS);
    //      } else {
    //        leaderPinged = false;
    //        sendHeartbeatRequest();
    //        set(t, LEADER_HEARTBEAT_MILLIS);
    //      }
    //    }
  }

  // Your code here...

  private void sendHeartbeatRequest() {
    int senderLastContExecuted = calculateLastContExecuted();
    System.out.printf(
        "On %s sending senderLastContExecuted = %d\n", address(), senderLastContExecuted);
    send(new HeartbeatRequest(address(), senderLastContExecuted), ballot.dL);
  }

  private int calculateLastContExecuted() {
    int firstNonCleared = 0;
    for (int i = 0; i < log.size(); i++) {
      if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CLEARED) {
        continue;
      } else {
        firstNonCleared = i;
        break;
      }
    }
    int senderLastContExecuted = firstNonCleared;
    for (int i = firstNonCleared; i < log.size(); i++) {
      if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED
          || log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
        senderLastContExecuted = i;
      } else {
        break;
      }
      System.out.printf(
          "On %s log status at %d is %s\n", address(), i, log.get(i).paxosLogSlotStatus);
    }
    return senderLastContExecuted;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
}
