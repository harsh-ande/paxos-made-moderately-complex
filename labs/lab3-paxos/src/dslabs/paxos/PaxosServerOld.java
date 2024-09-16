package dslabs.paxos;

import static dslabs.paxos.LeaderP2ATimer.LEADER_P2A_RETRY_MILLIS;

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

// Final submission : 6 sometimes executes

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServerOld extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  // Your code here...
  private AMOApplication amoApplication;

  // leader
  public Ballot lBallot;
  public boolean lActive;
  //  public Map<Integer, AMOCommand> lProposals; // store in which slots what commands were
  // proposed
  // TODO this tracking should be done at a command level and sendP2A() function should set it to 0
  public Map<Integer, Integer> lP2BsReceivedAtSlot;

  // replica
  public Map<Integer, LogEntry> rLog;
  public int rSlotIn, rSlotOut;

  // acceptor
  public Ballot aBallot;
  public Set<PValue> aAcceptedPValues;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServerOld(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;
    this.amoApplication = new AMOApplication(app);

    // Your code here...
    lBallot = new Ballot(getServerIndex(), address()); // server1 will have ballot 1 initially
    lActive = false;
    lP2BsReceivedAtSlot = new HashMap<>();

    rLog = new HashMap<>();
    rSlotIn = 0;
    rSlotOut = 0;

    aBallot = null;
    aAcceptedPValues = new HashSet<>();
  }

  @Override
  public void init() {
    // Your code here...

    // TODO Hardcoded leader to server 1 and acceptor ballots, need to remove this later.
    if (Objects.equals(address().toString(), "server3")) {
      lActive = true;
    }
    aBallot = new Ballot(3, servers[2]);
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
    System.out.printf("%s\n", address());
    //    for(int i=0;i<logSlotNum;i++){
    //      if(rLog.containsKey(i)){
    //        System.out.printf("slot %d %s\n",i,rLog.get(i).paxosLogSlotStatus);
    //      }
    //    }
    return rLog.containsKey(logSlotNum - 1)
        ? rLog.get(logSlotNum - 1).paxosLogSlotStatus
        : PaxosLogSlotStatus.EMPTY;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServerOld#status}.
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
    return rLog.containsKey(logSlotNum - 1) ? rLog.get(logSlotNum - 1).amoCommand.command : null;
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
    return rSlotOut + 1;
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
    return rSlotIn;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    System.out.printf(
        "Paxos request from %s with seq %d rcvd on %s, curr slot in %d\n",
        sender, m.getCommand().sequenceNumber, address(), rSlotIn);

    // rSlotIn should be empty ideally always
    if (rLog.containsKey(rSlotIn)) {
      //      for(int i=rSlotOut;i<rSlotIn;i++){
      //        if(rLog.get(i)!=null && rLog.get(i).amoCommand!=null &&
      // rLog.get(i).amoCommand.sequenceNumber==m.getCommand().sequenceNumber){
      //          System.out.printf("Entered here ** %d\n",i);
      //          AMOResult result = amoApplication.execute(m.getCommand());
      //          send(new PaxosReply(result), sender);
      //        }
      //      }
      System.out.printf(
          "SEVERE: rLog on %s already contains entry for rSlotIn %d\n", address(), rSlotIn);
      rSlotIn++;
      return;
    }

    // Add to replicas' log and send propose to all servers
    rLog.put(rSlotIn, new LogEntry(PaxosLogSlotStatus.CHOSEN, m.getCommand()));
    for (int i = 0; i < servers.length; i++) {
      send(new ProposeMessage(address(), rSlotIn, m.getCommand()), servers[i]);
    }
    rSlotIn++;
  }

  private void handleProposeMessage(ProposeMessage proposeMessage, Address sender) {
    // TODO Re-look at this if condition. What to do when a duplicate propose command is rcvd.
    if (!rLog.containsKey(proposeMessage.slotNumber)) {
      if (lActive) {
        sendP2A(proposeMessage.slotNumber, proposeMessage.command);
      }
    }
  }

  // TODO sendP1A. Should reset count of p2bs rcvd, should set a timer for p1b reception

  private void sendP2A(int slotNum, AMOCommand amoCommand) {
    System.out.printf(
        "DL %s sending P2A for seq %d slot %d\n", address(), amoCommand.sequenceNumber, slotNum);
    lP2BsReceivedAtSlot.put(slotNum, 1); // Assume it got a vote from itself
    for (Address address : servers) {
      if (!Objects.equals(address, address())) {
        send(new P2AMessage(lBallot, slotNum, amoCommand), address);
      }
    }
    set(new LeaderP2ATimer(slotNum, amoCommand), LEADER_P2A_RETRY_MILLIS);
  }

  private void onLeaderP2ATimer(LeaderP2ATimer t) {
    System.out.printf(
        "leader p2a timer timed out on %s for slot %d, total p2b heard yet %d <=%b\n",
        address(),
        t.slotNum,
        lP2BsReceivedAtSlot.get(t.slotNum),
        lP2BsReceivedAtSlot.get(t.slotNum) <= servers.length / 2);
    // TODO Add additional check to see if leader heartbeat alive
    if (lActive && lP2BsReceivedAtSlot.get(t.slotNum) <= servers.length / 2) {
      System.out.printf("Resending p2a for slot %d after timeout\n", t.slotNum);
      sendP2A(t.slotNum, t.amoCommand);
    }
  }

  private void handleP2AMessage(P2AMessage p2AMessage, Address sender) {
    if (p2AMessage.ballot.equals(aBallot)) {
      aAcceptedPValues.add(new PValue(aBallot, p2AMessage.slotNumber, p2AMessage.command));
    }
    send(new P2BMessage(aBallot, p2AMessage.slotNumber, p2AMessage.command), sender);
  }

  private void handleP2BMessage(P2BMessage p2BMessage, Address sender) {
    if (p2BMessage.ballot.equals(lBallot)) {
      lP2BsReceivedAtSlot.put(
          p2BMessage.slotNumber, lP2BsReceivedAtSlot.get(p2BMessage.slotNumber) + 1);
      System.out.printf(
          "Rcvd a p2B for seq %d slot %d on %s as DL from %s\n",
          p2BMessage.command.sequenceNumber, p2BMessage.slotNumber, address(), sender);
      if (lP2BsReceivedAtSlot.get(p2BMessage.slotNumber) > servers.length / 2) {
        System.out.printf(
            "Got P2B majority for seq %d slot %d on %s\n",
            p2BMessage.command.sequenceNumber, p2BMessage.slotNumber, address());
        for (Address address : servers) {
          send(new DecisionMessage(p2BMessage.slotNumber, p2BMessage.command, rLog), address);
        }
      }
    } else {
      if (p2BMessage.ballot.ballotNumber > lBallot.ballotNumber) {
        System.out.printf(
            "%s Preempted as p2b msg ballot %d was higher than lballot %d\n",
            address(), p2BMessage.ballot.ballotNumber, lBallot.ballotNumber);
        lActive = false;
        lBallot = p2BMessage.ballot;
        // TODO Start leader <-> DL heartbeat check here
      }
    }
  }

  private void handleDecisionMessage(DecisionMessage decisionMessage, Address sender) {
    // add to the decisions rcvd by marking that slot as accepted
    if (!rLog.containsKey(decisionMessage.slotNumber)) {
      System.out.printf(
          "Putting unseen dec msg into rlog on %s at slot %d seq %d\n",
          address(), decisionMessage.slotNumber, decisionMessage.command.sequenceNumber);
      rLog.put(
          decisionMessage.slotNumber,
          new LogEntry(PaxosLogSlotStatus.ACCEPTED, decisionMessage.command));
      rSlotIn++;
    }
    System.out.printf(
        "Decision msg arrived on %s for seq %d slot num %d \n",
        address(), decisionMessage.command.sequenceNumber, decisionMessage.slotNumber);
    LogEntry currentLogEntry = rLog.get(decisionMessage.slotNumber);
    // Commenting below as can be out of order
    //    if(Objects.equals(currentLogEntry.amoCommand, decisionMessage.command) &&
    if (rLog.get(decisionMessage.slotNumber).paxosLogSlotStatus != PaxosLogSlotStatus.CLEARED) {
      System.out.printf(
          "Decision msg accepted on %s for seq %d slot num %d rslout out %d\n",
          address(), decisionMessage.command.sequenceNumber, decisionMessage.slotNumber, rSlotOut);
      currentLogEntry.paxosLogSlotStatus = PaxosLogSlotStatus.ACCEPTED;
      rLog.put(decisionMessage.slotNumber, currentLogEntry);
      //      System.out.printf("rlog keys on %s - %s\n", address(), rLog.keySet());
      //      System.out.printf("Before entering loop on %s contains key %b log status %s\n",
      // address(), rLog.containsKey(rSlotOut), rLog.get(rSlotOut).paxosLogSlotStatus);

      // Adding hack below to fix gaps of log entries in leader
      if (!rLog.containsKey(rSlotOut)) {
        rLog.put(rSlotOut, new LogEntry(PaxosLogSlotStatus.ACCEPTED, decisionMessage.command));
      }
      while (rLog.containsKey(rSlotOut)
          && rLog.get(rSlotOut).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED) {
        // TODO For now considering that we will get all acceptances as proposed, no need to swap
        // for now
        // TODO When swapping propose the swapped out request
        System.out.printf(
            "Entered inside while loop rslotout %d dec slot %d\n",
            rSlotOut, decisionMessage.slotNumber);
        AMOResult result = amoApplication.execute(rLog.get(rSlotOut).amoCommand);
        send(new PaxosReply(result), rLog.get(rSlotOut).amoCommand.address);
        // TODO Move this to DL triggered GC
        // TODO After a network partition is removed, servers should talk and garbage collect - test
        // 11
        LogEntry slotOutLogEntry = rLog.get(rSlotOut);
        slotOutLogEntry.paxosLogSlotStatus = PaxosLogSlotStatus.CLEARED;
        slotOutLogEntry.amoCommand =
            new AMOCommand(
                slotOutLogEntry.amoCommand.sequenceNumber,
                slotOutLogEntry.amoCommand.address,
                null);
        rLog.put(rSlotOut, slotOutLogEntry);
        //        System.out.printf("For rslot out %d on %s, log status after put is %s\n",
        // rSlotOut, address(), rLog.get(rSlotOut).paxosLogSlotStatus);
        rSlotOut++;
      }
    }
  }

  private int getServerIndex() {
    for (int i = 0; i < servers.length; i++) {
      if (Objects.equals(servers[i].toString(), address().toString())) {
        return i + 1;
      }
    }
    return 0;
  }

  private Address getAddressFromString(String strAddress) {
    for (Address address : servers) {
      System.out.printf("%s %s\n", address.toString(), strAddress);
      if (address.toString().equals(strAddress)) {
        return address;
      }
    }
    System.out.printf("returning null ");
    return null;
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
