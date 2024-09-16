package dslabs.paxos;

// Your code here...

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Message;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

class Ballot implements Serializable {
  public int ballotNumber;
  public Address dL;

  public Ballot(int ballotNumber, Address dL) {
    this.ballotNumber = ballotNumber;
    this.dL = dL;
  }

  public boolean equals(Ballot ballot) {
    return this.ballotNumber == ballot.ballotNumber
        && Objects.equals(this.dL.toString(), ballot.dL.toString());
  }
}

class LogEntry implements Serializable {
  // TODO Do we really need to store below Ballot as well in LogEntry?
  //  public Ballot ballot;
  public PaxosLogSlotStatus paxosLogSlotStatus;
  public AMOCommand amoCommand;
  public AMOResult amoResult;
  public boolean p2ASent;

  public LogEntry(PaxosLogSlotStatus paxosLogSlotStatus, AMOCommand command) {
    //    this.ballot = ballot;
    this.paxosLogSlotStatus = paxosLogSlotStatus;
    this.amoCommand = command;
    this.p2ASent = false;
  }

  public void setAmoResult(AMOResult result) {
    this.amoResult = result;
  }

  public void setAmoCommand(AMOCommand command) {
    this.amoCommand = command;
  }

  public void setP2ASent(boolean p2ASent) {
    this.p2ASent = p2ASent;
  }
}

class PValue implements Serializable {
  public Ballot ballot;
  public int slotNum;
  public AMOCommand amoCommand;

  public PValue(Ballot ballot, int slotNum, AMOCommand amoCommand) {
    this.ballot = ballot;
    this.slotNum = slotNum;
    this.amoCommand = amoCommand;
  }
}

class P2AMessage implements Message {
  public Ballot ballot;
  public int slotNumber;
  public AMOCommand command;

  public P2AMessage(Ballot ballot, int slotNumber, AMOCommand command) {
    this.ballot = ballot;
    this.slotNumber = slotNumber;
    this.command = command;
  }
}

class P2BMessage implements Message {
  public Ballot ballot;
  public int slotNumber;
  public AMOCommand command;

  public P2BMessage(Ballot ballot, int slotNumber, AMOCommand amoCommand) {
    this.ballot = ballot;
    this.slotNumber = slotNumber;
    this.command = amoCommand;
  }
}

class ProposeMessage implements Message {
  public Address proposerReplica;
  public int slotNumber;
  public AMOCommand command;

  public ProposeMessage(Address proposerReplica, int slotNumber, AMOCommand command) {
    this.proposerReplica = proposerReplica;
    this.slotNumber = slotNumber;
    this.command = command;
  }
}

class DecisionMessage implements Message {
  public int slotNumber;
  public AMOCommand command;
  public Map<Integer, LogEntry> leaderLog;

  public DecisionMessage(int slotNumber, AMOCommand command, Map<Integer, LogEntry> leaderLog) {
    this.slotNumber = slotNumber;
    this.command = command;
    this.leaderLog = leaderLog;
  }
}

class HeartbeatRequest implements Message {
  public Address senderLeader;
  public int senderLastContExecuted;

  public HeartbeatRequest(Address senderLeader, int senderLastContExecuted) {
    this.senderLeader = senderLeader;
    this.senderLastContExecuted = senderLastContExecuted;
  }
}

class HeartbeatReply implements Message {
  public int garbageCollectTillSlot;
  public Map<Integer, LogEntry> leaderLog;

  public HeartbeatReply(int garbageCollectTillSlot, Map<Integer, LogEntry> leaderLog) {
    this.garbageCollectTillSlot = garbageCollectTillSlot;
    this.leaderLog = leaderLog;
  }
}
