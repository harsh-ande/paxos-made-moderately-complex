package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  public AMOCommand command;

  public ClientTimer(AMOCommand command) {
    this.command = command;
  }

  public AMOCommand getCommand() {
    return this.command;
  }
}

// Your code here...

final class LeaderP2ATimer implements Timer {
  static final int LEADER_P2A_RETRY_MILLIS = 20;
  public int slotNum;
  public AMOCommand amoCommand;

  // Your code here...

  public LeaderP2ATimer(int slotNum, AMOCommand amoCommand) {
    this.slotNum = slotNum;
    this.amoCommand = amoCommand;
  }
}

final class ReplicaLogSlotTimer implements Timer {
  static final int REPLICA_LOG_SLOT_RETRY_MILLIS = 100;
  public int slotNum;
  public AMOCommand amoCommand;

  // Your code here...

  public ReplicaLogSlotTimer(int slotNum, AMOCommand amoCommand) {
    this.slotNum = slotNum;
    this.amoCommand = amoCommand;
  }
}

final class LeaderHeartbeatTimer implements Timer {
  static final int LEADER_HEARTBEAT_MILLIS = 100;
  public Ballot ballot;

  // Your code here...

  public LeaderHeartbeatTimer(Ballot ballot) {
    this.ballot = ballot;
  }
}

final class ReplicaHoleFillTimer implements Timer {
  static final int REPLICA_HOLEFILL_MILLIS = 100;
  public int previousHoleSlot;
  public int previousSlotOut;

  // Your code here...

  public ReplicaHoleFillTimer(int previousHoleSlot, int previousSlotOut) {
    this.previousHoleSlot = previousHoleSlot;
    this.previousSlotOut = previousSlotOut;
  }
}

final class ProposeTimer implements Timer {
  static final int PROPOSE_MILLIS = 20;
  public int proposeSlot;

  // Your code here...

  public ProposeTimer(int proposeSlot) {
    this.proposeSlot = proposeSlot;
  }
}
