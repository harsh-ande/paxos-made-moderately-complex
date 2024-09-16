package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
  static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
  static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private AMOCommand command;

  public ClientTimer(AMOCommand amoCommand) {
    command = amoCommand;
  }

  public AMOCommand getCommand() {
    return command;
  }
}

// Your code here...

final class ClientViewTimer implements Timer {
  static final int CLIENT_VIEW_RETRY_MILLIS = 50;

  // Your code here...

  public ClientViewTimer() {}
}

@Data
final class ReplicationTimer implements Timer {
  public ReplicationRequest replicationRequest;
  static final int REPLICATION_MILLIS = 50;

  public ReplicationTimer(ReplicationRequest replicationRequest) {
    this.replicationRequest = replicationRequest;
  }
}
