package dslabs.paxos;

import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
  // Your code here...
  public AMOResult result;

  public PaxosReply(AMOResult result) {
    this.result = result;
  }

  public AMOResult getResult() {
    return this.result;
  }
}
