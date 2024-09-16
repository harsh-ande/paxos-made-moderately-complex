package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
  // Your code here...
  public int sequenceNumber;
  public Address address;
  public final Result result;

  public AMOResult(int sequenceNumber, Address address, Result result) {
    this.sequenceNumber = sequenceNumber;
    this.address = address;
    this.result = result;
  }
}
