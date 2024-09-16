package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final T application;

  // Your code here...
  public Map<Address, Integer> clientSequenceNumMap = new HashMap<Address, Integer>();
  public Map<Address, AMOResult> clientAMOResultMap = new HashMap<Address, AMOResult>();

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;
    AMOResult amoResult = null;
    if (this.alreadyExecuted(amoCommand)) {
      if (amoCommand.sequenceNumber < this.clientSequenceNumMap.get(amoCommand.address)) {
        return new AMOResult(amoCommand.sequenceNumber, amoCommand.address, null);
      } else {
        return this.clientAMOResultMap.get(amoCommand.address);
      }
    } else {
      Result result = this.application.execute(amoCommand.command);
      amoResult = new AMOResult(amoCommand.sequenceNumber, amoCommand.address, result);
      clientAMOResultMap.put(amoCommand.address, amoResult);
      clientSequenceNumMap.put(amoCommand.address, amoCommand.sequenceNumber);
    }
    return amoResult;
  }

  public Result executeReadOnly(Command command) {
    if (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    // Your code here...
    if (this.clientAMOResultMap.containsKey(amoCommand.address)
        && this.clientSequenceNumMap.get(amoCommand.address) >= amoCommand.sequenceNumber) {
      return true;
    }
    return false;
  }

  public Map<String, String> getState() {
    if (application instanceof KVStore) {
      return ((KVStore) application).GetState();
    }
    return null;
  }

  public void ingestState(Map<String, String> applicationState) {
    if (application instanceof KVStore) {
      ((KVStore) application).IngestState(applicationState);
    }
  }
}
