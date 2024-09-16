package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Message;
import java.util.Map;
import lombok.Data;

/* -----------------------------------------------------------------------------------------------
 *  ViewServer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Ping implements Message {
  private final int viewNum;
}

@Data
class GetView implements Message {}

@Data
class ViewReply implements Message {
  private final View view;

  public View getView() {
    return view;
  }
}

/* -----------------------------------------------------------------------------------------------
 *  Primary-Backup Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Request implements Message {
  // Your code here...
  public AMOCommand command;

  public View view;

  public Request(AMOCommand amoCommand, View view) {
    command = amoCommand;
    this.view = view;
  }
}

@Data
class Reply implements Message {
  // Your code here...
  public AMOResult result;
  public View view;

  public Reply(AMOResult amoResult, View view) {
    result = amoResult;
    this.view = view;
  }

  public AMOResult getResult() {
    return result;
  }
}

// Your code here...

class ApplicationStateRequest extends Request {
  public Map<String, String> applicationState;

  public ApplicationStateRequest(AMOCommand amoCommand, View view) {
    super(amoCommand, view);
  }

  public ApplicationStateRequest(Map<String, String> applicationState, View view) {
    super(null, view);
    this.applicationState = applicationState;
  }
}

class ApplicationStateReply extends Reply {
  public Map<String, String> applicationState;

  public ApplicationStateReply(AMOResult amoResult, View view) {
    super(amoResult, view);
  }

  public ApplicationStateReply(Map<String, String> applicationState, View view) {
    super(null, view);
    this.applicationState = applicationState;
  }
}

@Data
class ReplicationRequest implements Message {
  public Address client;
  public AMOCommand command;
  public View view;

  public ReplicationRequest(AMOCommand amoCommand, Address client, View view) {
    command = amoCommand;
    this.client = client;
    this.view = view;
  }
}

@Data
class ReplicationReply implements Message {
  // Your code here...
  public Address client;
  public AMOResult result;
  public View view;

  public ReplicationReply(AMOResult amoResult, Address client, View view) {
    result = amoResult;
    this.client = client;
    this.view = view;
  }

  public AMOResult getResult() {
    return result;
  }
}
