package dslabs.atmostonce;

import java.util.HashMap;
import java.util.Map;

public class ApplicationState {
  public Map<String, Integer> clientSequenceNumMap = new HashMap<String, Integer>();
  public Map<String, AMOResult> clientAMOResultMap = new HashMap<String, AMOResult>();

  public ApplicationState(
      Map<String, Integer> clientSequenceNumMap, Map<String, AMOResult> clientAMOResultMap) {
    this.clientSequenceNumMap = clientSequenceNumMap;
    this.clientAMOResultMap = clientAMOResultMap;
  }
}
