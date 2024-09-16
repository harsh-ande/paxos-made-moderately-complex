package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
  public static final int INITIAL_CONFIG_NUM = 0;

  private final int numShards;

  // Your code here...
  private int currentConfigNumber;
  private List<ShardConfig> historicShardConfigs;
  private Map<Integer, Integer> currentShardToGroupMap;
  private Map<Integer, Set<Address>> currentGroupToAddressMap;

  public ShardMaster(int numShards) {
    this.numShards = numShards;
    currentConfigNumber = INITIAL_CONFIG_NUM;
    historicShardConfigs = new LinkedList<>();
    currentShardToGroupMap = new HashMap<>();
    currentGroupToAddressMap = new HashMap<>();
    for (int i = 1; i < numShards + 1; i++) {
      currentShardToGroupMap.put(i, null);
    }
  }

  public interface ShardMasterCommand extends Command {}

  @Data
  public static final class Join implements ShardMasterCommand {
    private final int groupId;
    private final Set<Address> servers;
  }

  @Data
  public static final class Leave implements ShardMasterCommand {
    private final int groupId;
  }

  @Data
  public static final class Move implements ShardMasterCommand {
    private final int groupId;
    private final int shardNum;
  }

  @Data
  public static final class Query implements ShardMasterCommand {
    private final int configNum;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  public interface ShardMasterResult extends Result {}

  @Data
  public static final class Ok implements ShardMasterResult {}

  @Data
  public static final class Error implements ShardMasterResult {}

  @Data
  public static final class ShardConfig implements ShardMasterResult {
    private final int configNum;

    // groupId -> <group members, shard numbers>
    private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;

    public ShardConfig(int configNum, Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo) {
      this.configNum = configNum;
      this.groupInfo = groupInfo;
    }
  }

  @Override
  public Result execute(Command command) {
    if (command instanceof Join) {
      Join join = (Join) command;

      // Your code here...
      if (currentGroupToAddressMap.containsKey(join.groupId)) {
        return new Error();
      }
      int numOfIters = numOfTimesToAddNewGroup();
      System.out.printf("numOfIters = %d\n", numOfIters);
      for (int i = 0; i < numOfIters; i++) {
        int nextToBePutSlot = getNextToBePutSlot();
        currentShardToGroupMap.put(nextToBePutSlot, join.groupId);
      }
      currentGroupToAddressMap.put(join.groupId, join.servers);
      Set<Integer> zeroCountGroups = getZeroCountGroups();
      for (Integer zeroCountGroup : zeroCountGroups) {
        for (int i = 0; i < numOfIters; i++) {
          int nextToBePutSlot = getNextToBePutSlot();
          currentShardToGroupMap.put(nextToBePutSlot, zeroCountGroup);
        }
      }
      Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
      for (Integer group : currentGroupToAddressMap.keySet()) {
        groupInfo.put(group, getGroupInfoPairForGroupId(group));
      }
      historicShardConfigs.add(new ShardConfig(currentConfigNumber, groupInfo));
      currentConfigNumber++;
      System.out.printf("current shard to group map %s\n", currentShardToGroupMap);
      System.out.printf("current group to address map %s\n", currentGroupToAddressMap);
      System.out.printf("historic shard configs %s\n", historicShardConfigs);
      return new Ok();
    }

    if (command instanceof Leave) {
      Leave leave = (Leave) command;

      // Your code here...
      if (!currentGroupToAddressMap.containsKey(leave.groupId)) {
        return new Error();
      }
      int nullCount = markShardsWithGroupNull(leave.groupId);
      for (int i = 0; i < nullCount; i++) {
        int groupIdToAdd = getLowestCountGroup();
        currentShardToGroupMap.put(getNextToBePutSlot(), groupIdToAdd);
      }
      currentGroupToAddressMap.remove(leave.groupId);
      Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
      for (Integer group : currentGroupToAddressMap.keySet()) {
        groupInfo.put(group, getGroupInfoPairForGroupId(group));
      }
      historicShardConfigs.add(new ShardConfig(currentConfigNumber, groupInfo));
      currentConfigNumber++;
      System.out.printf("current shard to group map %s\n", currentShardToGroupMap);
      System.out.printf("current group to address map %s\n", currentGroupToAddressMap);
      System.out.printf("historic shard configs %s\n", historicShardConfigs);
      return new Ok();
    }

    if (command instanceof Move) {
      Move move = (Move) command;

      // Your code here...
      if ((move.shardNum < 1 || move.shardNum > numShards)
          || currentShardToGroupMap.get(move.shardNum) == move.groupId
          || !currentGroupToAddressMap.containsKey(move.groupId)) {
        return new Error();
      }
      currentShardToGroupMap.put(move.shardNum, move.groupId);
      System.out.printf("Just before removeAddressesIfGroupCountZero move cmd %s\n", move);
      removeAddressesIfGroupCountZero();

      Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
      for (Integer group : currentGroupToAddressMap.keySet()) {
        groupInfo.put(group, getGroupInfoPairForGroupId(group));
      }
      historicShardConfigs.add(new ShardConfig(currentConfigNumber, groupInfo));
      currentConfigNumber++;
      System.out.printf("current shard to group map %s\n", currentShardToGroupMap);
      System.out.printf("current group to address map %s\n", currentGroupToAddressMap);
      System.out.printf("historic shard configs %s\n", historicShardConfigs);
      return new Ok();
    }

    if (command instanceof Query) {
      Query query = (Query) command;

      // Your code here...
      if (query.configNum < 0 || query.configNum >= currentConfigNumber) {
        if (historicShardConfigs.size() > 0) {
          return historicShardConfigs.get(historicShardConfigs.size() - 1);
        }
        return new Error();
      }
      return historicShardConfigs.get(query.configNum);
    }

    System.out.printf("Hiiiiiii");
    throw new IllegalArgumentException();
  }

  private int getNextToBePutSlot() {
    for (int i = 1; i < numShards + 1; i++) {
      if (currentShardToGroupMap.get(i) == null) {
        return i;
      }
    }
    Map<Integer, Integer> groupToCountMap = new HashMap<>();
    for (int i = 1; i < numShards + 1; i++) {
      if (!groupToCountMap.containsKey(currentShardToGroupMap.get(i))) {
        groupToCountMap.put(currentShardToGroupMap.get(i), 0);
      }
      groupToCountMap.put(
          currentShardToGroupMap.get(i), groupToCountMap.get(currentShardToGroupMap.get(i)) + 1);
    }
    int highestAllocatedGroupId = -1;
    int highestCountSoFar = -1;
    for (Integer groupId : groupToCountMap.keySet()) {
      if (groupToCountMap.get(groupId) > highestCountSoFar) {
        highestCountSoFar = groupToCountMap.get(groupId);
        highestAllocatedGroupId = groupId;
      }
    }
    int highestAllocatedGroupShard = -1;
    for (Integer shard : currentShardToGroupMap.keySet()) {
      if (currentShardToGroupMap.get(shard) == highestAllocatedGroupId) {
        highestAllocatedGroupShard = shard;
        return highestAllocatedGroupShard;
      }
    }
    return -1;
  }

  private int numOfTimesToAddNewGroup() {
    int currentNumOfGroups = -1;
    System.out.printf("Just b4 exception %s\n", currentShardToGroupMap.values());
    //    currentShardToGroupMap.values().
    //    Set<Integer> currentGroupsSet = (Set<Integer>) currentShardToGroupMap.values().size();
    if (currentShardToGroupMap.values().contains(null)) {
      System.out.printf("Contains null\n");
      return (int) (numShards / (currentShardToGroupMap.values().stream().distinct().count()));
    }
    return (int)
        (numShards
            / (currentShardToGroupMap.values().stream().distinct().count()
                + 1
                + getZeroCountGroups().size()));
  }

  private Set<Integer> getZeroCountGroups() {
    Map<Integer, Integer> groupToCountMap = new HashMap<>();
    for (Integer groupId : currentGroupToAddressMap.keySet()) {
      if (!groupToCountMap.containsKey(groupId)) {
        groupToCountMap.put(groupId, 0);
      }
    }
    for (int i = 1; i < numShards + 1; i++) {
      if (currentShardToGroupMap.get(i) == null) {
        continue;
      }
      groupToCountMap.put(
          currentShardToGroupMap.get(i), groupToCountMap.get(currentShardToGroupMap.get(i)) + 1);
    }
    Set<Integer> zeroCountGroups = new HashSet<>();
    for (Integer groupId : groupToCountMap.keySet()) {
      if (groupToCountMap.get(groupId) == 0) {
        zeroCountGroups.add(groupId);
      }
    }
    return zeroCountGroups;
  }

  private Pair<Set<Address>, Set<Integer>> getGroupInfoPairForGroupId(int groupId) {
    Set<Integer> shards = new HashSet<>();
    System.out.printf("current shard to group map in get group info %s\n", currentShardToGroupMap);
    for (Integer shard : currentShardToGroupMap.keySet()) {
      if (currentShardToGroupMap.get(shard) == groupId) {
        shards.add(shard);
      }
    }
    // <group members, shard numbers>
    Pair<Set<Address>, Set<Integer>> groupInforPair =
        new ImmutablePair<>(currentGroupToAddressMap.get(groupId), shards);
    return groupInforPair;
  }

  private int markShardsWithGroupNull(int groupId) {
    int count = 0;
    for (Integer shard : currentShardToGroupMap.keySet()) {
      if (currentShardToGroupMap.get(shard) == groupId) {
        currentShardToGroupMap.put(shard, null);
        count++;
      }
    }
    return count;
  }

  private int getLowestCountGroup() {
    Map<Integer, Integer> groupToCountMap = new HashMap<>();
    for (int i = 1; i < numShards + 1; i++) {
      if (currentShardToGroupMap.get(i) == null) {
        continue;
      }
      if (!groupToCountMap.containsKey(currentShardToGroupMap.get(i))) {
        groupToCountMap.put(currentShardToGroupMap.get(i), 0);
      }
      groupToCountMap.put(
          currentShardToGroupMap.get(i), groupToCountMap.get(currentShardToGroupMap.get(i)) + 1);
    }
    //    for(Integer groupId: currentGroupToAddressMap.keySet()){
    //      if(!groupToCountMap.containsKey(groupId)){
    //        groupToCountMap.put(groupId, 0);
    //      }
    //    }
    int lowestAllocatedGroupId = -1;
    int lowestCountSoFar = numShards;
    for (Integer groupId : groupToCountMap.keySet()) {
      if (groupToCountMap.get(groupId) < lowestCountSoFar) {
        lowestCountSoFar = groupToCountMap.get(groupId);
        lowestAllocatedGroupId = groupId;
      }
    }
    return lowestAllocatedGroupId;
  }

  private void removeAddressesIfGroupCountZero() {
    int groupId = getLowestCountGroup();
    System.out.printf("Got groupid %d\n", groupId);
    for (int i = 1; i < numShards + 1; i++) {
      if (currentShardToGroupMap.get(i) == groupId) {
        return;
      }
    }
    currentGroupToAddressMap.remove(groupId);
    //    if(groupId!=-1){
    //      currentGroupToAddressMap.put(groupId, null);
    //    }
  }
}
