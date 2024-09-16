package dslabs.primarybackup;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

enum viewState {
  NOTHING,
  TOBEINCREMENTED,
  INCREMENTED
}

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  // Your code here...
  private View currentVSView;
  private int latestPrimaryView;
  private int latestBackupView;
  private Set<Address> extraServers;
  private boolean primaryPinged = false;
  private boolean backupPinged = false;
  private viewState primaryViewState = viewState.NOTHING;
  private boolean primarySanctionRequired = false;
  private View postSanctionView;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
  }

  @Override
  public void init() {
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
    // Your code here...
    currentVSView = new View(STARTUP_VIEWNUM, null, null);
    latestPrimaryView = -1;
    latestBackupView = -1;
    extraServers = new HashSet<>();
    postSanctionView = null;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    // Your code here...
    int nextView =
        currentVSView.viewNum() == STARTUP_VIEWNUM ? INITIAL_VIEWNUM : currentVSView.viewNum() + 1;
    if (currentVSView != null && Objects.equals(sender, currentVSView.getPrimary())) {
      if (m.viewNum() < latestPrimaryView) {
        return;
      }
    } else if (currentVSView != null && Objects.equals(sender, currentVSView.getBackup())) {
      if (m.viewNum() < latestBackupView) {
        return;
      }
    }

    if (currentVSView.primary() == null && currentVSView.backup() == null) {
      currentVSView = new View(nextView, sender, null);
      primaryPinged = true;
      latestPrimaryView = m.viewNum();
      send(new ViewReply(currentVSView), sender);
    } else if (currentVSView.primary() != null && currentVSView.backup() == null) {
      if (Objects.equals(sender, currentVSView.primary())) {
        //        System.out.printf("*** Here it is primary pinging when primary !=null and backup
        // null and curr view = %s\n", currentVSView);
        primaryPinged = true;
        latestPrimaryView = m.viewNum();
        if (latestPrimaryView != currentVSView.viewNum()) {
          send(new ViewReply(currentVSView), sender);
        }
        if (postSanctionView != null && backupPinged) {
          //          System.out.printf("********* postSanctionView != null && backupPinged ********
          // post view %s\n", postSanctionView);
          currentVSView = postSanctionView;
        } else if (postSanctionView != null) {
          latestBackupView = -1;
          postSanctionView = null;
        }
        send(new ViewReply(currentVSView), sender);
      } else {
        backupPinged = true;
        latestBackupView = m.viewNum();
        if (latestPrimaryView == currentVSView.viewNum()) {
          //          System.out.printf("*** Inside primaryview = currview block = %d",
          // latestPrimaryView);
          currentVSView = new View(nextView, currentVSView.primary(), sender);
          send(new ViewReply(currentVSView), sender); // TODO handle case when queue size >1
          postSanctionView = null;
        } else {
          //          System.out.printf("*** Inside primaryview != currview block. primary view =
          // %d. curr view = %s\n", latestPrimaryView, currentVSView);
          postSanctionView = new View(nextView, currentVSView.primary(), sender);
          send(new ViewReply(currentVSView), sender); // TODO handle case when queue size >1
        }
      }
    } else if (currentVSView.primary() == null && currentVSView.backup() != null) {
      if (Objects.equals(sender, currentVSView.backup())) {
        //                currentVSView = new View(nextView, currentVSView.backup(), null);
        backupPinged = true;
        latestBackupView = m.viewNum();
        send(new ViewReply(currentVSView), sender);
      }
      // TODO Add to extraServers queue
    } else if (currentVSView.primary() != null && currentVSView.backup() != null) {
      if (Objects.equals(sender, currentVSView.primary())) {
        primaryPinged = true;
        latestPrimaryView = m.viewNum();
      } else if (Objects.equals(sender, currentVSView.backup())) {
        backupPinged = true;
        latestBackupView = m.viewNum();
      }
      send(new ViewReply(currentVSView), sender);
      // TODO Add to extraServers queue
    }
    if (((currentVSView.primary() != null && !Objects.equals(sender, currentVSView.primary()))
        && (currentVSView.backup() != null && !Objects.equals(sender, currentVSView.backup())))) {
      extraServers.add(sender);
    }
  }

  private void handleGetView(GetView m, Address sender) {
    // Your code here...
    if (Objects.equals(sender, currentVSView.backup())) {
      if (latestBackupView == currentVSView.getViewNum()) {
        return;
      }
    } else if (Objects.equals(sender, currentVSView.primary())) {
      if (latestPrimaryView == currentVSView.getViewNum()) {
        return;
      }
    }
    send(new ViewReply(currentVSView), sender);
    System.out.printf(
        "** Got view request in viewserver from client %s. Returning view %s\n",
        sender, currentVSView);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    // Your code here...
    //        System.out.printf("onPingCheckTimer triggered for %s\n", t.toString());
    //        System.out.printf("%b primary, %b backup\n", primaryPinged, backupPinged);
    //        int nextView = currentVSView.viewNum() == STARTUP_VIEWNUM ? INITIAL_VIEWNUM :
    // currentVSView.viewNum() + 1;
    if (primaryPinged && backupPinged) {
      backupPinged = false;
      primaryPinged = false;
      set(t, PING_CHECK_MILLIS);
    } else if (!primaryPinged && backupPinged) {
      System.out.printf(" ******* Primary didnt ping and backup pinged\n");
      Address nextBackup;
      if (extraServers.isEmpty()) {
        nextBackup = null;
      } else {
        nextBackup = extraServers.iterator().next();
        extraServers.remove(nextBackup);
      }
      //            System.out.printf("latest backup view %d\n", latestBackupView);
      //            System.out.printf("curr view number %d\n", currentVSView.viewNum());
      if (latestPrimaryView == currentVSView.viewNum()
          && latestBackupView == currentVSView.viewNum()) {
        currentVSView = new View(currentVSView.viewNum() + 1, currentVSView.backup(), nextBackup);
      } else {
        extraServers.add(nextBackup);
      }
      backupPinged = false;
      primaryPinged = false;
      set(t, PING_CHECK_MILLIS);
    } else if (primaryPinged && !backupPinged) { // TODO: And backup exists check
      if (currentVSView.backup() != null && latestPrimaryView == currentVSView.viewNum()) {
        System.out.printf(
            "Primary pinged, backup didn't ping block. latest primary view %d\n",
            latestPrimaryView);
        currentVSView = new View(currentVSView.viewNum() + 1, currentVSView.primary(), null);
      }
      backupPinged = false;
      primaryPinged = false;
      set(t, PING_CHECK_MILLIS);
    } else if (!primaryPinged && !backupPinged) {
      backupPinged = false;
      primaryPinged = false;
      set(t, PING_CHECK_MILLIS);
    }
    extraServers = new HashSet<>();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private void handleBackupDown() {}

  private void handlePrimaryDown() {}
}
