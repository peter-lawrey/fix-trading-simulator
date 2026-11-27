package com.helesto.core;

import com.helesto.journal.Main;
import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class Exchange {
  public boolean isAutomaticTrade() {
    return Main.automaticTrade();
  }

  public void setAutomaticTrade(boolean value) {
    Main.automaticTrade(value);
  }

  public int getAutomaticTradeSeconds() {
    return Main.delay();
  }

  public void setAutomaticTradeSeconds(int value) {
    Main.delay(value);
  }
}
