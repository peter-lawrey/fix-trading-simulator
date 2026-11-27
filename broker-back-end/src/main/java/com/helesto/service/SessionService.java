package com.helesto.service;

import com.helesto.dto.*;
import com.helesto.journal.Main;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class SessionService {
  public SessionDto sessionGet() {
    return Main.status().mapTo(SessionDto.class);
  }

  public SessionDto startInitiator() throws Exception {
    return Main.control("start").mapTo(SessionDto.class);
  }

  public SessionDto stopInitiator() throws Exception {
    return Main.control("stop").mapTo(SessionDto.class);
  }

  public SessionDto logout() throws Exception {
    return Main.control("logout").mapTo(SessionDto.class);
  }
}
