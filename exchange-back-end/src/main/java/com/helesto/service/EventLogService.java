package com.helesto.service;

import com.helesto.dto.*;
import com.helesto.journal.Main;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class EventLogService {
  public EventLogDto[] eventLogGet() throws Exception {
    var rows = Main.events();
    var result = new EventLogDto[rows.size()];
    for (int i = 0; i < result.length; i++) {
      var r = rows.getJsonObject(i);
      result[i] = new EventLogDto(r.getInteger("id"), r.getString("time"), r.getString("text"));
    }
    return result;
  }
}
