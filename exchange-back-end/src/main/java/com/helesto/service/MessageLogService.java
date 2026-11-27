package com.helesto.service;

import com.helesto.dto.*;
import com.helesto.journal.Main;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class MessageLogService {
  public static final String INCOMING = "incoming", OUTGOING = "outgoing";

  public MessageLogDto[] messageLogGet(String direction, Integer before, Integer limit)
      throws Exception {
    var rows = Main.messageLog(INCOMING.equals(direction), before, limit);
    var result = new MessageLogDto[rows.size()];
    for (int i = 0; i < result.length; i++) {
      var r = rows.getJsonObject(i);
      result[i] = new MessageLogDto(r.getInteger("id"), r.getString("time"), r.getString("text"));
    }
    return result;
  }
}
