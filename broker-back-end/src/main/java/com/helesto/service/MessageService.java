package com.helesto.service;

import com.helesto.dto.*;
import com.helesto.journal.Main;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class MessageService {
  public MessageDto[] messageGet() throws Exception {
    var rows = Main.messages(false, true);
    var result = new MessageDto[rows.size()];
    for (int i = 0; i < result.length; i++) {
      var r = rows.getJsonObject(i);
      result[i] = new MessageDto(r.getInteger("msgseqnum"), r.getString("message"));
    }
    return result;
  }
}
