package com.helesto.core;

import com.helesto.journal.Main;
import com.helesto.socket.OrderSocket;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@ApplicationScoped
public class Bootstrap {
  @Inject OrderSocket orders;

  void start(@Observes StartupEvent event) throws Exception {
    Main.open(orders::broadcast);
  }

  void stop(@Observes ShutdownEvent event) throws Exception {
    Main.close();
  }
}
