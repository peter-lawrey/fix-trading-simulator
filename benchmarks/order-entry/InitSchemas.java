import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;

/** Initialize only the fresh PostgreSQL instance created by the benchmark. */
class InitSchemas {
  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[0]);
    try (var conn =
            DriverManager.getConnection(
                System.getenv("SIM_DB_URL"),
                System.getenv("SIM_DB_USER"),
                System.getenv("SIM_DB_PASSWORD"));
        var statement = conn.createStatement()) {
      conn.setAutoCommit(false);
      for (int lane = 0; lane < 2; lane++) {
        for (String role : new String[] {"broker", "exchange"}) {
          String sql = Files.readString(root.resolve("init-" + role + ".sql"));
          statement.execute(sql.replaceAll("\\b" + role + "\\b", "b" + lane + role));
        }
      }
      conn.commit();
      try (var values =
          statement.executeQuery(
              "select current_setting('server_version'), "
                  + "current_setting('fsync'), current_setting('synchronous_commit')")) {
        values.next();
        System.out.printf(
            "PostgreSQL %s, fsync=%s, synchronous_commit=%s%n",
            values.getString(1), values.getString(2), values.getString(3));
      }
    }
  }
}
