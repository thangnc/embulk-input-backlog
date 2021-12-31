Embulk::JavaPlugin.register_input(
  "backlog", "org.embulk.input.backlog.BacklogInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
