package opendt

object Config {
  val BOOTSTRAP_SERVERS = "localhost:9092"
  val TASKS_TOPIC = "tasks"
  val FRAGMENTS_TOPIC = "fragments"
  val SIM_TIME_RATIO = 1.0 // Real-time simulation
}
