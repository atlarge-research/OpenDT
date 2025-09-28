package opendt

object Config {
    val TASKS_TOPIC    = "tasks"
    val FRAGMENTS_TOPIC = "fragments"
    val BOOTSTRAP_SERVERS = "localhost:9092"
    val SIM_TIME_RATIO: Double = 1.0 / 30.0
    val WINDOW_SIZE_MS: Long = 300000
    val WINDOW_SIZE_MS_SIM: Long = (WINDOW_SIZE_MS * SIM_TIME_RATIO).toLong
}
