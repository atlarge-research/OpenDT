package opendt

object Config {
    val TASKS_TOPIC    = "tasks"
    val FRAGMENTS_TOPIC = "fragments"
    val BOOTSTRAP_SERVERS = "localhost:9092"
    val SIM_TIME_RATIO: Double = 1.0 / 30.0
    val WINDOW_SIZE_MS: Long = 300000
    val TOPOLOGY_FINDING_DEADLINE_MS: Long = WINDOW_SIZE_MS - 30000
    val WINDOW_SIZE_MS_SIM: Long = (WINDOW_SIZE_MS * SIM_TIME_RATIO).toLong
    val TOPOLOGY_FINDING_DEADLINE_MS_SIM: Long =  (TOPOLOGY_FINDING_DEADLINE_MS * SIM_TIME_RATIO).toLong
}
