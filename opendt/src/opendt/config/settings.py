TIME_SCALE = 1 / 10 # so currently 30 virtual seconds = 5 real mins
REAL_WINDOW_SIZE_SEC = 5 * 60 # 5 min windows
VIRTUAL_WINDOW_SIZE = REAL_WINDOW_SIZE_SEC * TIME_SCALE

# TODO set (update) this and show this directly from the UI
SLO_MAX_ENERGY = 0  # the total energy consumed to complete all the tasks in the timeframe
SLO_MAX_TIME = 0    # the total time taken to complete all the tasks in the timeframe
