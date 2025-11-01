# create_or_recreate_topics.py
# pip install kafka-python
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
import time

BOOTSTRAP = "kafka:29092"   # adjust if needed
REPLICATION = 1             # single-broker dev
DEFAULT_CONFIGS = {
    "retention.ms": "604800000",  # 7 days
    "cleanup.policy": "delete",
}

# Define the topics you want to (re)create here:
TOPICS_TO_CREATE = [
    {"name": "tasks",  "partitions": 3, "configs": {}},
    {"name": "fragments", "partitions": 1, "configs": {}},
]

def wait_for_topics_absent(admin: KafkaAdminClient, names, timeout=30, poll=0.5):
    deadline = time.time() + timeout
    names = set(names)
    while time.time() < deadline:
        existing = set(admin.list_topics())
        if names.isdisjoint(existing):
            return True
        time.sleep(poll)
    return False

def wait_for_topics_present(admin: KafkaAdminClient, names, timeout=30, poll=0.5):
    deadline = time.time() + timeout
    names = set(names)
    while time.time() < deadline:
        existing = set(admin.list_topics())
        if names.issubset(existing):
            return True
        time.sleep(poll)
    return False

def recreate_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="opendt-admin")

    try:
        # 1) Delete targets that already exist
        existing = set(admin.list_topics())
        to_delete = [t["name"] for t in TOPICS_TO_CREATE if t["name"] in existing]
        if to_delete:
            print("Deleting existing topics:", to_delete)
            try:
                admin.delete_topics(topics=to_delete, timeout_ms=15000)
            except UnknownTopicOrPartitionError:
                # If some disappeared between list and delete, ignore
                pass

            ok = wait_for_topics_absent(admin, to_delete, timeout=60)
            if not ok:
                raise RuntimeError(f"Timed out waiting for deletion: {to_delete}")

        # 2) Create topics with the requested configs
        new_topics = []
        for spec in TOPICS_TO_CREATE:
            configs = dict(DEFAULT_CONFIGS)
            configs.update(spec.get("configs", {}))  # allow per-topic overrides

            new_topics.append(
                NewTopic(
                    name=spec["name"],
                    num_partitions=spec.get("partitions", 1),
                    replication_factor=REPLICATION,
                    topic_configs=configs,
                )
            )

        print("Creating topics:", [t.name for t in new_topics])
        try:
            admin.create_topics(new_topics=new_topics, validate_only=False, timeout_ms=20000)
        except TopicAlreadyExistsError:
            # Can happen if the broker auto-created it between delete and create; ignore
            pass

        ok = wait_for_topics_present(admin, [t.name for t in new_topics], timeout=60)
        if not ok:
            raise RuntimeError("Timed out waiting for topics to appear")

        print("Done.")
    finally:
        admin.close()

if __name__ == "__main__":
    recreate_topics()

