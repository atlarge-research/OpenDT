"""Kafka topic management utilities."""

import logging
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

logger = logging.getLogger(__name__)


def ensure_topics_exist(
    bootstrap_servers: str,
    topics: list[str],
    num_partitions: int = 1,
    replication_factor: int = 1,
    max_retries: int = 10,
    retry_delay: float = 2.0,
) -> bool:
    """Ensure Kafka topics exist, creating them if necessary.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topics: List of topic names to create
        num_partitions: Number of partitions per topic
        replication_factor: Replication factor
        max_retries: Maximum number of connection retries
        retry_delay: Delay between retries in seconds

    Returns:
        True if all topics exist or were created successfully

    Raises:
        KafkaError: If topics cannot be created after max retries
    """
    for attempt in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="topic-manager",
                request_timeout_ms=10000,
            )

            # Get existing topics
            existing_topics = admin_client.list_topics()
            topics_to_create = [t for t in topics if t not in existing_topics]

            if not topics_to_create:
                logger.info(f"All topics already exist: {topics}")
                admin_client.close()
                return True

            # Create missing topics
            new_topics = [
                NewTopic(
                    name=topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
                for topic in topics_to_create
            ]

            try:
                admin_client.create_topics(new_topics, validate_only=False)
                logger.info(f"Created topics: {topics_to_create}")

                # Give Kafka a moment to create the topics
                time.sleep(1.0)

                # Verify topics were created
                existing_topics_after = admin_client.list_topics()
                still_missing = [t for t in topics_to_create if t not in existing_topics_after]

                if still_missing:
                    logger.warning(f"Some topics may not be ready yet: {still_missing}")
                else:
                    logger.info(f"✓ Successfully ensured all topics exist: {topics}")

                admin_client.close()
                return True

            except TopicAlreadyExistsError:
                logger.info("Topics already exist (race condition)")
                admin_client.close()
                return True

        except KafkaError as e:
            logger.warning(f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    return False


def wait_for_topics(
    bootstrap_servers: str,
    topics: list[str],
    max_retries: int = 30,
    retry_delay: float = 1.0,
) -> bool:
    """Wait for Kafka topics to exist.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topics: List of topic names to wait for
        max_retries: Maximum number of retries
        retry_delay: Delay between retries in seconds

    Returns:
        True if all topics exist

    Raises:
        TimeoutError: If topics don't exist after max retries
    """
    for attempt in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="topic-waiter",
                request_timeout_ms=5000,
            )

            existing_topics = admin_client.list_topics()
            missing_topics = [t for t in topics if t not in existing_topics]

            admin_client.close()

            if not missing_topics:
                logger.info(f"All topics ready: {topics}")
                return True

            logger.info(
                f"Waiting for topics (attempt {attempt + 1}/{max_retries}): {missing_topics}"
            )
            time.sleep(retry_delay)

        except KafkaError as e:
            logger.debug(f"Kafka not ready yet: {e}")
            time.sleep(retry_delay)

    raise TimeoutError(f"Topics not found after {max_retries} attempts: {topics}")
