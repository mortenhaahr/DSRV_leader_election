from dsrv_leader_election.event_logger.mock_logger import MockEventLogger
from dsrv_leader_election.event_logger.tc_types import TypedTCData
from dsrv_leader_election.event_logger.topic_mapping import TopicMapping


def test_emit_stores_messages_internally() -> None:
    logger = MockEventLogger(topic_mapping=TopicMapping({"election": "raft/election"}))

    logger.emit("election", TypedTCData("Map", {"term": 2, "leader_id": 1}))

    assert len(logger.emitted_messages) == 1
    assert logger.emitted_messages[0].var == "election"
    assert logger.emitted_messages[0].topic == "raft/election"
    assert logger.emitted_messages[0].value == TypedTCData(
        "Map", {"term": 2, "leader_id": 1}
    )


def test_emit_uses_fallback_topic_name() -> None:
    logger = MockEventLogger(topic_mapping=TopicMapping({}))

    logger.emit("heartbeat", TypedTCData("Bool", True))

    assert logger.emitted_messages[0].topic == "heartbeat"


def test_clear_removes_stored_messages() -> None:
    logger = MockEventLogger(topic_mapping=TopicMapping({}))
    logger.emit("status", TypedTCData("Str", "ok"))

    logger.clear()

    assert logger.emitted_messages == []
    assert logger.last() is None
