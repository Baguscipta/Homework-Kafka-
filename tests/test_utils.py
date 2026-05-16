from kafka_project.utils import load_messages


def test_load_messages_reads_list():
    msgs = load_messages("sample_data/messages.json")
    assert isinstance(msgs, list)
    assert len(msgs) >= 1
