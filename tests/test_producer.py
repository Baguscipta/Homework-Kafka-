from kafka_project.producer import _json_serializer


def test_json_serializer_returns_bytes():
    data = {"hello": "world"}
    out = _json_serializer(data)
    assert isinstance(out, (bytes, bytearray))
