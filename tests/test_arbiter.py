from arbiter import arbitrate


def test_arbitrate_placeholder():
    assert arbitrate([]) is None
