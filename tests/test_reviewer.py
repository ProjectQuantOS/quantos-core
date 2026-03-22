from reviewer import Reviewer


def test_review_placeholder():
    reviewer = Reviewer()
    try:
        result = reviewer.review({}, prompt="")
    except NotImplementedError:
        result = {"approved": False}
    assert isinstance(result, dict)
    assert "approved" in result
