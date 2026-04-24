from greybeam_mcp.cancel import CancelToken


def test_token_starts_unset():
    t = CancelToken()
    assert t.is_set() is False


def test_token_set_is_idempotent():
    t = CancelToken()
    t.set()
    assert t.is_set()
    t.set()
    assert t.is_set()


def test_register_cancel_fires_on_set():
    t = CancelToken()
    fired = []
    t.register_cancel(lambda: fired.append(1))
    assert fired == []
    t.set()
    assert fired == [1]


def test_register_cancel_fires_immediately_if_already_set():
    t = CancelToken()
    t.set()
    fired = []
    t.register_cancel(lambda: fired.append(1))
    assert fired == [1]


def test_set_only_fires_callbacks_once():
    t = CancelToken()
    fired = []
    t.register_cancel(lambda: fired.append(1))
    t.set()
    t.set()
    assert fired == [1]


def test_callback_exception_does_not_propagate():
    t = CancelToken()

    def boom() -> None:
        raise RuntimeError("boom")

    t.register_cancel(boom)
    t.set()  # must not raise
    assert t.is_set()
