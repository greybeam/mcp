import threading

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


def test_callback_runs_on_setting_thread():
    """Per spec §5.1, callbacks fire from the thread that calls set()."""
    t = CancelToken()
    callback_thread: list[int] = []
    t.register_cancel(lambda: callback_thread.append(threading.get_ident()))

    setter = threading.Thread(target=t.set)
    setter.start()
    setter.join()

    assert callback_thread == [setter.ident]


def test_multiple_callbacks_all_fire():
    t = CancelToken()
    fired: list[int] = []
    t.register_cancel(lambda: fired.append(1))
    t.register_cancel(lambda: fired.append(2))
    t.set()
    assert fired == [1, 2]
