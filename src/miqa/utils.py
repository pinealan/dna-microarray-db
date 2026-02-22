import logging
import httpx


def streamed_download(url: str, filename: str) -> None:
    with httpx.stream('GET', url) as response:
        with open(filename, 'wb') as f:
            for chunk in response.iter_bytes():
                f.write(chunk)


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )
    logging.getLogger('httpcore').setLevel(logging.INFO)


def assert_non_empty_list_str(v):
    assert isinstance(v, list)
    assert len(v) > 0
    assert isinstance(v[0], str)


def assert_list_str(v):
    assert isinstance(v, list)
    if len(v) > 0:
        assert isinstance(v[0], str)
