import httpx


def streamed_download(url: str, filename: str) -> None:
    with httpx.stream('GET', url) as response:
        with open(filename, 'wb') as f:
            for chunk in response.iter_bytes():
                f.write(chunk)
