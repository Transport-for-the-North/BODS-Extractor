# -*- coding: utf-8 -*-
"""Functionality for downloading data from API."""

##### IMPORTS #####
# Standard imports
import logging
from typing import Optional

# Third party imports
import caf.toolkit
import requests
from pydantic import dataclasses
from tqdm import tqdm

# Local imports


##### CONSTANTS #####
LOG = logging.getLogger(__name__)
TIMEOUT = 20
BODS_API_BASE_URL = "https://data.bus-data.dft.gov.uk/api/v1/"


##### CLASSES #####
class APIAuth(caf.toolkit.BaseConfig):
    """Loads and stores BODS API account details."""

    name: str
    password: str


@dataclasses.dataclass
class BoundingBox:
    """Bounding box parameter for BODS API requests."""

    min_latitude: float
    max_latitude: float
    min_longitude: float
    max_longitude: float

    def as_str(self) -> str:
        """Convert to str in the format for BODS API requests.

        Format: "min_latitude,max_latitude,min_longitude,max_latitude".
        Each number is formatted to 5 decimal places.
        """
        return (
            f"{self.min_latitude:.5f},{self.max_latitude:.5f},"
            f"{self.min_longitude:.5f},{self.max_latitude:.5f}"
        )


##### FUNCTIONS #####
def get_str(**kwargs) -> str:
    """Get request which decodes content to string, wraps `get`.

    See Also
    --------
    `get`: for list of parameters.
    """
    content, encoding = get(**kwargs)

    if encoding is None:
        # TODO Determine encoding and decode if possible
        raise ValueError("unknown encoding")

    return content.decode(encoding)


def get(
    url: str,
    auth: Optional[APIAuth] = None,
    timeout: int = TIMEOUT,
    pbar: bool = True,
    **kwargs,
) -> tuple[bytes, Optional[str]]:
    """Get request with progress bar.

    Wraps `requests.get` function.

    Parameters
    ----------
    url : str
        URL for the request.
    auth : Optional[APIAuth], optional
        _description_, by default None
    timeout : int, default `TIMEOUT`
        Timeout for request.
    pbar : bool, default True
        Whether, or not, to display a progress bar using tqdm.
    kwargs : Keyword arguments
        Additional keyword arguments to pass directly to `requests.get`.

    Returns
    -------
    bytes
        Content returned from get request.
    str | None
        Encoding of content, if given by response.

    Raises
    ------
    HTTPError
    """
    if auth is None:
        auth = None
    else:
        auth = (auth.name, auth.password)

    response = requests.get(url, stream=True, auth=auth, timeout=timeout, **kwargs)

    message = (
        f"""Get request sent to "{response.url}", response """
        f"status {response.status_code} - {response.reason}"
    )
    if response.ok:
        LOG.debug(message)
    else:
        LOG.warning(message)

    response.raise_for_status()

    if pbar:
        content = _pbar_download(response)
    else:
        content = response.content

    return content, response.encoding


def _pbar_download(response: requests.Response) -> bytes:
    total_size = int(response.headers["content-length"])
    pbar = tqdm(
        total=total_size, desc=f"Downloading {response.url}", unit="B", unit_scale=True
    )

    # Attempt to get roughly 1000 chunks unless that would be < 10_000 bytes
    chunk_size = max(10_000, total_size // 1000)

    content = b""
    for data in response.iter_content(chunk_size=chunk_size, decode_unicode=False):
        content += data
        pbar.update(len(data))

    pbar.close()

    return content
