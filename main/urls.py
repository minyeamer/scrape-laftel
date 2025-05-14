from gscraper.base.spider import GET, POST, API
from typing import Literal


LAFTEL = "laftel"


def URL(method: Literal["GET","POST","API"], host: str, uri: str, query=str(), **params) -> str:
    if method == GET: return GET_URL(host, uri, query, **params)
    elif method == POST: return POST_URL(host, uri, query, **params)
    elif method == API: return API_URL(host, uri, query, **params)
    else: return str()


def GET_URL(host: str, uri: str, query=str(), **params) -> str:
    if host == LAFTEL: return LAFTEL_GET_URL(uri, query, **params)


def POST_URL(host: str, uri: str, query=str(), **params) -> str:
    if host == LAFTEL: return LAFTEL_POST_URL(uri, query, **params)


def API_URL(host: str, uri: str, query=str(), **params) -> str:
    if host == LAFTEL: return LAFTEL_API_URL(uri, query, **params)


def LAFTEL_GET_URL(uri: str, query=str(), **params) -> str:
    if uri == "main": return "https://laftel.net/"
    elif uri == "model": return f"https://laftel.net/finder?modal={query}&route=review" # modelId
    else: return str()


def LAFTEL_POST_URL(uri: str, query=str(), **params) -> str:
    return str()


def LAFTEL_API_URL(uri: str, query=str(), **params) -> str:
    if uri == "info": return "https://api.laftel.net/api/v1.0/info/discover/"
    elif uri == "search": return "https://api.laftel.net/api/search/v1/discover/"
    elif uri == "statistics": return f"https://api.laftel.net/api/items/v1/{query}/statistics/" # modelId
    else: return str()
