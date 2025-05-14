from gscraper.base.spider import Spider, AsyncSpider, GET, API
from gscraper.base.session import Flow
from gscraper.base.types import Data, JsonData, Records, Id
from gscraper.utils.request import get_headers

from main.data import LAFTEL_CATEGORY_SCHEMA
from main.data import LAFTEL_SEARCH_INFO, LAFTEL_SEARCH_PARAMS
from main.data import LAFTEL_STATISTICS_INFO
from main.urls import URL, LAFTEL

from typing import Dict


class LaftelSpider(Spider):
    operation = "laftelSpider"
    host = LAFTEL
    where = "LAFTEL"


class LaftelAsyncSpider(AsyncSpider):
    operation = "laftelSpider"
    host = LAFTEL
    where = "LAFTEL"


class LaftelCategorySpider(LaftelSpider):
    operation = "laftelCategory"
    which = "information"
    responseType = "records"
    returnType = "records"
    info = LAFTEL_CATEGORY_SCHEMA()
    flow = Flow("category")

    @LaftelSpider.init_session
    def crawl(self, **context) -> Data:
        return self.gather(**context)

    @LaftelSpider.retry_request
    @LaftelSpider.limit_request
    def fetch(self, **context) -> Records:
        url = URL(API, LAFTEL, "info")
        referer = URL(GET, LAFTEL, "main")
        headers = get_headers(url, referer=referer, origin=referer)
        response = self.request_json(GET, url, headers=headers, **context)
        return self.parse(response, **context)

    @LaftelSpider.validate_response
    def parse(self, response: JsonData, **context) -> Records:
        data = list()
        data += [dict(name=__genre, type="genre") for __genre in response["genres"]]
        data += [dict(name=__tag, type="tag") for __tag in response["tags"]]
        return self.map(data, **context)


class LaftelSearchSpider(LaftelAsyncSpider):
    operation = "laftelSearch"
    which = "models"
    iterateArgs = ["query"]
    iterateUnit = 1
    pagination = True
    responseType = "records"
    returnType = "records"
    root = ["results"]
    countby = "start"
    info = LAFTEL_SEARCH_INFO()
    flow = Flow("search")

    @LaftelAsyncSpider.init_session
    async def crawl(self, query: Records, sortType="rank", size=None, pageSize=60, offset=0, **context) -> Data:
        args, context = self.validate_params(locals())
        return await self.gather_count(*args, countPath=["count"], **context)

    @LaftelAsyncSpider.retry_request
    @LaftelAsyncSpider.limit_request
    async def fetch(self, query: Dict, sortType="rank", pageSize=60, start=0, **context) -> Records:
        url = URL(API, LAFTEL, "search")
        referer = URL(GET, LAFTEL, "main")
        params = LAFTEL_SEARCH_PARAMS(**query, sortType=sortType, offset=start, size=pageSize)
        headers = get_headers(url, referer=referer, origin=referer)
        response = await self.request_json(GET, url, params=params, headers=headers, **context)
        return self.parse(response, **context)


class LaftelStatisticsSpider(LaftelAsyncSpider):
    operation = "laftelStatistics"
    which = "statistics"
    iterateArgs = ["modelId"]
    iterateUnit = 1
    responseType = "dict"
    returnType = "records"
    info = LAFTEL_STATISTICS_INFO()
    flow = Flow("statistics")

    @LaftelAsyncSpider.init_session
    async def crawl(self, modelId: Id, **context) -> Data:
        return await self.gather(*self.validate_args(modelId), **context)

    @LaftelAsyncSpider.retry_request
    @LaftelAsyncSpider.limit_request
    async def fetch(self, modelId: str, **context) -> Records:
        url = URL(API, LAFTEL, "statistics", modelId)
        referer = URL(GET, LAFTEL, "model", modelId)
        headers = get_headers(url, referer=referer, origin=referer)
        response = await self.request_json(GET, url, headers=headers, **context)
        return self.parse(response, modelId=modelId, **context)
