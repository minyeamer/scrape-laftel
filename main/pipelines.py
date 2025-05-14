from gscraper.base.spider import AsyncPipeline, Dag, Task
from gscraper.base.types import Data, Records, IndexLabel

from main.spiders import LaftelSearchSpider, LaftelStatisticsSpider
from main.data import LAFTEL_SEARCH_PLUS_INFO, LAFTEL_RAITING_SCHEMA
from main.urls import LAFTEL

import pandas as pd


class LaftelAsyncPipeline(AsyncPipeline):
    operation = "laftelPipeline"
    host = LAFTEL
    where = "LAFTEL"


class LaftelSearchPipeline(LaftelAsyncPipeline):
    operation = "laftelSearchPlus"
    returnType = "dataframe"
    info = LAFTEL_SEARCH_PLUS_INFO()
    dags = Dag(
        Task(operator=LaftelSearchSpider, task="async_crawl", dataName="search", dataType="dataframe", fields=tuple()),
        Task(operator=LaftelStatisticsSpider, task="crawl_statistics", dataName="statistics", dataType="dataframe",
            fields=tuple(), params=list(), derivData=["search"]),
    )

    @AsyncPipeline.init_task
    async def crawl(self, query: Records, sortType="rank", size=None, pageSize=60, offset=0, **context) -> Data:
        return await self.gather(**self.from_locals(locals()))

    @AsyncPipeline.limit_request
    async def crawl_statistics(self, worker: LaftelStatisticsSpider, search: pd.DataFrame, **params) -> pd.DataFrame:
        return await worker.crawl(search["modelId"].unique().tolist())

    @AsyncPipeline.arrange_data
    def map_reduce(self, search: pd.DataFrame, statistics: pd.DataFrame, **context) -> Data:
        duplicated = [__column for __column in statistics.columns if (__column != "modelId") and (__column in search)]
        if duplicated:
            statistics = statistics.drop(columns=duplicated)
        return search.merge(statistics, how="left", on="modelId")

    def get_upload_columns(self, name=str(), **context) -> IndexLabel:
        if name == "ratings": return LAFTEL_RAITING_SCHEMA().get("name")
        else: return list()
