from gscraper.base.session import Info, Query, Schema, Field
from gscraper.base.session import Join
from gscraper.base.session import QUERY, NULLABLE, NULLIFY, INDEX

from gscraper.base.abstract import Variable, PAGE_RANGE_QUERY
from gscraper.base.spider import PipelineInfo
from gscraper.base.gcloud import BigQuerySchema


LAFTEL_RAITING_SCHEMA = lambda: BigQuerySchema(*[
    {"name":"modelId", "type":"STRING", "description":"ID", "mode":"REQUIRED"},
    {"name":"rank", "type":"INTEGER", "description":"순위"},
    {"name":"title", "type":"STRING", "description":"제목"},
    {"name":"imageUrl", "type":"STRING", "description":"이미지주소"},
    {"name":"isAdult", "type":"BOOLEAN", "description":"성인이용가"},
    {"name":"isEnding", "type":"BOOLEAN", "description":"완결여부"},
    {"name":"genres", "type":"STRING", "description":"장르"},
    {"name":"averageScore", "type":"FLOAT", "description":"평균별점"},
    {"name":"countScore", "type":"INTEGER", "description":"별점수"},
    {"name":"countScore05", "type":"INTEGER", "description":"0.5점"},
    {"name":"countScore10", "type":"INTEGER", "description":"1.0점"},
    {"name":"countScore15", "type":"INTEGER", "description":"1.5점"},
    {"name":"countScore20", "type":"INTEGER", "description":"2.0점"},
    {"name":"countScore25", "type":"INTEGER", "description":"2.5점"},
    {"name":"countScore30", "type":"INTEGER", "description":"3.0점"},
    {"name":"countScore35", "type":"INTEGER", "description":"3.5점"},
    {"name":"countScore40", "type":"INTEGER", "description":"4.0점"},
    {"name":"countScore45", "type":"INTEGER", "description":"4.5점"},
    {"name":"countScore50", "type":"INTEGER", "description":"5.0점"},
    {"name":"latestEpisodeCreatedTime", "type":"DATETIME", "description":"마지막방영일시"}
])


LAFTEL_SEARCH_PARAMS = lambda genres=list(), tags=list(), sortType="rank", offset=0, size=60: {
    "sort": sortType,
    **({"genres": ','.join(genres)} if genres else {}),
    **({"tags": ','.join(tags)} if tags else {}),
    "viewable": "true",
    "offset": offset,
    "size": size,
}


LAFTEL_CATEGORY_QUERY = lambda: Query()

LAFTEL_SEARCH_QUERY = lambda: Query(
    Variable(name="query", type="[DICT]", desc="쿼리", arr_options=dict(drop_empty=False)),
    Variable(name="sortType", type="STRING", desc="정렬기준", default="rank"),
    *PAGE_RANGE_QUERY(size=None, pageSize=60, offset=0),
)

LAFTEL_STATISTICS_QUERY = lambda: Query(
    Variable(name="modelId", type="[STRING]", desc="ID"),
)


LAFTEL_CATEGORY_SCHEMA = lambda: Schema(
    Field(name="name", type="STRING", desc="이름", mode=NULLABLE, path=["name"]),
    Field(name="type", type="STRING", desc="타입", mode=NULLABLE, path=["type"]),
)

LAFTEL_SEARCH_SCHEMA = lambda: Schema(
    Field(name="modelId", type="STRING", desc="ID", mode=NULLIFY, path=["id"]),
    Field(name="rank", type="INTEGER", desc="순위", mode=INDEX),
    Field(name="title", type="STRING", desc="제목", mode=NULLABLE, path=["name"]),
    Field(name="imageUrl", type="STRING", desc="이미지주소", mode=NULLABLE, path=["images",0,"img_url"]),
    Field(name="isAdult", type="BOOLEAN", desc="성인이용가", mode=NULLABLE, path=["is_adult"]),
    Field(name="isEnding", type="BOOLEAN", desc="완결여부", mode=NULLABLE, path=["is_ending"]),
    Field(name="genres", type="STRING", desc="장르", mode=NULLABLE, path=["genres"], apply=Join(sep=',')),
    Field(name="rating", type="INTEGER", desc="평점", mode=NULLABLE, path=["rating"]),
    Field(name="latestEpisodeCreatedTime", type="DATETIME", desc="마지막방영일시", mode=NULLABLE, path=["latest_episode_created"], apply=(lambda x: str(x).split('.')[0])),
)

LAFTEL_STATISTICS_SCHEMA = lambda: Schema(
    Field(name="modelId", type="STRING", desc="ID", mode=QUERY, path=["modelId"]),
    Field(name="averageScore", type="FLOAT", desc="평균별점", mode=NULLABLE, path=["average_score"]),
    Field(name="countScore", type="INTEGER", desc="별점수", mode=NULLABLE, path=["count_score"]),
    Field(name="countScore05", type="INTEGER", desc="0.5점", mode=NULLABLE, path=["count_score_05"]),
    Field(name="countScore10", type="INTEGER", desc="1.0점", mode=NULLABLE, path=["count_score_10"]),
    Field(name="countScore15", type="INTEGER", desc="1.5점", mode=NULLABLE, path=["count_score_15"]),
    Field(name="countScore20", type="INTEGER", desc="2.0점", mode=NULLABLE, path=["count_score_20"]),
    Field(name="countScore25", type="INTEGER", desc="2.5점", mode=NULLABLE, path=["count_score_25"]),
    Field(name="countScore30", type="INTEGER", desc="3.0점", mode=NULLABLE, path=["count_score_30"]),
    Field(name="countScore35", type="INTEGER", desc="3.5점", mode=NULLABLE, path=["count_score_35"]),
    Field(name="countScore40", type="INTEGER", desc="4.0점", mode=NULLABLE, path=["count_score_40"]),
    Field(name="countScore45", type="INTEGER", desc="4.5점", mode=NULLABLE, path=["count_score_45"]),
    Field(name="countScore50", type="INTEGER", desc="5.0점", mode=NULLABLE, path=["count_score_50"]),
)


LAFTEL_CATEGORY_INFO = lambda: Info(
    query = LAFTEL_CATEGORY_QUERY(),
    category = LAFTEL_CATEGORY_SCHEMA(),
)

LAFTEL_SEARCH_INFO = lambda: Info(
    query = LAFTEL_SEARCH_QUERY(),
    search = LAFTEL_SEARCH_SCHEMA(),
)

LAFTEL_STATISTICS_INFO = lambda: Info(
    query = LAFTEL_STATISTICS_QUERY(),
    statistics = LAFTEL_STATISTICS_SCHEMA(),
)

LAFTEL_SEARCH_PLUS_INFO = lambda: PipelineInfo(
    query = LAFTEL_SEARCH_QUERY(),
    search = LAFTEL_SEARCH_SCHEMA(),
    statistics = LAFTEL_STATISTICS_SCHEMA(),
)
