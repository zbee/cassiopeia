from typing import Type, TypeVar, MutableMapping, Any, Iterable
import urllib

from datapipelines import (
    DataSource,
    PipelineContext,
    Query,
    NotFoundError,
    validate_query,
)
from .common import RiotAPIService, APINotFoundError
from ...data import Region, Platform
from ...dto.account import AccountDto
from ..uniquekeys import convert_to_continent

T = TypeVar("T")


class AccountAPI(RiotAPIService):
    @DataSource.dispatch
    def get(
            self,
            type: Type[T],
            query: MutableMapping[str, Any],
            context: PipelineContext = None,
    ) -> T:
        pass

    @DataSource.dispatch
    def get_many(
            self,
            type: Type[T],
            query: MutableMapping[str, Any],
            context: PipelineContext = None,
    ) -> Iterable[T]:
        pass

    _validate_get_account_query = (
        Query.has("puuid")
        .as_(str)
        .or_("gameName")
        .as_(str)
        .and_("tagLine")
        .as_(str)
        .also.has("region")
        .as_(Region)
    )

    @get.register(AccountDto)
    @validate_query(_validate_get_account_query, convert_to_continent)
    def get_account(
            self, query: MutableMapping[str, Any], context: PipelineContext = None
    ) -> AccountDto:
        if "puuid" in query:
            url = ("https://{platform}.api.riotgames.com/riot/account/v1/accounts/by-puuid/{"
                   "puuid}").format(
                platform=query["continent"].value.lower(), puuid=query["puuid"]
            )
            endpoint = "accounts/by-puuid/puuid"
        elif "gameName" in query and "tagLine" in query:
            url = ("https://{platform}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{"
                   "gameName}/{tagLine}").format(
                platform=query["continent"].value.lower(),
                gameName=query["gameName"],
                tagLine=query["tagLine"]
            )
            endpoint = "accounts/by-riot-id/gameName/tagLine"
        else:
            endpoint = ""

        try:
            app_limiter, method_limiter = self._get_rate_limiter(
                Platform.from_region(query["region"]), endpoint
            )
            data = self._get(
                url, {}, app_limiter=app_limiter, method_limiter=method_limiter
            )
        except APINotFoundError as error:
            raise NotFoundError(str(error)) from error

        return AccountDto(**data)
