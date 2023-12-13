from typing import Union

from datapipelines import NotFoundError
from merakicommons.cache import lazy_property
from merakicommons.container import searchable

from ..data import Region
from .common import CoreData, CassiopeiaGhost, ghost_load_on
from ..dto.account import AccountDto

##############
# Data Types #
##############


class AccountData(CoreData):
    _dto_type = AccountDto
    _renamed = {}

##############
# Core Types #
##############


@searchable(
    {
        str: ["puuid", "gameName", "tagLine", ]
    }
)
class Account(CassiopeiaGhost):
    _data_types = {AccountData}

    def __init__(
            self,
            *,
            puuid: str = None,
            gameName: str = None,
            tagLine: str = None,
            region: Union[Region, str] = None
    ):
        kwargs = {"region": region}

        if puuid is not None:
            kwargs["puuid"] = puuid
        if gameName is not None:
            kwargs["gameName"] = gameName
        if tagLine is not None:
            kwargs["tagLine"] = tagLine
        super().__init__(**kwargs)

    @classmethod
    def __get_query_from_kwargs__(
            cls,
            *,
            puuid: str = None,
            gameName: str = None,
            tagLine: str = None,
            region: Union[Region, str]
    ) -> dict:
        query = {"region": region}

        if puuid is not None:
            query["puuid"] = puuid
        if gameName is not None:
            query["gameName"] = gameName
        if tagLine is not None:
            query["tagLine"] = tagLine

        return query

    def __get_query__(self):
        query = {"region": self.region, "platform": self.platform}

        try:
            query["puuid"] = self._data[AccountData].puuid
        except AttributeError:
            pass
        try:
            query["gameName"] = self._data[AccountData].gameName
        except AttributeError:
            pass
        try:
            query["tagLine"] = self._data[AccountData].tagLine
        except AttributeError:
            pass
        assert (
                ("name" in query and "tagLine" in query) or "puuid" in query
        )
        return query

    def __eq__(self, other: "Account"):
        if not isinstance(other, Account) or self.region != other.region:
            return False
        s = {}
        o = {}
        if hasattr(self._data[AccountData], "puuid"):
            s["puuid"] = self.puuid
        if hasattr(other._data[AccountData], "puuid"):
            o["puuid"] = other.puuid
        if hasattr(self._data[AccountData], "gameName"):
            s["gameName"] = self.gameName
        if hasattr(other._data[AccountData], "gameName"):
            o["gameName"] = other.gameName
        if hasattr(self._data[AccountData], "tagLine"):
            s["tagLine"] = self.tagLine
        if hasattr(other._data[AccountData], "tagLine"):
            o["tagLine"] = other.tagLine
        if any(s.get(key, "s") == o.get(key, "o") for key in s):
            return True
        else:
            return self.puuid == other.puuid

    def __str__(self):
        tagLine = "?"
        gameName = "?"
        puuid = "?"

        if hasattr(self._data[AccountData], "tagLine"):
            tagLine = self.tagLine
        if hasattr(self._data[AccountData], "gameName"):
            gameName = self.gameName
        if hasattr(self._data[AccountData], "puuid"):
            puuid = self.puuid

        return "Account(gameName='{gameName}', tagLine='{tagLine}' puuid='{puuid}')".format(
            gameName=gameName, tagLine=tagLine, puuid=puuid
        )

    @property
    def exists(self):
        try:
            if not self._Ghost__all_loaded:
                self.__load__()
            self.puuid  # Make sure we can access this attribute
            return True
        except (AttributeError, NotFoundError):
            return False

    @lazy_property
    def region(self) -> Region:
        """The region for this summoner."""
        return Region(self._data[AccountData].region)

    @CassiopeiaGhost.property(AccountData)
    @ghost_load_on
    def puuid(self) -> str:
        return self._data[AccountData].puuid

    @CassiopeiaGhost.property(AccountData)
    @ghost_load_on
    def gameName(self) -> str:
        return self._data[AccountData].gameName

    @CassiopeiaGhost.property(AccountData)
    @ghost_load_on
    def tagLine(self) -> str:
        return self._data[AccountData].tagLine

    @property
    def riot_id(self) -> str:
        return self.gameName + "#" + self.tagLine
