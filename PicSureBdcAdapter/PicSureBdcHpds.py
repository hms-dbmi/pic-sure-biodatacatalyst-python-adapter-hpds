# -*- coding: utf-8 -*-

from PicSureHpdsLib.PicSureHpds import Adapter as HpdsAdapter
from PicSureHpdsLib.PicSureHpds import HpdsResourceConnection
from PicSureHpdsLib.PicSureHpdsQuery import Query as HpdsQuery
from .QueryModifier import QueryModifier
import json

class Adapter(HpdsAdapter):
    def __init__(self, connection):
        HpdsAdapter.__init__(self, connection)

    def useResource(self, resource_uuid):
        uuid = resource_uuid
        if uuid is None and len(self.connection_reference.resource_uuids) == 1:
            uuid = self.connection_reference.resource_uuids[0]
        else:
            if uuid is None:
                # throw exception if a resource uuid wass not provided and more than 1 resource exists
                raise KeyError('Please specify a UUID, there is more than 1 resource.')

        if uuid in self.connection_reference.resource_uuids:
            return ResourceConnection(self.connection_reference, uuid)
        else:
            raise KeyError('Resource UUID "'+uuid+'" was not found!')


class ResourceConnection(HpdsResourceConnection):
    def query(self, load_query=None):
        if "queryTemplate" in self._profile_info and load_query is None:
            if(self._profile_info["queryTemplate"] is None):
                # Set to empty query if template from profile is null
                load_query = '{}'
            if len(str(self._profile_info["queryTemplate"])) > 0:
                # Set to queryTemplate if it exists in the psama profile
                load_query = self._profile_info["queryTemplate"]
        else:
            # If query template does not exist in profile then make an empty load query
            # Do this to to avoid null exceptions
            load_query = '{}'
        return Query(self, load_query)


class Query(HpdsQuery):
    def __init__(self, conn, load_query):
        super().__init__(conn, load_query)
        self._default_query_consents = QueryModifier.default_query_consents(self)
    def save(self):
        QueryModifier.modify_query(self)
        return super().save()
    def show(self):
        QueryModifier.modify_query(self)
        return super().show()
    def buildQuery(self, *args):
        QueryModifier.modify_query(self)
        return super().buildQuery(*args)
    def getCount(self, asAsync=False, timeout=30):
        QueryModifier.modify_query(self)
        return super().getCount(asAsync, timeout)
    def getCrossCounts(self, asAsync=False):
        QueryModifier.modify_query(self)
        return super().getCrossCounts(asAsync)
    def getResults(self, asAsync=False, timeout=30):
        QueryModifier.modify_query(self)
        return super().getResults(asAsync, timeout)
    def getResultsDataFrame(self, asAsync=False, timeout=30, **kwargs):
        QueryModifier.modify_query(self)
        return super().getResultsDataFrame(asAsync, timeout, **kwargs)
