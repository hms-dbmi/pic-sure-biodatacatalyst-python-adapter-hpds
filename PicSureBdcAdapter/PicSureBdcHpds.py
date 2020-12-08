# -*- coding: utf-8 -*-

from PicSureHpdsLib.PicSureHpds import Adapter as HpdsAdapter
from PicSureHpdsLib.PicSureHpds import HpdsResourceConnection
from PicSureHpdsLib.PicSureHpdsQuery import Query as HpdsQuery
from .ConsentsModifier import ConsentsModifier
import pandas as pd
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
    def list_consents(self):
        if "queryTemplate" in self._profile_info:
            if(self._profile_info["queryTemplate"] is None):
                # Set to empty query if template from profile is null
                self._consents = pd.DataFrame({"consent":[], "harmonized":[], "topmed":[]})
            elif len(str(self._profile_info["queryTemplate"])) > 0:
                qt = json.loads(self._profile_info["queryTemplate"])

                harmonized = []
                harmonized_yn = []
                for key in ConsentsModifier.consent_paths["harmonized"]:
                    if key in qt["categoryFilters"]:
                        harmonized = harmonized + qt["categoryFilters"][key]
                harmonized = list(set(harmonized))

                topmed = []
                topmed_yn = []
                for key in ConsentsModifier.consent_paths["topmed"]:
                    if key in qt["categoryFilters"]:
                        topmed = topmed + qt["categoryFilters"][key]
                topmed = list(set(topmed))


                all_consents = list(set(harmonized + topmed))
                if ["\\_consents\\"] in qt["categoryFilters"]:
                    all_consents = list(set(all_consents + qt["categoryFilters"]["\\_consents\\"]))
                for key in all_consents:
                    if key in harmonized:
                        harmonized_yn.append("Y")
                    else:
                        harmonized_yn.append("N")
                    if key in topmed:
                        topmed_yn.append("Y")
                    else:
                        topmed_yn.append("N")

                self._consents = pd.DataFrame({"consent":all_consents, "harmonized":harmonized_yn, "topmed":topmed_yn})
            return self._consents

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
        self._default_query_consents = ConsentsModifier.default_query_consents(self)
    def save(self):
        ConsentsModifier.modify_query(self)
        return super().save()
    def show(self):
        ConsentsModifier.modify_query(self)
        return super().show()
    def buildQuery(self, *args):
        ConsentsModifier.modify_query(self)
        return super().buildQuery(*args)
    def getCount(self, asAsync=False, timeout=30):
        ConsentsModifier.modify_query(self)
        return super().getCount(asAsync, timeout)
    def getCrossCounts(self, asAsync=False):
        ConsentsModifier.modify_query(self)
        return super().getCrossCounts(asAsync)
    def getResults(self, asAsync=False, timeout=30):
        ConsentsModifier.modify_query(self)
        return super().getResults(asAsync, timeout)
    def getResultsDataFrame(self, asAsync=False, timeout=30, **kwargs):
        ConsentsModifier.modify_query(self)
        return super().getResultsDataFrame(asAsync, timeout, **kwargs)
