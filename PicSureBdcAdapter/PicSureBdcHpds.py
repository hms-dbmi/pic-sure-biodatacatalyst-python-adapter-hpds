# -*- coding: utf-8 -*-

import PicSureClient
from PicSureHpdsLib.PicSureHpds import Adapter as HpdsAdapter
from PicSureHpdsLib.PicSureHpds import HpdsResourceConnection
from PicSureHpdsLib.PicSureHpdsQuery import Query as HpdsQuery
from .ConsentsModifier import ConsentsModifier
from .PicSureDictionary import PicSureDictionary
import pandas as pd
import json

class Adapter(HpdsAdapter):
    def __init__(self, PICSURE_network_URL, token):
        client = PicSureClient.Client()
        connection = client.connect(PICSURE_network_URL, token, False)
        HpdsAdapter.__init__(self, connection)

    def useOpenPicSure(self):
        return self.useResource("70c837be-5ffc-11eb-ae93-0242ac130002", False)
    
    def useAuthPicSure(self):
        return self.useResource("02e23f52-f354-4e8b-992c-d37c8b9ba140")
    
    def useDictionary(self):
        return self.useResource("36363664-6231-6134-2d38-6538652d3131")
    
    def useResource(self, resource_uuid, isAuth=True):
        uuid = resource_uuid
        if uuid is None and len(self.connection_reference.resource_uuids) == 1:
            uuid = self.connection_reference.resource_uuids[0]
        else:
            if uuid is None:
                # throw exception if a resource uuid wass not provided and more than 1 resource exists
                raise KeyError('Please specify a UUID, there is more than 1 resource.')

        if uuid in self.connection_reference.resource_uuids:
            return ResourceConnection(self.connection_reference, uuid, isAuth)
        else:
            raise KeyError('Resource UUID "'+uuid+'" was not found!')


class ResourceConnection(HpdsResourceConnection):
    def __init__(self, connection_reference, uuid, isAuth = True):
        self.isAuth = isAuth
        HpdsResourceConnection.__init__(self, connection_reference, uuid)
        
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
                if "\\_consents\\" in qt["categoryFilters"]:
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
        if self.isAuth :
            return AuthQuery(self, load_query)
        else :
            return OpenQuery(self, load_query)

    def dictionary(self):
        return PicSureDictionary(self)

# Temporarily override the help text to remove mention of getApproximateVariantCount & 
#   getVariantsDataFrame until they are ready for BDC env.
class BaseQuery(HpdsQuery):
    def help(self):
        print("""
        .select()       list of data fields to return from resource for each record
        .crosscounts()  list of data fields that cross counts will be calculated for
        .require()      list of data fields that must be present in all returned records
        .anyof()        list of data fields that records must be a member of at least one entry
        .studies()      list of studies that are selected that the query will run against
        .filter()       list of data fields and conditions that returned records satisfy
                  [ Filter keys exert an AND relationship on returned records      ]
                  [ Categorical values have an OR relationship on their key        ]
                  [ Numerical Ranges are inclusive of their start and end points   ]
        .getCount()             single count indicating the number of matching records
        .getCrossCounts()       array indicating number of matching records per cross-count keys
        .getResults()           CSV-like string containing the matching records
        .getResultsDataFrame()  pandas DataFrame containing the matching records...
                                  Params "asAsynch" and "timeout" are used by function, any
                                  additional named parameters are passed to pandas.read_csv()
        .getRunDetails()        details about the last run of the query
        .show()                 lists all current query parameters
        .save()                 returns the JSON-formatted query request as string
        .load(query)            set query's current criteria to those in given JSON string
            * getCount(), getResults(), and getResultsDataFrame() functions can also
              accept options that run queries differently which might help with
              connection timeouts. Example: .getResults(async=True, timeout=60)
        """)

class AuthQuery(BaseQuery):
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

class OpenQuery(BaseQuery):
    def __init__(self, conn, load_query):
        super().__init__(conn, load_query)
        self._default_query_consents = ConsentsModifier.default_query_consents(self)
    def help(self):
        print("""
        .crosscounts()  list of data fields that cross counts will be calculated for
        .require()      list of data fields that must be present in all returned records
        .anyof()        list of data fields that records must be a member of at least one entry
        .studies()      list of studies that are selected that the query will run against
        .filter()       list of data fields and conditions that returned records satisfy
                  [ Filter keys exert an AND relationship on returned records      ]
                  [ Categorical values have an OR relationship on their key        ]
                  [ Numerical Ranges are inclusive of their start and end points   ]

        .getCount()             single count indicating the number of matching records
        .getCrossCounts()       array indicating number of matching records per cross-count keys
        .getRunDetails()        details about the last run of the query
        .show()                 lists all current query parameters
        .save()                 returns the JSON-formatted query request as string
        .load(query)            set query's current criteria to those in given JSON string
        
        """)

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

