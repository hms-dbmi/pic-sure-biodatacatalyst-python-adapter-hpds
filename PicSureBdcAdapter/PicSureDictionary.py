# -*- coding: utf-8 -*-
import json
import re
import pandas as pd
from .DictionaryResult import DictionaryResult

class PicSureDictionary:
    """ Main class of library """
    def __init__(self, refHpdsResourceConnection):
        self._refResourceConnection = refHpdsResourceConnection
        self.resourceUUID = refHpdsResourceConnection.resource_uuid
        self._apiObj = refHpdsResourceConnection.connection_reference._api_obj()
        # deal with queryScopes from the PSAMA profile function
        if "queryScopes" in refHpdsResourceConnection._profile_info:
            self._profile_queryScopes = refHpdsResourceConnection._profile_info["queryScopes"]
        else:
            self._profile_queryScopes = []    
        r = re.compile("\\\\.*\\\\")
        def stripSlashes(topLevelPath):
            return topLevelPath.replace("\\","")
        self._included_studies = list(map(stripSlashes, filter(r.match, self._profile_queryScopes)))

    def help(self):
        print ("""
            [HELP] PicSureBdcAdapter.Adapter(url, token).useDictionary().dictionary()
            %s
            %s
        """ % (
            self.find.__doc__, 
            self.genotype_annotations.__doc__, 
        ))

    def find(self, term=None):
        if term == None:
            query = {"query":{"searchTerm":"","includedTags":[],"excludedTags":[],"returnTags":"true","offset":0,"limit":10000000}}
        else:
            query = {"query":{"searchTerm":str(term),"includedTags":[],"excludedTags":[],"returnTags":"true","offset":0,"limit":10000000}}
        results = json.loads(self._apiObj.search(self.resourceUUID, json.dumps(query)))
        def isInScope(result):
            return result['result']['studyId'].split('.')[0] in self._included_studies
        results['results']['searchResults'] = list(filter(isInScope, results['results']['searchResults']))
        return  DictionaryResult(results)
    find.__doc__ = """
    .find()                 Lists all phenotypic data dictionary entries
    .find(search_string)    Lists all phenotypic data dictionary entries that match the search string"""
