class ConsentsModifier:
    harmonizedPaths = ["\\DCC Harmonized data set"]
    consent_paths = {
        "harmonized": ["\\_harmonized_consent\\"],
        "topmed": ["\\_topmed_consents\\"]
    }
    default_consents = {}

    @staticmethod
    def default_query_consents(query):
        ConsentsModifier.default_consents = {}
        temp = {}
        for path in ConsentsModifier.consent_paths["harmonized"]:
            if path in query._lstFilter.data:
                temp[path] = query._lstFilter.data[path]
        ConsentsModifier.default_consents["harmonized"] = temp
        temp = {}
        for path in ConsentsModifier.consent_paths["topmed"]:
            if path in query._lstFilter.data:
                temp[path] = query._lstFilter.data[path]
        ConsentsModifier.default_consents["topmed"] = temp
        return ConsentsModifier.default_consents

    @staticmethod
    def modify_query(query):
        keys = []
        keys = keys + list(query._lstSelect.data.keys())
        keys = keys + list(query._lstFilter.data.keys())
        keys = keys + list(query._lstAnyOf.data.keys())
        keys = keys + list(query._lstRequire.data.keys())
        keys = keys + list(query._lstCrossCntFields.data.keys())
        keys = list(set(keys))
        has_harmonized = False
        for path in ConsentsModifier.harmonizedPaths:
            for match in keys:
                if match.find(path) != -1:
                    has_harmonized = True
                    break
        if not has_harmonized:
            for path in ConsentsModifier.consent_paths["harmonized"]:
                if path in query._lstFilter.data:
                    del query._lstFilter.data[path]
        else:
            for path in query._default_query_consents["harmonized"]:
                if not path in query._lstFilter.data:
                    query._lstFilter.data[path] = query._default_query_consents["harmonized"][path]

        variant_filters = [k for k in query._lstFilter.data.keys() if query._lstFilter.data[k]['HpdsDataType'] == 'info']
        if len(variant_filters) == 0:
            for path in ConsentsModifier.consent_paths["topmed"]:
                if path in query._lstFilter.data:
                    del query._lstFilter.data[path]
        else:
            for path in query._default_query_consents["topmed"]:
                if not path in query._lstFilter.data:
                    query._lstFilter.data[path] = query._default_query_consents["topmed"][path]

        return query