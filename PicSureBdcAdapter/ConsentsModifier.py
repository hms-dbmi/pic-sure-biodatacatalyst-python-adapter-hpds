class ConsentsModifier:
    harmonizedPaths = ["\\DCC Harmonized data set"]
    consent_paths = {
        "harmonized": ["\\_harmonized_consent\\"],
        "topmed": ["\\_topmed_consents\\"]
    }
    default_consents = {}

    @staticmethod
    def default_query_consents(query):
        default_consents = {}
        temp = {}
        for path in ConsentsModifier.consent_paths["harmonized"]:
            if path in query._lstFilter.data:
                temp[path] = query._lstFilter.data[path]
        default_consents["harmonized"] = temp
        temp = {}
        for path in ConsentsModifier.consent_paths["topmed"]:
            if path in query._lstFilter.data:
                temp[path] = query._lstFilter.data[path]
        default_consents["topmed"] = temp
        return default_consents

    @staticmethod
    def modify_query(query):
        keys = query._lstFilter.data.keys()
        has_harmonized = False
        for path in ConsentsModifier.harmonizedPaths:
            for match in keys:
                if path in match:
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