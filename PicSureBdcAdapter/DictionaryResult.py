# -*- coding: utf-8 -*-
import json
from IPython.display import display, HTML
import pandas as pd

class DictionaryResult:
    """ Results of a dictionary search. """

    def help(self):
        print ("""
        
.count()            %s

.varInfo(HPDS_PATH) %s

.displayResults()   %s

.dataframe()        %s

.listPaths()        %s
        
        """ % (
            self.count.__doc__, 
            self.varInfo.__doc__, 
            self.displayResults.__doc__, 
            self.dataframe.__doc__,
            self.listPaths.__doc__
        ))
        

    def count(self):
        return len(self.results['results']['searchResults'])

    count.__doc__ = """
    Get the number of search results in this DictionaryResult object.
    """

    def varInfo(self, path):
        
        def __resultInfoString__(result):
            result = result['result']
            conceptPath = result['metadata']['HPDS_PATH'];
            infoString = "<hr>" + conceptPath.replace('\\','\\\\') + '<hr>'
            for key in result:
                element = result[key]
                elementType = type(element)
                if(element):
                    if(elementType is dict):
                        infoString += key + ' : <br><table>'
                        for key2 in element.keys():
                            infoString += "<tr><td>" + str(key2) + "</td><td>" + str(element[key2]) + "</td></tr>"
                        infoString += "</table><br>"
                    elif(elementType is list):
                        infoString += "<p>" + key + ' : [ ' + ", ".join(element) + " ]</p><br>"
                    else:
                        infoString += key + " : " + str( result[key] ) + "<br>"
            return infoString

        for result in self.results['results']['searchResults']:
            resultEntry = result['result']
            conceptPath = resultEntry['metadata']['HPDS_PATH']
            if( path == conceptPath ):
                display( HTML( __resultInfoString__(result) ) )
                return 
        display("No result found")
        return
    
    varInfo.__doc__ = """
    Display all information available on a particular HPDS_PATH corresponding to a search result.
    """

    def displayResults(self):
        def __transform__(df):
            df['Concept Path'] = df['Concept Path'].apply(lambda x : x.replace('\\','\\\\'))
            return df
        results = map(self.__extractResult__, self.results['results']['searchResults'])
        resultsDF = pd.DataFrame.from_records(list(results),columns=['Concept Path', 'Group Description', 'Variable Description', 'Data Type', '<span style="text-align:left">Values</span>'])
        with pd.option_context('display.max_colwidth', None, 'display.max_rows', 300, 'display.max_columns', None,'display.expand_frame_repr', False):
            display(HTML( __transform__(resultsDF).to_html(escape=False).replace("\\n","<br>") ) )

    displayResults.__doc__ = """
    Display the search results in a truncated and formatted table form. 
    Use the varInfo function to get extended information about any variable.
    """

    def dataframe(self):
                
        def __resultToColumns__(result):
            record = result['result'].copy()
            metadata = record['metadata']
            del record['metadata']
            record.update(metadata)
            return record
        
        results = map(__resultToColumns__, self.results['results']['searchResults'])
        return pd.DataFrame.from_records(list(results))
        
    dataframe.__doc__ = """
    Flatten the results into a pandas dataframe.
    """

    def listPaths(self):
        results = map(self.__path__, map(self.__extractResult__, self.results['results']['searchResults']))
        listOfResults = list(results)
        listOfResults.sort()
        return listOfResults
        
    listPaths.__doc__ = """
    Extract the paths of all results into a list. This list can be added to the 
    select() or crosscounts() or anyof() lists of a query. For example:
        
        query.select().add(dictionaryResult.listPaths()
        query.getResultsDataFrame(low_memory=False)
        
    That would get you a dataframe with the values for all of the variables matching your search.
    """
        
    def __init__(self, results):
        self.results = results

    def __extractResult__(self, entry):    
        def __leftAlign__(value):
            return "<p style='text-align:left'>"+value+"</p>"

        def __ul__(values):
            list = "<ul style='text-align:left'>"
            for value in values:
                list = list + "<li>" + value + "</li>"
            return list + "</ul>"
            
        result = entry['result']
        metadata = result['metadata']
        dataType = ("categorical" if result['is_categorical'] else "continuous")
        values = (result['values'].values() if result['is_categorical'] else ["1 - 100"])
        conceptPath = result['metadata']['HPDS_PATH']
        return [conceptPath, __leftAlign__(metadata['dataTableDescription']), __leftAlign__(metadata['description']), dataType, __ul__(values)]

    def __path__(self, result):
        return result[0]
            

