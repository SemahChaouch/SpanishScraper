import requests
import pyodbc
from functools import lru_cache
import xmltodict
from multiprocessing import cpu_count , Process
@lru_cache(maxsize=None)     
def getCPV(listURL, code):
    listResponse = requests.get(listURL)
    listDict = xmltodict.parse(listResponse.content)
    i = 0
    while i < len(listDict['gc:CodeList']['SimpleCodeList']['Row'] ):
        currentObj = listDict['gc:CodeList']['SimpleCodeList']['Row'][i]
        if(currentObj['Value'][0]['SimpleValue'] == code):
            return currentObj['Value'][2]['SimpleValue']
        i += 1
@lru_cache(maxsize=None)
def getListValue(listURL, code):
    listResponse = requests.get(listURL)
    listDict = xmltodict.parse(listResponse.content)
    i = 0
    while i < len(listDict['gc:CodeList']['SimpleCodeList']['Row'] ):
        currentObj = listDict['gc:CodeList']['SimpleCodeList']['Row'][i]
    if(currentObj['Value'][0]['SimpleValue'] == code):
        return currentObj['Value'][1]['SimpleValue']
    i += 1
cnxn=pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:test1233332.database.windows.net,1433;Database=scraper12;Uid=Holiso;Pwd=Sentokizaru1;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30')
cursor = cnxn.cursor()
Ads={'Anuncio Previo','EN PLAZO','PENDIENTE DE ADJUDICACION','ANUL','Anulada'}
Contracts={'Adjudicada','Resuelta'}
url = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"
while url:
    response = requests.get(url)
    data = xmltodict.parse(response.content)
    
  
