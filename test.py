import requests
import time
from functools import lru_cache
import xmltodict
from multiprocessing import cpu_count , Process
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
url = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"
response = requests.get(url)
data = xmltodict.parse(response.content)
for i in range(len(data["feed"]["entry"])):
    currentItem=data["feed"]["entry"][i]
    a=getListValue(currentItem['cac-place-ext:ContractFolderStatus']['cbc-place-ext:ContractFolderStatusCode']['@listURI'],currentItem['cac-place-ext:ContractFolderStatus']['cbc-place-ext:ContractFolderStatusCode']['#text']) 

            


    

