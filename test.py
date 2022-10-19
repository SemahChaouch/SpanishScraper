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
    for i in range(len(data["feed"]["entry"])):
        typ=None
        currentItem=data["feed"]["entry"][i]
        databaseRecord={}
        databaseRecord['I_EXT_ID'] =int(currentItem['id'].replace ("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", ""))
        b=currentItem['cac-place-ext:ContractFolderStatus']['cbc-place-ext:ContractFolderStatusCode']['#text']
        
        if b == 'ADJ' or b =='RES' :
            type = 'Contract'
        else :
                type ='Ad'
        cpvs=[]
        databaseRecord['CPV_COUNT']=0
        if 'cac:RequiredCommodityClassification' in currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']:
            if isinstance(currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:RequiredCommodityClassification'],list) :
                for i in range(len(currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:RequiredCommodityClassification'])):
                    databaseRecord["CPV_COUNT"]=databaseRecord["CPV_COUNT"]+1
                    cpvs.append(getCPV(currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:RequiredCommodityClassification'][i]['cbc:ItemClassificationCode']['@listURI'],currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:RequiredCommodityClassification'][i]['cbc:ItemClassificationCode']['#text'])+' '+ currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:RequiredCommodityClassification'][i]['cbc:ItemClassificationCode']['#text'])
                databaseRecord['MAIN_CPV']=';'.join(cpvs) 
            else :
                databaseRecord['MAIN_CPV']=getCPV(currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:RequiredCommodityClassification']['cbc:ItemClassificationCode']['@listURI'],currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:RequiredCommodityClassification']['cbc:ItemClassificationCode']['#text'])
                databaseRecord["CPV_COUNT"]=databaseRecord["CPV_COUNT"]+1
        else :
            databaseRecord['MAIN_CPV']=''
        print(databaseRecord['MAIN_CPV'],databaseRecord['CPV_COUNT'])
  
