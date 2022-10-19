from calendar import c
from enum import IntEnum
from pickletools import floatnl
import pyodbc
from functools import lru_cache
from re import T
import requests
import xmltodict
from multiprocessing import Process


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
        
Ads={'Anuncio Previo','EN PLAZO','PENDIENTE DE ADJUDICACION','ANUL','Anulada'}
Contracts={'Adjudicada','Resuelta'}


def addTender(ListTender):
    cnxn=pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:test1233332.database.windows.net,1433;Database=scraper12;Uid=Holiso;Pwd=Sentokizaru1;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30')
    cursor = cnxn.cursor()
    for i in range(len(ListTender)):
        type = None
        currentItem=ListTender[i]
#                  check if id exists in T_GW_SPAIN_ADS
        FolderStatus= getListValue(currentItem['cac-place-ext:ContractFolderStatus']['cbc-place-ext:ContractFolderStatusCode']['@listURI'],currentItem['cac-place-ext:ContractFolderStatus']['cbc-place-ext:ContractFolderStatusCode']['#text']) 
        if FolderStatus in Ads :
            type = 'Ad'
            cursor.execute(f"SELECT ID FROM T_GW_SPAIN_ADS WHERE ID=?",(currentItem['id'].replace("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", "")))
            result=cursor.fetchone()
            if result :
                print("Ad existant for ID = ",(currentItem['id'].replace ("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", ""))) 
                continue
        elif FolderStatus in Contracts :
            type= 'Contract'
            cursor.execute(f"SELECT ID FROM T_GW_SPAIN_CONTRACTS WHERE ID=?",(currentItem['id'].replace("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", "")))
            result=cursor.fetchone()
            if result :
                print("Contract already exist for ID = ",(currentItem['id'].replace ("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", ""))) 
                continue
        else :
            raise Exception("Unknown status")
       
#                 check if the number of Lots matches the number of awarded lots
        if type == 'Contract':
            if 'cac:TenderResult' not in currentItem['cac-place-ext:ContractFolderStatus'] :
                print("No Awarded lots yet for Folder ID = ",(currentItem['id'].replace ("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", ""))) 
                continue
            TenderResultCount=len(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'] ) if isinstance(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'],list) else 1
            if 'cac:ProcurementProjectLot' in currentItem['cac-place-ext:ContractFolderStatus']:
                if len(currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProjectLot']) != TenderResultCount:
                    print("Not enough of Awarded lots yet for Folder ID = ",(currentItem['id'].replace ("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", ""))) 
                    continue
            
        databaseRecord={}
        databaseRecord['I_EXT_ID'] =int(currentItem['id'].replace ("https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/", ""))
        databaseRecord['T_LINK'] = currentItem['link']['@href']
        databaseRecord['T_SUMMARY'] = currentItem['summary']['#text']
        #databaseRecord['T_TITLE'] = currentItem['title']
        databaseRecord['D_EXT_UPDATED_DATE'] = currentItem['updated']
        databaseRecord['T_CONTRACT_FOLDER_ID'] = currentItem['cac-place-ext:ContractFolderStatus']['cbc:ContractFolderID']
        #databaseRecord['T_CONTACT_FOLDER_STATUS'] = getListValue(currentItem['cac-place-ext:ContractFolderStatus']['cbc-place-ext:ContractFolderStatusCode']['@listURI'],currentItem['cac-place-ext:ContractFolderStatus']['cbc-place-ext:ContractFolderStatusCode']['#text'])
        

        MetaDataRecord={}
        MetaDataRecord['T_CONTRACTING_PARTY_TYPE'] = getListValue(currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cbc:ContractingPartyTypeCode']['@listURI'],currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cbc:ContractingPartyTypeCode']['#text']) 
        activities=[]
        if isinstance(currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cbc:ActivityCode'],list):
            for i in range(len(currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cbc:ActivityCode'])):
                currentActivityCode = currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cbc:ActivityCode'][i]
                activities.append(getListValue(currentActivityCode['@listURI'], currentActivityCode['#text']))
            MetaDataRecord['T_CONTRACTING_PARTY_ACTIVITY_CODE']=';'.join(activities) 
        else:
            currentActivityCode = currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cbc:ActivityCode']    
            MetaDataRecord['T_CONTRACTING_PARTY_ACTIVITY_CODE']  = getListValue(currentActivityCode['@listURI'], currentActivityCode['#text'])
        MetaDataRecord['T_CONTRACTING_PARTY_PROFILE_URL'] = currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cbc:BuyerProfileURIID']
        
    

        party=currentItem['cac-place-ext:ContractFolderStatus']['cac-place-ext:LocatedContractingParty']['cac:Party']
        databaseRecord["T_CONTRACTING_PARTY"]=party['cac:PartyName']['cbc:Name']+' ,'+party['cac:PostalAddress']['cbc:CityName'] +' '+party['cac:PostalAddress']['cac:Country']['cbc:Name']
        
        Nif=party["cac:PartyIdentification"][0]['cbc:ID']['#text']
        cursor.execute(f"SELECT T_NIF FROM T_GOVWISE_ENTITIES WHERE T_NIF=?",Nif)
        result = cursor.fetchone()
        if not result :
            cursor.execute(f"INSERT INTO T_GOVWISE_ENTITIES ([T_NIF],[T_NAME],[F_Buyer],[F_Supplier]) VALUES (?,?,?,?)",Nif,party['cac:PartyName']['cbc:Name']+' ,'+party['cac:PostalAddress']['cbc:CityName'] +' '+party['cac:PostalAddress']['cac:Country']['cbc:Name'],True,False)
            cursor.commit()
        #PROJECT TYPE
        cursor.execute(f"SELECT I_ENTITY FROM T_GOVWISE_ENTITIES WHERE T_NIF=?",Nif)
        id=cursor.fetchone()[0]
        for key in MetaDataRecord :
            cursor.execute(f"SELECT I_METADATA FROM T_GOVWISE_ENTITIES_METADATA WHERE T_NAME=?",key)
            if not result :
                cursor.execute(f"INSERT INTO T_GOVWISE_ENTITIES_METADATA ([E_ENTITY],[T_NAME],[T_VALUE]) VALUES (?,?,?)",id,key,MetaDataRecord[key])
                cursor.commit()
        databaseRecord['T_PROCUREMENT_PROJECT_TYPE'] = getListValue(currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cbc:TypeCode']['@listURI'], currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cbc:TypeCode']['#text'])
        


        cpvs=[]
        databaseRecord["CPV_COUNT"]=0
        #====================
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


        databaseRecord['T_PRICE']=currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProject']['cac:BudgetAmount']['cbc:TaxExclusiveAmount']['#text']

        #CRITERIA CHECK LIST
        criteria=[]
        if 'cac:AwardingTerms' in currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingTerms']:
            if isinstance(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingTerms']['cac:AwardingTerms']['cac:AwardingCriteria'],list):
                for i in range(len(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingTerms']['cac:AwardingTerms']['cac:AwardingCriteria'])):
                    criteria.append(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingTerms']['cac:AwardingTerms']['cac:AwardingCriteria'][i]['cbc:Description'])
                databaseRecord["AWARDING_CRITERIA"]=';'.join(criteria) 
            else:
                databaseRecord["AWARDING_CRITERIA"]=currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingTerms']['cac:AwardingTerms']['cac:AwardingCriteria']['cbc:Description']
        else :
            databaseRecord["AWARDING_CRITERIA"]=''
        #PROCEDURE
        databaseRecord["PROCEDURE"]=getListValue(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingProcess']['cbc:ProcedureCode']['@listURI'],currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingProcess']['cbc:ProcedureCode']['#text'])

        #DEADLINE
        if 'cac:TenderSubmissionDeadlinePeriod' in currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingProcess']:
            if 'cbc:EndDate' in currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingProcess']['cac:TenderSubmissionDeadlinePeriod']:
                databaseRecord["DEADLINE"]=currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderingProcess']['cac:TenderSubmissionDeadlinePeriod']['cbc:EndDate']
        else :
            databaseRecord["DEADLINE"]=''
        #LEGALDOCREFERENCE
        if 'cac:LegalDocumentReference' in currentItem['cac-place-ext:ContractFolderStatus']:
            databaseRecord["LEGAL_DOC_REF"]=currentItem['cac-place-ext:ContractFolderStatus']['cac:LegalDocumentReference']['cac:Attachment']['cac:ExternalReference']['cbc:URI']
        else:
            databaseRecord["LEGAL_DOC_REF"]=''
        #TECHNICALDOCREFERENCE
        if 'cac:TechnicalDocumentReference' in currentItem['cac-place-ext:ContractFolderStatus']:
            databaseRecord["TECH_DOC_REF"]=currentItem['cac-place-ext:ContractFolderStatus']['cac:TechnicalDocumentReference']['cac:Attachment']['cac:ExternalReference']['cbc:URI']
        else:
            databaseRecord["TECH_DOC_REF"]=''
        if type == 'Ad':
            databaseRecord.pop('DEADLINE')
            cursor.execute(f"INSERT INTO T_GW_SPAIN_ADS ( [id],[reference],[contractDesignation],[D_LastUpdate],[E_FolderID], [contractingFirstNif], [modelType], [cpvCount], [cpvFirst], [basePrice], [ambientCriteria], [deadline], [T_LegalDocLink], [T_TechDocLink]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", list(databaseRecord.values()))
            cursor.commit()
            print(' AD added ')
        else :
            databaseRecord.pop('DEADLINE')
            cursor.execute(f"INSERT INTO T_GW_SPAIN_CONTRACTS ( [id],[reference],[objectBriefDescription],[publicationDate],[E_FolderID], [contractingFirstNif], [contractFundamentationType], [cpvCount], [cpvFirst], [parsedPrice], [ambientCriteria], [ContractingProcedureType], [T_LegalDocLink], [T_TechDocLink]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", list(databaseRecord.values()))
            cursor.commit()
            print('CONTRACT added')
        #print(databaseRecord)
        if ('cac:ProcurementProjectLot' in currentItem['cac-place-ext:ContractFolderStatus']):
             for i in range(len(currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProjectLot'])):
                 lot=currentItem['cac-place-ext:ContractFolderStatus']['cac:ProcurementProjectLot'][i]
                 lotRecord={}
                 lotRecord['E_AdID']= databaseRecord['I_EXT_ID']
                 lotRecord['N_LotID']=lot['cbc:ID']['#text']
                 lotRecord['T_LotDescription']=lot['cac:ProcurementProject']['cbc:Name']
                 if type == 'Contract'and isinstance(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'],list) and 'cac:LegalMonetaryTotal' in currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'][i]:
                    lotRecord['N_LotPrice']=float(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'][i]['cac:AwardedTenderedProject']['cac:LegalMonetaryTotal']['cbc:PayableAmount']['#text'])
                    databaseRecord['T_PRICE']=databaseRecord['T_PRICE']+lotRecord['N_LotPrice']
                 else :
                     lotRecord['N_LotPrice']=float(lot['cac:ProcurementProject']['cac:BudgetAmount']['cbc:TotalAmount']['#text'])
                     databaseRecord['T_PRICE']=databaseRecord['T_PRICE']+lotRecord['N_LotPrice']
                 lotcpv=[]
                 if isinstance(lot['cac:ProcurementProject']['cac:RequiredCommodityClassification'],list) :
                     for j in range(len(lot['cac:ProcurementProject']['cac:RequiredCommodityClassification'])):
                         lotcpv.append(getCPV(lot['cac:ProcurementProject']['cac:RequiredCommodityClassification'][j]['cbc:ItemClassificationCode']['@listURI'],lot['cac:ProcurementProject']['cac:RequiredCommodityClassification'][j]['cbc:ItemClassificationCode']['#text'])+' '+ lot['cac:ProcurementProject']['cac:RequiredCommodityClassification'][j]['cbc:ItemClassificationCode']['#text'])
                     lotRecord['L_CPVs']=';'.join(lotcpv) 
                 else :
                     lotRecord['L_CPVs']=getCPV(lot['cac:ProcurementProject']['cac:RequiredCommodityClassification']['cbc:ItemClassificationCode']['@listURI'],lot['cac:ProcurementProject']['cac:RequiredCommodityClassification']['cbc:ItemClassificationCode']['#text'])+' '+lot['cac:ProcurementProject']['cac:RequiredCommodityClassification']['cbc:ItemClassificationCode']['#text']
                 if type=='Contract':
                    cursor.execute(f"INSERT INTO T_GW_SPAIN_CONTRACTS_LOTS ([E_ContractID],[N_LotID],[T_LotDescription],[N_LotPrice],[L_CPVS]) VALUES (?,?,?,?,?)", list(lotRecord.values()))
                    cursor.commit()
                    print('ADDED LOT')
                 else :
                    cursor.execute(f"INSERT INTO T_GW_SPAIN_ADS_LOTS ([E_AdID],[N_LotID],[T_LotDescription],[N_LotPrice],[L_CPVS]) VALUES (?,?,?,?,?)", list(lotRecord.values()))
                    cursor.commit()
                    print('ADDED LOT')



        if ('cac:TenderResult' in currentItem['cac-place-ext:ContractFolderStatus']):
            if isinstance(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'],list):
                 for i in range(len(currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'])):
                    TenderResult=currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult'][i]
                    cursor.execute(f"SELECT I_GW_LotID FROM T_GW_SPAIN_CONTRACTS_LOTS WHERE N_LotID = ? AND E_ContractID=? ", i+1,databaseRecord['I_EXT_ID'])
                    SupplierRecord={}
                    SupplierRecord['E_ContractID']= databaseRecord['I_EXT_ID']
                    SupplierRecord['E_LotID']=cursor.fetchone()[0]
                    SupplierRecord['T_SupplierVat']=TenderResult['cac:WinningParty']['cac:PartyIdentification']['cbc:ID']['#text']
                    SupplierRecord['T_SupplierName']=TenderResult['cac:WinningParty']['cac:PartyName']['cbc:Name']
                    cursor.execute(f"INSERT INTO T_GW_SPAIN_CONTRACTS_LOT_SUPPLIERS ([E_ContractID],[E_LotID],[T_SupplierVat],[T_SupplierName]) VALUES (?,?,?,?)", list(SupplierRecord.values())[0:4])
                    cursor.commit()
                    print('ADDED SUPPLIER LOT')
            else :
                TenderResult=currentItem['cac-place-ext:ContractFolderStatus']['cac:TenderResult']
                if 'cac:WinningParty' in TenderResult :
                    cursor.execute(f"INSERT INTO T_GW_SPAIN_CONTRACTS_LOTS ([E_ContractID],[N_LotID],[T_LotDescription],[N_LotPrice],[L_CPVS]) VALUES (?,?,?,?,?)",databaseRecord['I_EXT_ID'],1,currentItem['title'],float(databaseRecord['T_PRICE']) ,databaseRecord['MAIN_CPV'])
                    cursor.commit()
                    cursor.execute(f"SELECT I_GW_LotID FROM T_GW_SPAIN_CONTRACTS_LOTS WHERE N_LotID = ? AND E_ContractID=? ", 1,databaseRecord['I_EXT_ID'])
                    SupplierRecord={}
                    SupplierRecord['E_ContractID']= databaseRecord['I_EXT_ID']
                    SupplierRecord['E_LotID']=cursor.fetchone()[0]
                    SupplierRecord['T_SupplierVat']=TenderResult['cac:WinningParty']['cac:PartyIdentification']['cbc:ID']['#text']
                    SupplierRecord['T_SupplierName']=TenderResult['cac:WinningParty']['cac:PartyName']['cbc:Name']
                    cursor.execute(f"INSERT INTO T_GW_SPAIN_CONTRACTS_LOT_SUPPLIERS ([E_ContractID],[E_LotID],[T_SupplierVat],[T_SupplierName]) VALUES (?,?,?,?)", list(SupplierRecord.values())[0:4])
                    cursor.commit()
                    print('ADDED SUPPLIER LOT')

        




if __name__ == '__main__':   
    url = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"
    z=0
    while url :
        print('--------------------- NEW URL ----------------------')
        response = requests.get(url)
        data = xmltodict.parse(response.content)
        entries=data["feed"]["entry"]
        #divide entries into 4 lists
        entries1=entries[0::4]
        entries2=entries[1::4]
        entries3=entries[2::4]
        entries4=entries[3::4]
      

       
        #create 4 threads
        p1 = Process(target=addTender, args=(entries1,))
        p2 = Process(target=addTender, args=(entries2,))
        p3 = Process(target=addTender, args=(entries3,))
        p4 = Process(target=addTender, args=(entries4,))

       
        p1.start()
        p2 : p2.start()
        p3 : p3.start()
        p4 : p4.start()

        p1.join()
        p2 : p2.join()
        p3 : p3.join()
        p4 : p4.join()

        for i in range(len(data['feed']['link'])):
            if data['feed']['link'][i]['@rel']=='next':
                url=data['feed']['link'][i]['@href']
            else :
                url=''



