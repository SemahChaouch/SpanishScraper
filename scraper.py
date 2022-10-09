from random import betavariate
from tkinter.tix import ROW
from urllib import request
import bs4
from bs4 import BeautifulSoup ;
import requests;
import pandas as pd;



html_text=requests.get("https://contrataciondelestado.es/wps/portal/!ut/p/b1/jc7JCsIwGATgZ_EB5P-zth7TLa3UBWKryUV6EKl0uYjPbyxeW53bwDcw4MCumcCAE8EFXMANzau9N892HJru0528RiYMVUQUarMjqGTKpKBItKEeWA8Ei3m9rY_SFBqxyLOkrIhATeV_e5yJwl_7M7iJ8PQQx1lOMTQsQVomVSVzXzX9gqWLE1j4sM_H_gbWs2D2CxVwAqsi6F2XbYoHb9Rq9QbIwaQO/dl4/d5/L2dBISEvZ0FBIS9nQSEh/pw/Z7_BS88AB1A0GSM10A6E365201G25/act/id=0/p=ACTION_NAME_PARAM=SourceAction/p=idLicitacion=767809735/523817407399/-/").text
#html_text=requests.get("https://contrataciondelestado.es/wps/portal/!ut/p/b1/jY_LCsIwFES_xQ-Q3CTNJS7TR5JKfUBotdlIFyKVPjbi99sWXRqd3cA5DEM8qdeIkjIQkpMz8UPzbG_Nox2Hppu7x0vspFQxVWDcjoLCjKNgQI1jE1BPgOBJVG2rI7rcAORWp0VJBRiG__nwJQoWP8oOSaItA-l4CqxIyxLtXD9-APixfyI-OGHYGwhdXIDAh70d-yvpfaf1Jr9HarV6AVGCtHo!/dl4/d5/L2dBISEvZ0FBIS9nQSEh/pw/Z7_BS88AB1A0GSM10A6E365201G25/act/id=0/p=ACTION_NAME_PARAM=SourceAction/p=idLicitacion=835023606/522166459905/-/").text
soup=BeautifulSoup(html_text,'lxml')
info=soup.findAll('div',class_="capaAtributos fondoAtributosDetalle")
col=[]
data=[]


#---- Tender Name -----
t=soup.find('ul',id='fila0').find('li',id="fila0_columna0")
count=1
for j in t :
    if j.text not in {'\n','\r\n\t\t\t\t\t\xa0\xa0\r\n\t\t\t\t\t'}:
        if count % 2 == 1:
            col.append(j.text)
        else :
            data.append(j.text)
        count=count+1



#----- General tender details ------
for i in (info[0].findAll('ul')):
    count=1
    for j in i :
        if type(j) is not bs4.element.NavigableString: 
            if count % 2 == 1:
                col.append(j.find('span').text)
            else :
                data.append(j.find('span').text)
            count=count+1




#----- Tender Informacion -----
for i in (info[1].findAll('ul')):
    count=1
    for j in i :
        if type(j) is not bs4.element.NavigableString: 
            if count % 2 == 1:
                col.append(j.find('span').text)
            else :
                data.append(j.find('span').text)
            count=count+1

    
        


#----- Tender footer ------ 
tab=soup.find('table',summary=True)
rows=tab.findAll('tr',class_=True)
for row in rows:
    count=0
    for i in (row.findAll('div')):
        if count == 0 :
            data.append(i.text)
        if count == 1 :
            col.append(i.text)
            col.append(i.text+":XML")
        if count == 2 :
            for a in i.findAll('a',href=True):
                if a.text=="Xml":
                    data.append(a["href"].replace("amp;",""))
        count=count+1
    
col.pop()
df=pd.DataFrame(data,col,columns=['VALUE'])
df.to_csv("test.csv")



    


    

    
    





