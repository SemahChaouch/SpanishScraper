from sqlite3 import DatabaseError
import requests
import time
import pyodbc 
import xmltodict
from multiprocessing import cpu_count , Process
def count(n):
    count=0
    while count<n :
        print(count)
        count = count +1
tab=[1,2,3,4,5,6,7,8]
def test(t,i):
    print(t[i])
url = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"

    
    

