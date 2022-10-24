import requests
LISTING_URL = f"https://www.acingov.pt/acingovprod/2/zonaPublica/zona_publica_c/indexProcedimentosAjax/true"

payload = "procedure_search="
headers = {
  'Accept': 'text/html; charset=utf-8',
  'Accept-Language': 'pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
  'Connection': 'keep-alive',
  'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
  'Cookie': 'acingov=3c3pamfcvq4fh41db8m963gpbf; SERVERID=prod0046; SERVERID=prod0047; acingov=6qqlpvqn4u80qm6pgdp56nr422',
  'Origin': 'https://www.acingov.pt',
  'Referer': 'https://www.acingov.pt/acingovprod/2/zonaPublica/zona_publica_c/indexProcedimentos',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36' ,
  'X-Requested-With': 'XMLHttpRequest',
  'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"'
}
proxies ={
  'https://':'127.0.0.1:8899',
  'http://':'https://127.0.0.1:8899'
}


  
response = requests.request("POST", "https://www.acingov.pt/acingovprod/2/zonaPublica/zona_publica_c/indexProcedimentosAjax/true" , headers=headers,data=payload,proxies=proxies) 
print(response.json())
