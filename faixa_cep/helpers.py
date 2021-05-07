from lxml.html import fromstring
import requests
import time
from itertools import cycle
import traceback
import random
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
import jsonlines

my_time_out = 20

def get_proxy_list():
    """Retorna uma lista de proxies supostamente válidos"""
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies


#Learn how to save the json to a buffer instead of the file system
def df_to_jsonl(data_frame: pd.DataFrame, UF: str):
    """Recebe um DataFrame, retorna um jsonl"""
    data_frame.to_json("tabela.json", orient = "records")
    with open("./tabela.json") as f:
        data = json.load(f)

    #Turn DataFrame into Jsonl
    with open(f'./{UF}.jsonl', 'w') as outfile:
        for entry in data:
            json.dump(entry, outfile)
            outfile.write('\n')


def clean_data(df: pd.DataFrame):
    """Recebe um DataFrame, retorna um DataFrame tratado da maneira adequada """
    df = df.drop_duplicates()
    df = df.dropna()
    return df

def make_post_request(post_fields, proxy):
    """Recebe dicionário com os post fields e um proxy válido, retora uma request"""
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "http://www.buscacep.correios.com.br",
    "Referer": "http://www.buscacep.correios.com.br/sistemas/buscacep/buscaFaixaCep.cfm",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
    }

    url = "http://www.buscacep.correios.com.br/sistemas/buscacep/resultadoBuscaFaixaCEP.cfm"

    print("")
    print("Trying to Make the post request.")
    request = requests.post(url = url, data = post_fields, headers = headers, proxies = {"http": proxy, "https": proxy}, timeout = my_time_out)
    return request


def request_text_to_table(request, page_content_index):
    """Recebe request, retorna uma tabela de faixas de CEP"""
    result = request.text
    soup = BeautifulSoup(result, 'html.parser')
    page_content = soup.find_all(name= 'table', class_ ='tmptabela')
    tabela_UF = page_content[page_content_index]
    return tabela_UF


def table_to_df(tabela_UF):
    """Recebe tabela, Retorna DataFrame"""
    logradouros = pd.read_html(str(tabela_UF),flavor= "bs4")
    page_table_data_frame = logradouros[0]
    return page_table_data_frame

def proxy_list_to_cycle():

    print("Tentando conseguir a proxy list")
    proxy_list_attempts = 0
            
    proxies = get_proxy_list()
    while len(proxies) == 0:
        proxies = get_proxy_list()
        print("oi")
        if proxy_list_attempts == 5:
            print("There was a problem retrieving the proxies List. Try again later.")
            raise Exception()

        print(f"Tentativa: {proxy_list_attempts}")
        proxy_list_attempts = proxy_list_attempts + 1 
        

    print(f"Proxy left: {proxies}")
    #Turn the proxy list into a cycle
    proxy_pool = cycle(proxies)

    return proxy_pool


def request_to_dataframe(UF):
    """Recebe string do estado, retona DataFrame com faixa de CEP do estado"""
 
    #Try to load the proxy list. If after several attempts it still doesn't work, raise an exception and quit.
    proxy_pool = proxy_list_to_cycle()
    
    #Set initial values for post request's parameters.
    pagini = 1
    pagfim = 50
    count = 1
    while True:
        #random sleep times to decrease the chances of being blocked.
        num1 = random.randint(2,5)
        time.sleep(num1)
        try: 
            #select_proxy from proxy pool.
            proxy = next(proxy_pool)
            print(f"Proxy atual: {proxy}")

            #Define o post Field de acordo com a página Atual. Para a primeira página os campos "Bairro", "qtdrow", "pagini", "pagfim" não são considerados.
            if count == 1:
                post_fields = {"UF":UF, "Localidade":""}
                full_dataframe = pd.DataFrame()
            else:
                post_fields = {"UF": UF, "Localidade":"**", "Bairro":"", "qtdrow":"50", "pagini":str(pagini),"pagfim": str(pagfim)}

            #Makes the post request
            request = make_post_request(post_fields, proxy)

            #Extrai tabela com as faixas de CEP do HTML. Se estivermos na primeira página, o conteúdo se encontra no primeiro index do page content, caso o contrário, se encontra no próximo index.
            if count == 1:
                UF_table = request_text_to_table(request = request, page_content_index = 1)
            else: 
                UF_table = request_text_to_table(request = request, page_content_index = 0)

        except requests.exceptions.ProxyError:
            print("")
            print(f"Error with the proxy: {proxy}")
            print(f"Proxies left: {proxy_pool}")
            print("Tentando novamente")
            print("")

            continue
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as err:
            print("")
            print('Servidor demorando muito')
            print("Tentando novamente")
            print("")
            continue

        except Exception as e:
            print("")
            print(e)
            proxy_pool = proxy_list_to_cycle()

            continue

        #Turning the table into a dataframe.
        current_page_df = table_to_df(UF_table)

        #Concat DataFrames for each page into one DataFrame
        full_dataframe = pd.concat([full_dataframe, current_page_df])

        print(f"Total de dados coletados sobre o Estado {UF}: {full_dataframe.shape[0]} ") 

        #Sair do loop de post requests para o estado atual se chegamos na última página.
        if current_page_df.shape[0] < 49:
            print(f"Última página do estado:{UF}")
            break

        #Incrementa o número da página e o contador de página. 
        pagini += 50
        pagfim += 50
        count = count + 1

    return full_dataframe

