from helpers import *

def buscar_faixa_cep(states):
  #recebe uma lista de estados, retorna um jsonl com as faixas de CEP de cada estado.

  for UF in states:
    print(f"Estado: {UF}")
    #From the request, return the data frame
    full_dataframe = request_to_dataframe(UF)

    #Clean DataFrame
    full_dataframe = clean_data(full_dataframe)

    #Turn DataFrame into Jsonl
    df_to_jsonl(full_dataframe, UF)

if __name__ == '__main__':
  states =  ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

  buscar_faixa_cep(states)

  # execute only if run as a script
