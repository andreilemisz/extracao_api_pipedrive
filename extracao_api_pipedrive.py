"""
Esse programa faz uma busca na API do Pipedrive, extrai as informações das Deals, filtra apenas as colunas que a gente estabeleceu nas variáveis, depois remove todas as linhas cujo status seja "perdido", e salva tudo em um arquivo .csv 
"""
import os
import requests
import json
import csv
import pandas as pd

###########################
# Atenção: 
# Sempre verificar qual o nome dos cabeçalhos para substituir nas configurações abaixo
# Também pode desativar funções específicas no final dependendo se já tiver JSON ou CSV pronto
###########################

""" Variáveis globais. Alterar para funcionar com o relatório emitido pela API """

# Primeiro as variáveis da API
API_Token = ""
API_Dados = {
    "api_token": API_Token,
    "limit": 500
    }
# É a ID da primeira linha que vai ser consultada, depois ela aumenta para cada página que for lida
start = 0
# Para evitar repetir o cabeçalho várias vezes durante a passagem pro CSV, assim fica 1 vez só
ja_tem_cabecalho = False

# Caminho onde está o JSON pronto, se for o caso. Não é necessário preencher se for fazer a request da API automaticamente
caminho_json = 'any.json' 
# O próximo começa vazio. Vai ser preenchido se fizer a consulta na API ou se fizer o load padrão da execução do programa. Se fizer o load automatico do programa, ele vai puxar o JSON que estiver no caminho da variavel "caminho_json" que está estabelecida acima
dados_em_json = None
# Local onde será criado o arquivo .CSV, que será usado para carregar as funções de filtragem. Se não for fazer a conversão de JSON para CSV e já tiver a tabela pronta, basta alterar o caminho dela aqui
caminho_csv = "any.csv"
# Esse vai ser o arquivo final, com todos os filtros aplicados
caminho_csv_limpo = "any.csv"

# Cria automaticamente um arquivo .csv que vai ser preenchido pela função. Se ele já existir, o programa apaga ele pra não dar problema no futuro
if not os.path.exists(caminho_csv):
    with open(caminho_csv, 'w', newline="", encoding="utf-8"):
        pass
else:
    os.remove(caminho_csv)

# Segundo passo, variáveis da transformação do JSON em CSV
# Para novos valores, precisa adicionar nessa variável e dentro da função arquivo_json_para_csv nos dois momentos perto do fim
# Esse lugar é o nome do cabeçalho das colunas, então pode ser renomeado do jeito que quiser, desde que apareca lá no fim vinculado com alguma variável de valor
valores_a_buscar = ["any"]

def extracao_dados_api(start, ja_tem_cabecalho): 
    """ Apenas ativar essa função se precisar extrair o JSON direto da API da discadora """
    
    # A URL tem que ficar aqui dentro para pegar o valor mais atualizado do start
    API_URL = f"https://app.pipedrive.com/v1/deals?start={start}"

    # Impedir o programa de continuar se bater o limite das linhas consultadas da API
    if start is False:
        print("Criação da tabela finalizada.")
    
    else:

        # Comando GET para pegar os dados da API
        resultado_extracao = requests.get(API_URL, params=API_Dados)
        dados_em_json = resultado_extracao.json()

        # Criando o arquivo JSON com o que foi extraído da request
        with open(caminho_json, 'w', encoding='utf-8') as f:
            json.dump(dados_em_json, f, ensure_ascii=False, indent=4)

        print(f"Extrai da API a página {start}")
        verificar_paginacao(start, dados_em_json, ja_tem_cabecalho)

        # O fim dessa função é a extração em JSON da consulta na API da discadora
        # O JSON está salvo na variável "dados_em_json" para ser usado nas próximas etapas

def verificar_paginacao(start, dados_em_json, ja_tem_cabecalho):
    """ Função que busca na variável específica do JSON se ele aponta para a existência de mais uma página além daquela que está sendo exibida """

    busca_paginacao = dados_em_json.get('additional_data', []).get("pagination", [])
    if busca_paginacao.get("more_items_in_collection", False) == False:
        print("Acabou a paginação.")
        paginacao_finalizada = True
        arquivo_json_para_csv(start, dados_em_json, paginacao_finalizada, ja_tem_cabecalho)
    else:
        print("Ainda tem paginação. Vou aumentar a consulta em 500 pontos.")
        start += 500
        arquivo_json_para_csv(start, dados_em_json, ja_tem_cabecalho)

def arquivo_json_para_csv(start, dados_em_json, ja_tem_cabecalho, paginacao_finalizada=None):
    """ Essa função pega o arquivo JSON no argumento 1 (dados_em_json) e transforma ele em um arquivo .CSV com cabeçalho e os valores específicos das linhas que a gente selecionar nas variáveis gerais e no final da função nos campos específicos """

    # A categoria 'data' é uma lista dentro do retorno em JSON e é onde estão os valores que a gente precisa, por isso precisa fazer essa busca primeiro
    dados_na_categoria_correta = dados_em_json.get('data', [])
    with open(caminho_csv, 'a', newline="", encoding="utf-8") as arquivocsv:
        cabecalhos = valores_a_buscar
        funcao_escritor = csv.DictWriter(arquivocsv, fieldnames=cabecalhos)
        if ja_tem_cabecalho == False:
            funcao_escritor.writeheader()
            ja_tem_cabecalho = True


        """ Aqui é preciso escrever cada valor que for puxado para coluna """
        for linha in dados_na_categoria_correta:
            any_valor = linha.get("any", None)
            
            # Modelo
                # _valor = linha.get("", None)

            """ Mesma coisa aqui, para cada coluna a ser adicionada tem que adicionar a sintaxe abaixo nos dois lugares """
            if any_valor is not None:
                funcao_escritor.writerow({'any': any_valor})
             
    if paginacao_finalizada is True:
        print("Arquivo .csv terminado! Começando a limpeza dos processos perdidos.")
        limpeza_csv()
    else:
        print(f"Vou extrair a API novamente só que agora com a start em {start}")
        extracao_dados_api(start, ja_tem_cabecalho)

    # O fim dessa função é a criação de uma planilha CSV criada com cabeçalhos e os valores que a gente decidiu filtrar

def limpeza_csv():
    """ Remove as linhas cujo status seja 'lost' """

    df = pd.read_csv(caminho_csv)
    df_limpo = df[df["status_atual"] != "lost"]
    df_limpo.to_csv(caminho_csv_limpo, index=False)
    print(f"CSV limpo terminado. O arquivo se encontra em {caminho_csv_limpo}.")

###########################
""" Execução do Programa """

""" Programar quais operações são necessárias, na ordem """

if __name__ == "__main__":

    extracao_dados_api(start, ja_tem_cabecalho)
        # O resultado dessa primeira função é que a variavel "dados_em_json" contem a JSON da consulta feita
    # if not os.path.exists(caminho_json):
    #     extracao_dados_api(start)

    #     # Para o próximo passo, é preciso que a variável 'dados_em_json' esteja com um arquivo JSON já lido. Isso já está certo se extracao_dados_api() for usado antes. Se não for o caso, ele ativa a leitura do JSON que está na variável "caminho_json" que é configurada no cabeçalho do programa
    # if dados_em_json == None:
    #     with open(caminho_json, 'r', encoding="utf-8") as json_aberto:
    #         dados_em_json = json.load(json_aberto)

    #     # Terceiro passo é transformar o arquivo JSON que está em "dados_em_json" para uma tabela CSV
    # arquivo_json_para_csv(dados_em_json)

# Fim do programa