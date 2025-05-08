import pandas as pd


def limpar_csv_arquivo(caminho_original, caminho_corrigido):
    linhas_validas = []
    
    # Abrir o arquivo original para leitura
    with open(caminho_original, "r", encoding="utf-8") as origem:
        for i, linha in enumerate(origem):
            # Manter o cabeçalho ou linhas com 5 campos
            if i == 0 or linha.count(",") == 4:

                # Separar os campos e verificar o timestamp
                campos = linha.strip().split(',')
                try:
                    # Obter o timestamp
                    timestamp = campos[0]
                    
                    # Verificar se o timestamp contém apenas um ponto
                    if timestamp.count('.') == 1:
            
                    # Adicionar a linha se o timestamp for válido
                        linhas_validas.append(linha)
                
                except ValueError:
                    # Caso o timestamp não seja válido, ignorar a linha
                    continue

    # Salvar as linhas válidas em um novo arquivo
    with open(caminho_corrigido, "w", encoding="utf-8") as destino:
        destino.writelines(linhas_validas)

    print(f"Arquivo filtrado salvo em: {caminho_corrigido}")



def pegar_x_linhas(caminho_entrada, caminho_saida, n_topo=200_000, n_base=200_000):
    # Lê as primeiras n_topo linhas
    df_topo = pd.read_csv(caminho_entrada, nrows=n_topo)
    
    # Conta total de linhas do arquivo para calcular onde começa o trecho final
   # # total_linhas = sum(1 for _ in open(caminho_entrada)) - 1  # -1 para o cabeçalho

    # Lê as últimas n_base linhas (usando skiprows)
    # skip_linhas = total_linhas - n_base
    # df_base = pd.read_csv(caminho_entrada, skiprows=range(1, skip_linhas + 1))  # pula linhas, mas mantém o cabeçalho

    df_base = df_topo.copy()

    # Concatena os dois pedaços
    df_resultado = pd.concat([df_topo, df_base], ignore_index=True)

    # Salva o resultado
    df_resultado.to_csv(caminho_saida, index=False)
    print(f"Arquivo com {n_topo} linhas do topo e {n_base} do final salvo em: {caminho_saida}")



# Passos do processo
caminho_entrada = "bigData.csv"
caminho_filtrado = "data_limpo.csv"
caminho_saida = "data_300k.csv"

# Filtrar o CSV original
# limpar_csv_arquivo(caminho_entrada, caminho_filtrado)

# Pegar as 300k linhas do arquivo filtrado
pegar_x_linhas(caminho_filtrado, caminho_saida)
