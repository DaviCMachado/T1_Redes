import pandas as pd
import sys

def limpar_csv_arquivo(caminho_original, caminho_corrigido):
    linhas_validas = []
    
    # Limite máximo para o valor do timestamp (não mais necessário, pois vamos arredondar)
    LIMITE_TIMESTAMP = sys.maxsize  # O maior valor para um inteiro (dependendo do sistema)

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
                    
                    # Verificar se o timestamp tem milissegundos
                    partes_timestamp = timestamp.split('.')
                    if len(partes_timestamp) == 2:
                        # Arredondar a parte inteira para segundos (removendo a parte decimal)
                        timestamp_inteiro = int(partes_timestamp[0])
                        # Arredondar a parte decimal para o inteiro mais próximo
                        timestamp_arredondado = round(float(timestamp))
                        campos[0] = str(timestamp_arredondado)  # Substitui o timestamp original

                    # Adicionar a linha com o timestamp corrigido
                    linhas_validas.append(','.join(campos) + '\n')
                
                except (ValueError, IndexError, OverflowError):
                    # Caso o timestamp não seja válido, ignorar a linha
                    continue

    # Salvar as linhas válidas em um novo arquivo
    with open(caminho_corrigido, "w", encoding="utf-8") as destino:
        destino.writelines(linhas_validas)

    print(f"Arquivo filtrado e corrigido salvo em: {caminho_corrigido}")


def pegar_x_linhas(caminho_entrada, caminho_saida, n_topo=5000_000, n_base=200_000):
    # Lê as primeiras n_topo linhas
    df_topo = pd.read_csv(caminho_entrada, nrows=n_topo)
    
    # Copia as últimas n_base linhas
    df_base = df_topo.tail(n_base).copy()

    # Concatena os dois pedaços
    df_resultado = pd.concat([df_topo, df_base], ignore_index=True)

    # Cabeçalho fixo
    cabecalho_padrao = "timestamp,src_ip,dst_ip,protocol,length\n"

    # Salva o resultado com cabeçalho fixo
    with open(caminho_saida, "w", encoding="utf-8") as f:
        f.write(cabecalho_padrao)  # Escreve o cabeçalho manualmente
        df_resultado.to_csv(f, index=False, header=False, lineterminator='\n')  # Força o uso de '\n' como separador de linha

    print(f"Arquivo com {n_topo} linhas do topo e {n_base} do final salvo em: {caminho_saida}")

# Passos do processo
caminho_entrada = "data_limpo.csv"
caminho_filtrado = "data_limpo_filtrado.csv"
caminho_saida = "data_300k.csv"

# Filtrar o CSV original (caso queira usar)
# limpar_csv_arquivo(caminho_entrada, caminho_filtrado)

# Pegar as 300k linhas do arquivo filtrado
pegar_x_linhas(caminho_filtrado, caminho_saida)
