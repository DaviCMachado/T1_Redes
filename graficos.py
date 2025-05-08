import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import dataProcessing
from collections import Counter
from matplotlib import dates as mdates

plt.style.use('seaborn-v0_8-darkgrid')

def gerar_graficos(stats):
    for pasta in ["img/barras", "img/tempo", "img/heatmap", "img/scatter"]:
        os.makedirs(pasta, exist_ok=True)

    df = pd.DataFrame(stats["relacao_tamanho_frequencia"], columns=["tamanho", "ipg"])

    gerar_scatter_tamanho_frequencia(df, caminho="img/scatter/tamanho_frequencia.png")

    # gerar_barra(stats["top_ips_origem"], "Top IPs de Origem", "img/barras/ip_origem.png")

    gerar_barra(stats["top_ips_destino"], "Top IPs de Destino", "img/barras/ip_destino.png")

    if not isinstance(stats["pacotes_por_tempo"], pd.DataFrame):
        if isinstance(stats["pacotes_por_tempo"], dict):
            stats["pacotes_por_tempo"] = pd.DataFrame(list(stats["pacotes_por_tempo"].items()), columns=["timestamp", "count"])
        elif isinstance(stats["pacotes_por_tempo"], list):
            stats["pacotes_por_tempo"] = pd.DataFrame(stats["pacotes_por_tempo"], columns=["timestamp", "count"])
    
    stats["pacotes_por_tempo"]['timestamp'] = pd.to_datetime(stats["pacotes_por_tempo"]['timestamp'], errors='coerce')
    stats["pacotes_por_tempo"].set_index('timestamp', inplace=True)
    gerar_tempo(stats["pacotes_por_tempo"], "Pacotes ao Longo do Tempo", "img/tempo/pacotes_tempo.png")

    gerar_heatmap_ips_ativos(stats["heatmap_ips_tempo"]["matriz"],
                                 stats["heatmap_ips_tempo"]["ips"],
                                 stats["heatmap_ips_tempo"]["tempos"],
                                 "Mapa de Calor dos IPs mais Ativos",
                                 "img/heatmap/ips_ativos_tempo.png")

    gerar_trafego_agrupado_tempo(stats["trafego_por_minuto"],
                                     "Tráfego Agregado ao Longo do Tempo",
                                     "img/tempo/trafego_agregado_tempo.png")



def gerar_trafego_agrupado_tempo(trafego_dict, titulo, caminho):
    if not trafego_dict:
        return
        
    df = pd.DataFrame(list(trafego_dict.items()), columns=["timestamp", "bytes"])
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(inplace=True)
    df = df[df['timestamp'].dt.year == 2025]
    df.set_index('timestamp', inplace=True)
    df.sort_index(inplace=True)
    
    if df.empty:
        return
        
    # Resample por minuto e somar
    df_resampled = df.resample('1min').sum()
    
    # Calcular o valor acumulado (montante)
    df_resampled['bytes_acumulado'] = df_resampled['bytes'].cumsum()
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_resampled.index, df_resampled['bytes_acumulado'], 
             color='royalblue', linewidth=2, label='Tráfego Acumulado (Bytes)')
    
    plt.title(titulo, fontsize=16)
    plt.xlabel("Tempo (minutos)", fontsize=14)
    plt.ylabel("Volume de Tráfego Acumulado (Bytes)", fontsize=14)
    
    # Para este período curto (05:00-05:15), mostrar cada minuto
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 60, 1)))
    
    # Limitar o eixo X para mostrar apenas o período de interesse
    inicio = pd.Timestamp('2025-01-01 05:00:00')  # Usar uma data genérica de 2025
    fim = pd.Timestamp('2025-01-01 05:15:00')
    plt.xlim(inicio, fim)
    
    plt.xticks(rotation=45, ha='right', fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.tight_layout()
    salvar_figura(caminho)

def salvar_figura(caminho):
    try:
        plt.tight_layout()
        plt.savefig(caminho)
    except Exception as e:
        print(f"Erro ao salvar o gráfico em {caminho}: {e}")
    finally:
        plt.close()

def gerar_scatter_tamanho_frequencia(df, caminho):
    if df.empty or "tamanho" not in df.columns or "ipg" not in df.columns:
        return

    plt.figure(figsize=(10, 6))
    plt.scatter(df['ipg'], df['tamanho'], alpha=0.5, s=10, c='blue')
    plt.xlabel("Inter-Packet Gap (segundos)")
    plt.ylabel("Tamanho do Pacote (bytes)")
    plt.title("Relação entre IPG e Tamanho de Pacote")
    plt.grid(True)
    plt.tight_layout()
    salvar_figura(caminho)

def gerar_barra(dados_dict, titulo, caminho_saida):
    import matplotlib.pyplot as plt
    nomes = list(dados_dict.keys())
    valores = list(dados_dict.values())
    
    plt.figure(figsize=(10, 6))
    plt.bar(nomes, valores, color='skyblue')
    plt.title(titulo)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(caminho_saida)
    plt.close()


def gerar_tempo(stats, titulo, caminho_imagem):
    if stats.empty:
        return

    stats.index = pd.to_datetime(stats.index, errors='coerce')
    stats = stats.dropna()
    
    if stats.empty:
        return

    min_date = pd.Timestamp('0001-01-01')
    max_date = pd.Timestamp('9999-12-31')
    stats = stats[(stats.index >= min_date) & (stats.index <= max_date)]

    if stats.empty:
        return

    try:
        start_time = pd.Timestamp(stats.index.date[0]) + pd.Timedelta(hours=5)
        end_time = start_time + pd.Timedelta(minutes=15)
        stats_intervalo = stats[(stats.index >= start_time) & (stats.index <= end_time)]
        if stats_intervalo.empty:
            stats_intervalo = stats
    except IndexError:
        stats_intervalo = stats

    if stats_intervalo.empty:
        return

    stats_aggregated = stats_intervalo.resample('min').sum()

    plt.figure(figsize=(10, 6))
    plt.plot(stats_aggregated.index, stats_aggregated.values, label=titulo)
    plt.title(titulo)
    plt.xlabel('Tempo')
    plt.ylabel('Quantidade de Pacotes')
    plt.xticks(rotation=45)
    salvar_figura(caminho_imagem)

def gerar_heatmap_ips_ativos(matrix, ips, tempos, titulo, caminho):
    if matrix is None or not ips or not tempos:
        return
    
    # Filtra os tempos para manter apenas os de 2025
    tempos_2025 = [tempo for tempo in tempos if pd.to_datetime(tempo).year == 2025]
    
    # Ordena os tempos de 2025 em ordem crescente
    tempos_2025 = sorted(tempos_2025, key=lambda x: pd.to_datetime(x))
    
    # Converter o defaultdict para um DataFrame
    matrix_df = pd.DataFrame.from_dict(matrix, orient='index').fillna(0)
    
    
    # Certifique-se de que ips e tempos_2025 sejam listas
    ips = list(ips)  # Converte para lista caso seja um set

    # Filtra a matriz e ordena com base nos tempos de 2025
    matrix_df = matrix_df.loc[ips, tempos_2025]  # Ordena pela lista de IPs e tempos de 2025


    plt.figure(figsize=(12, 8))
    sns.heatmap(matrix_df, xticklabels=tempos_2025, yticklabels=ips, cmap='YlGnBu')

    plt.xlabel("Tempo")
    plt.ylabel("IPs")
    plt.title(titulo)

    # Salvar a figura no caminho especificado
    salvar_figura(caminho)





def gerar_variacao_traffico(stats, janela_tempo=5, caminho_imagem="img/tempo/variacao_traffico.png"):
    # Verifique se 'pacotes_por_tempo' é um dicionário e converta-o para DataFrame
    if isinstance(stats["pacotes_por_tempo"], dict):
        pacotes_por_tempo = pd.DataFrame(list(stats["pacotes_por_tempo"].items()), columns=["timestamp", "count"])
    elif isinstance(stats["pacotes_por_tempo"], list):
        pacotes_por_tempo = pd.DataFrame(stats["pacotes_por_tempo"], columns=["timestamp", "count"])
    else:
        pacotes_por_tempo = stats["pacotes_por_tempo"]

    # Certifique-se de que o índice seja do tipo datetime
    pacotes_por_tempo['timestamp'] = pd.to_datetime(pacotes_por_tempo['timestamp'], errors='coerce')

    # Defina o timestamp como índice
    pacotes_por_tempo.set_index('timestamp', inplace=True)

    # Resample os dados para janelas de 5s
    pacotes_por_tempo_resampled = pacotes_por_tempo.resample(f'{janela_tempo}s').sum()

    # Plotar a variação de pacotes ao longo do tempo
    plt.figure(figsize=(10, 6))
    plt.plot(pacotes_por_tempo_resampled.index, pacotes_por_tempo_resampled['count'], label="Tráfego (Pacotes)")
    plt.xlabel('Tempo')
    plt.ylabel('Quantidade de Pacotes')
    plt.title(f'Variação de Tráfego ao Longo do Tempo ({janela_tempo}s)')
    plt.xticks(rotation=45)
    salvar_figura(caminho_imagem)



import graficos2
if __name__ == "__main__":
    stats = dataProcessing.analisar_estatisticas("data_300k.csv")    
    gerar_graficos(stats)
    graficos2.second_step()


