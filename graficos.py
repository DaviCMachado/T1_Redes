import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import dataProcessing
import json
from collections import Counter

plt.style.use('seaborn-v0_8-darkgrid')

def gerar_graficos(stats):
    for pasta in ["img/barras", "img/tempo", "img/heatmap", "img/boxplots", "img/scatter"]:
        os.makedirs(pasta, exist_ok=True)

    # Gráfico Scatter: Tamanho x Frequência
    if "scatter_tamanho_frequencia" in stats:
        gerar_scatter_tamanho_frequencia(stats["scatter_tamanho_frequencia"],
                                     "Dispersão: Tamanho x IPG",
                                     "img/scatter/tamanho_frequencia.png")


    if "entropia_ips_origem" in stats:
        gerar_barra({"Entropia IPs Origem": stats["entropia_ips_origem"]}, 
                    "Entropia da Distribuição dos IPs de Origem", 
                    "img/barras/entropia_ips_origem.png")

    if "top_ips_origem" in stats:
        gerar_barra(stats["top_ips_origem"], "Top IPs de Origem", "img/barras/ip_origem.png")

    if "top_ips_destino" in stats:
        gerar_barra(stats["top_ips_destino"], "Top IPs de Destino", "img/barras/ip_destino.png")

    if "pacotes_por_tempo" in stats:
        if not isinstance(stats["pacotes_por_tempo"], pd.DataFrame):
            if isinstance(stats["pacotes_por_tempo"], dict):
                stats["pacotes_por_tempo"] = pd.DataFrame(list(stats["pacotes_por_tempo"].items()), columns=["timestamp", "count"])
            elif isinstance(stats["pacotes_por_tempo"], list):
                stats["pacotes_por_tempo"] = pd.DataFrame(stats["pacotes_por_tempo"], columns=["timestamp", "count"])
        
        stats["pacotes_por_tempo"]['timestamp'] = pd.to_datetime(stats["pacotes_por_tempo"]['timestamp'], errors='coerce')
        stats["pacotes_por_tempo"].set_index('timestamp', inplace=True)
        gerar_tempo(stats["pacotes_por_tempo"], "Pacotes ao Longo do Tempo", "img/tempo/pacotes_tempo.png")

    if "heatmap_ips_tempo" in stats:
        gerar_heatmap_ips_ativos(stats["heatmap_ips_tempo"]["matriz"],
                                 stats["heatmap_ips_tempo"]["ips"],
                                 stats["heatmap_ips_tempo"]["tempos"],
                                 "Mapa de Calor dos IPs mais Ativos",
                                 "img/heatmap/ips_ativos_tempo.png")

    if "ipg_por_ip" in stats:
        gerar_boxplot_ipg_por_ip(stats["ipg_por_ip"], "IPG por IP", "img/boxplots/ipg_por_ip.png")

def salvar_figura(caminho):
    try:
        plt.tight_layout()
        plt.savefig(caminho)
    except Exception as e:
        print(f"Erro ao salvar o gráfico em {caminho}: {e}")
    finally:
        plt.close()

def gerar_scatter_tamanho_frequencia(df, titulo, caminho):
    if df.empty or "tamanho" not in df.columns or "timestamp" not in df.columns:
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp', 'tamanho'])

    # Calcular IPG (diferença de tempo entre pacotes consecutivos)
    df = df.sort_values('timestamp')
    df['ipg'] = df['timestamp'].diff().dt.total_seconds()
    df = df.dropna(subset=['ipg'])

    plt.figure(figsize=(10, 6))
    plt.scatter(df['tamanho'], df['ipg'], alpha=0.5)
    plt.xlabel("Tamanho do Pacote (bytes)")
    plt.ylabel("IPG (segundos)")
    plt.title(titulo)
    salvar_figura(caminho)


def gerar_barra(dados, titulo, caminho_saida):
    if not dados:
        return
    nomes = [str(k) for k in dados.keys()]
    valores = list(dados.values())

    plt.figure(figsize=(10, 6))
    plt.bar(nomes, valores, color='skyblue')
    plt.xlabel("Categorias")
    plt.ylabel("Frequência")
    plt.title(titulo)
    plt.xticks(rotation=45)
    salvar_figura(caminho_saida)

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
    plt.figure(figsize=(12, 8))
    sns.heatmap(matrix, xticklabels=tempos, yticklabels=ips, cmap='YlGnBu')
    plt.xlabel("Tempo")
    plt.ylabel("IPs")
    plt.title(titulo)
    salvar_figura(caminho)

def gerar_boxplot_ipg_por_ip(ipg_dict, titulo, caminho):
    if not ipg_dict:
        return
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=list(ipg_dict.values()))
    plt.xticks(ticks=range(len(ipg_dict)), labels=list(ipg_dict.keys()), rotation=45, ha='right')
    plt.ylabel("IPG (s)")
    plt.title(titulo)
    salvar_figura(caminho)

def calcular_atividade_ips(arquivo_csv):
    df = pd.read_csv(arquivo_csv)
    ips = list(df['ip_origem']) + list(df['ip_destino'])
    ip_count = Counter(ips)
    top_10_ips = ip_count.most_common(10)

    with open("top_10_ips.json", "w") as f:
        json.dump(top_10_ips, f, indent=4)

    return top_10_ips

if __name__ == "__main__":
    stats = dataProcessing.analisar_estatisticas("data_300k.csv")    
    gerar_graficos(stats)

    top_10_ips = stats.get("top_ips_origem", {})
    with open("top_10_ips.json", "w") as f:
        json.dump(top_10_ips, f, indent=4)
