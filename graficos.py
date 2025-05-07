import datetime
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
import numpy as np
import dataProcessing
import json
from collections import Counter

# Estilo global dos gráficos
plt.style.use('seaborn-v0_8-darkgrid')

def gerar_graficos(stats):
    for pasta in ["img/barras", "img/histograms", "img/pizza", "img/tempo", "img/heatmap", "img/boxplots"]:
        os.makedirs(pasta, exist_ok=True)

    # Gráficos de Barras
    if "protocolos" in stats:
        gerar_barra(stats["protocolos"], "Frequência de Protocolos", "img/barras/protocolos.png")

    if "top_ips_origem" in stats:
        gerar_barra(stats["top_ips_origem"], "Top IPs de Origem", "img/barras/ip_origem.png")

    if "top_ips_destino" in stats:
        gerar_barra(stats["top_ips_destino"], "Top IPs de Destino", "img/barras/ip_destino.png")

    # Histograma
    if "tamanhos" in stats:
        gerar_histograma(stats["tamanhos"], "Distribuição dos Tamanhos dos Pacotes", "img/histograms/tamanhos.png")

    # Pizza
    if "protocolos" in stats:
        gerar_pizza(stats["protocolos"], "Proporção de Protocolos", "img/pizza/protocolos_pizza.png")

    if "top_ips_origem" in stats:
        gerar_pizza(stats["top_ips_origem"], "Proporção dos Principais IPs de Origem", "img/pizza/ip_origem_pizza.png")

    if "top_ips_destino" in stats:
        gerar_pizza(stats["top_ips_destino"], "Proporção dos Principais IPs de Destino", "img/pizza/ip_destino_pizza.png")

    # Tempo
    if "pacotes_por_tempo" in stats:
        # Verifique se pacotes_por_tempo é um DataFrame
        if not isinstance(stats["pacotes_por_tempo"], pd.DataFrame):
            # Se for um dicionário de tempos e pacotes
            if isinstance(stats["pacotes_por_tempo"], dict):
                stats["pacotes_por_tempo"] = pd.DataFrame(list(stats["pacotes_por_tempo"].items()), columns=["timestamp", "count"])
            # Se for uma lista de tuplas (timestamp, count)
            elif isinstance(stats["pacotes_por_tempo"], list):
                stats["pacotes_por_tempo"] = pd.DataFrame(stats["pacotes_por_tempo"], columns=["timestamp", "count"])
        
        # Garantir que a coluna 'timestamp' seja convertida para datetime
        stats["pacotes_por_tempo"]['timestamp'] = pd.to_datetime(stats["pacotes_por_tempo"]['timestamp'], errors='coerce')

        # Defina o índice como o 'timestamp'
        stats["pacotes_por_tempo"].set_index('timestamp', inplace=True)

        # Agora você pode gerar o gráfico
        gerar_tempo(stats["pacotes_por_tempo"], "Pacotes ao Longo do Tempo", "img/tempo/pacotes_tempo.png")

    # Heatmaps
    if "matriz_correlacao" in stats:
        if isinstance(stats["matriz_correlacao"], pd.DataFrame):
            if not stats["matriz_correlacao"].empty and stats["matriz_correlacao"].shape[0] > 1:
                print(f"Gerando heatmap de correlação com matriz de dimensão {stats['matriz_correlacao'].shape}")
                gerar_heatmap(stats["matriz_correlacao"], "Correlação entre Variáveis", "img/heatmap/correlacao.png")
            else:
                print("Warning: Matriz de correlação vazia ou com dimensão insuficiente.")
        else:
            matriz_corr = pd.DataFrame(stats["matriz_correlacao"])
            if not matriz_corr.empty and matriz_corr.shape[0] > 1:
                print(f"Matriz de correlação convertida para DataFrame com dimensão {matriz_corr.shape}")
                gerar_heatmap(matriz_corr, "Correlação entre Variáveis", "img/heatmap/correlacao.png")
            else:
                print("Warning: Matriz de correlação convertida está vazia ou com dimensão insuficiente.")
         
    # Boxplot para IPG por IP
    if "ipg_por_ip" in stats:
        gerar_boxplot_ipg_por_ip(stats["ipg_por_ip"], "IPG por IP", "img/boxplots/ipg_por_ip.png")

    if "cdf_tamanhos" in stats:
        gerar_cdf(stats["cdf_tamanhos"], "CDF dos Tamanhos de Pacotes", "img/histograms/cdf_tamanhos.png")

    if "scatter_tamanho_frequencia" in stats:
        gerar_scatter_tamanho_frequencia(stats["scatter_tamanho_frequencia"], "Tamanho vs Frequência", "img/tempo/scatter.png")

    if "heatmap_ips_tempo" in stats:
        gerar_heatmap_ips_ativos(stats["heatmap_ips_tempo"]["matriz"],
                                 stats["heatmap_ips_tempo"]["ips"],
                                 stats["heatmap_ips_tempo"]["tempos"],
                                 "Mapa de Calor dos IPs mais Ativos",
                                 "img/heatmap/ips_ativos_tempo.png")

    if "protocolos_por_tempo" in stats:
        gerar_linhas_por_protocolo(stats["protocolos_por_tempo"], "Protocolos ao Longo do Tempo", "img/tempo/protocolos_tempo.png")

    if "protocolos_por_ip" in stats:
        gerar_barras_empilhadas(stats["protocolos_por_ip"], "Protocolos por IP (Empilhado)", "img/barras/protocolos_por_ip.png")

    if "tamanhos_por_protocolo" in stats:
        gerar_violin_tamanho_por_protocolo(stats["tamanhos_por_protocolo"], "Distribuição de Tamanhos por Protocolo", "img/barras/violin_tamanhos_protocolo.png")

    if "skewness_kurtosis_ipg" in stats:
        gerar_barra_dupla(stats["skewness_kurtosis_ipg"], "Skewness e Kurtosis do IPG", "img/barras/skew_kurt_ipg.png")

    if "horizontal_scan" in stats:
        gerar_barra(stats["horizontal_scan"], "Destinos Únicos por IP (Scan Horizontal)", "img/barras/horizontal_scan.png")

    if "ip_atividade" in stats:
        dados_ip_atividade = dict(stats["ip_atividade"])
        gerar_barra(dados_ip_atividade, "Top 10 IPs Mais Ativos", "img/barras/ip_atividade.png")


def salvar_figura(caminho):
    try:
        plt.tight_layout()
        plt.savefig(caminho)
    except Exception as e:
        print(f"Erro ao salvar o gráfico em {caminho}: {e}")
    finally:
        plt.close()


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

def gerar_barra_dupla(dados, titulo, caminho_saida):
    if not dados:
        return
    ips = list(dados.keys())
    skewness = [val["skewness"] for val in dados.values()]
    kurtosis = [val["kurtosis"] for val in dados.values()]

    x = np.arange(len(ips))
    width = 0.35

    plt.figure(figsize=(12, 6))
    plt.bar(x - width/2, skewness, width, label='Skewness', color='skyblue')
    plt.bar(x + width/2, kurtosis, width, label='Kurtosis', color='salmon')

    plt.xlabel('IPs')
    plt.ylabel('Valor')
    plt.title(titulo)
    plt.xticks(x, ips, rotation=45, ha='right')
    plt.legend()
    salvar_figura(caminho_saida)


def gerar_histograma(valores, titulo, caminho):
    if not valores:
        return
    plt.figure()
    plt.hist(valores, bins=20, color='orange')
    plt.title(titulo)
    plt.ylabel("Frequência")
    plt.xlabel("Tamanho (bytes)")
    salvar_figura(caminho)

def gerar_pizza(dados, titulo, caminho):
    if not dados:
        return
    nomes = list(dados.keys())
    valores = list(dados.values())
    plt.figure()
    plt.pie(valores, labels=nomes, autopct='%1.1f%%', startangle=140)
    plt.title(titulo)
    salvar_figura(caminho)

def gerar_tempo(stats, titulo, caminho_imagem):
    if stats.empty:
        print(f"Warning: Empty DataFrame for {titulo}. Skipping graph.")
        return
    
    if not pd.api.types.is_datetime64_any_dtype(stats.index):
        stats.index = pd.to_datetime(stats.index, errors='coerce') 
    
    stats = stats.dropna()
    
    if stats.empty:
        print(f"Warning: No valid datetime values for {titulo}. Skipping graph.")
        return
    
    min_date = pd.Timestamp('0001-01-01')
    max_date = pd.Timestamp('9999-12-31')
    stats = stats[(stats.index >= min_date) & (stats.index <= max_date)]
    
    if stats.empty:
        print(f"Warning: No values within valid date range for {titulo}. Skipping graph.")
        return
    
    try:
        # Intervalo de tempo 05:00 e 05:15
        start_time = pd.Timestamp(stats.index.date[0]) + pd.Timedelta(hours=5)
        end_time = start_time + pd.Timedelta(minutes=15)
        stats = stats[(stats.index >= start_time) & (stats.index <= end_time)]
        
        if stats.empty:
            print(f"Warning: No data in the 05:00-05:15 interval for {titulo}. Using full time range instead.")
            start_time = stats.index.min()
            end_time = stats.index.max()
            stats = stats
    except IndexError:
        print(f"Warning: Could not access date information for {titulo}. Using full time range.")
        stats = stats
    
    if stats.empty:
        print(f"Warning: No data to plot for {titulo} after filtering. Skipping graph.")
        return
    
    stats_aggregated = stats.resample('T').sum()  
    
    plt.figure(figsize=(10, 6))
    plt.plot(stats_aggregated.index, stats_aggregated.values, label=titulo)
    plt.title(titulo)
    plt.xlabel('Tempo')
    plt.ylabel('Quantidade de Pacotes')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(caminho_imagem)
    plt.close()

def gerar_heatmap(dados, titulo, caminho):
    """
    Gera um gráfico de heatmap a partir de uma matriz de correlação ou dados similares.
    
    Args:
        dados: DataFrame com dados para o heatmap
        titulo: Título do gráfico
        caminho: Caminho para salvar a imagem
    """
    try:
        # Verificar se os dados são None ou um DataFrame vazio
        if dados is None or (isinstance(dados, pd.DataFrame) and dados.empty):
            print(f"Warning: Dados vazios para o heatmap '{titulo}'. Pulando geração.")
            return
            
        # Garantir que os dados sejam um DataFrame
        if not isinstance(dados, pd.DataFrame):
            try:
                dados = pd.DataFrame(dados)
            except Exception as e:
                print(f"Erro ao converter dados para DataFrame em '{titulo}': {e}")
                return
                
        if dados.select_dtypes(include=['number']).empty:
            print(f"Warning: Não há valores numéricos para o heatmap '{titulo}'. Pulando geração.")
            return
            
        # Remover colunas/linhas que são totalmente NaN
        dados = dados.dropna(how='all', axis=0).dropna(how='all', axis=1)
        
        if not dados.empty and dados.shape[0] > 1 and dados.shape[1] > 1:
            plt.figure(figsize=(10, 8))
            
            ax = sns.heatmap(
                dados, 
                cmap='turbo',
                annot=False, 
                robust=True,  
                cbar_kws={'label': 'Valor'}, 
                square=True  
            )
            plt.title(titulo, fontsize=14, pad=20)
            plt.xticks(rotation=45, ha='right', fontsize=9)
            plt.yticks(fontsize=9)
            
            plt.tight_layout()
            plt.savefig(caminho, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Heatmap '{titulo}' gerado com sucesso: {caminho}")
        else:
            print(f"Warning: Dados insuficientes para o heatmap '{titulo}' após limpeza. Pulando geração.")
    except Exception as e:
        print(f"Erro ao gerar heatmap '{titulo}': {e}")

def gerar_boxplot_ipg_por_ip(ipg_dict, titulo, caminho):
    if not ipg_dict:
        return
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=list(ipg_dict.values()))
    plt.xticks(ticks=range(len(ipg_dict)), labels=list(ipg_dict.keys()), rotation=45, ha='right')
    plt.ylabel("IPG (s)")
    plt.title(titulo)
    salvar_figura(caminho)

def gerar_cdf(valores, titulo, caminho):
    if not valores:
        return
    sorted_vals = np.sort(valores)
    yvals = np.arange(1, len(sorted_vals)+1) / len(sorted_vals)

    plt.figure()
    plt.plot(sorted_vals, yvals, marker='.', linestyle='none')
    plt.xlabel("Tamanho (bytes)")
    plt.ylabel("Probabilidade Acumulada")
    plt.title(titulo)
    plt.grid(True)
    salvar_figura(caminho)

def gerar_scatter_tamanho_frequencia(scatter_tamanho_frequencia, titulo, output_file):
    tempos = [
        item[1].total_seconds() 
        if isinstance(item[1], datetime.timedelta) 
        else float(item[1]) 
        for item in scatter_tamanho_frequencia
    ]
    tamanhos = [item[0] for item in scatter_tamanho_frequencia]

    plt.figure(figsize=(10,6))
    plt.scatter(tempos, tamanhos, alpha=0.5)
    plt.title(titulo)
    plt.xlabel("IPG (segundos)")
    plt.ylabel("Tamanho do Pacote (bytes)")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()

def gerar_heatmap_ips_ativos(matrix, ips, tempos, titulo, caminho):
    if matrix is None or not ips or not tempos:
        return
    plt.figure(figsize=(12, 8))
    sns.heatmap(matrix, xticklabels=tempos, yticklabels=ips, cmap='YlGnBu')
    plt.xlabel("Tempo")
    plt.ylabel("IPs")
    plt.title(titulo)
    salvar_figura(caminho)

def gerar_linhas_por_protocolo(dados, titulo, caminho):
    if not dados:
        return
    plt.figure(figsize=(12, 6))
    for protocolo, tempos in dados.items():
        x = sorted(tempos.keys())
        y = [tempos[t] for t in x]
        plt.plot(x, y, marker='o', label=protocolo)

    plt.xlabel("Tempo")
    plt.ylabel("Quantidade de Pacotes")
    plt.title(titulo)
    plt.xticks(rotation=45, ha='right')
    plt.legend()
    salvar_figura(caminho)

def gerar_barras_empilhadas(dados, titulo, caminho):
    if not dados:
        return
    df = pd.DataFrame(dados).T.fillna(0)
    ax = df.plot(kind="bar", stacked=True, figsize=(12, 6))

    plt.title(titulo)
    plt.xlabel("IPs")
    plt.ylabel("Número de Pacotes")
    plt.xticks(rotation=45, ha='right')
    plt.legend(title="Protocolos", bbox_to_anchor=(1.05, 1), loc='upper left')
    salvar_figura(caminho)

def gerar_violin_tamanho_por_protocolo(dados, titulo, caminho):
    if not dados:
        return
    all_data = []
    for protocolo, tamanhos in dados.items():
        for tamanho in tamanhos:
            all_data.append({"Protocolo": protocolo, "Tamanho": tamanho})
    df = pd.DataFrame(all_data)

    plt.figure(figsize=(10, 6))
    sns.violinplot(x="Protocolo", y="Tamanho", data=df)
    plt.title(titulo)
    salvar_figura(caminho)


def calcular_atividade_ips(arquivo_csv):
    df = pd.read_csv(arquivo_csv)
    ips = list(df['ip_origem']) + list(df['ip_destino'])
    ip_count = Counter(ips)
    top_10_ips = ip_count.most_common(10)
    
    # Salvar os 10 IPs em um arquivo JSON
    with open("top_10_ips.json", "w") as f:
        json.dump(top_10_ips, f, indent=4)

    return top_10_ips


if __name__ == "__main__":
    stats = dataProcessing.analisar_estatisticas("data_300k.csv")    
    gerar_graficos(stats)
    top_10_ips = stats.get("top_ips_origem", {})
    
    with open("top_10_ips.json", "w") as f:
        json.dump(top_10_ips, f, indent=4)