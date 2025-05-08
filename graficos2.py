import matplotlib.pyplot as plt
import json
import os

# Função para carregar os dados do arquivo JSON
def carregar_dados_json(caminho):
    with open(caminho, 'r') as f:
        return json.load(f)

# Função para garantir que as pastas existam
def garantir_pastas():
    pastas = ['img/barras', 'img/tempo', 'img/heatmap']
    for pasta in pastas:
        if not os.path.exists(pasta):
            os.makedirs(pasta)

# Função para salvar as figuras
def salvar_figura(caminho_imagem):
    plt.tight_layout()
    plt.savefig(caminho_imagem)
    plt.close()

# Função para gerar gráficos gerais
def gerar_graficos2(dados):
    garantir_pastas()

    # 1. Top IPs de Origem
    ips_origem = dados['top_ips_origem']
    plt.figure(figsize=(10, 6))
    plt.bar(ips_origem.keys(), ips_origem.values(), color='skyblue')
    plt.title("Top 10 IPs de Origem")
    plt.xlabel("IP de Origem")
    plt.ylabel("Número de Pacotes")
    plt.xticks(rotation=45)
    salvar_figura('img/barras/top_ips_origem.png')

    # 2. IPG por IP
    ipg_por_ip = {ip: info['media_ipg'] for ip, info in dados['ipg_por_ip'].items()}
    plt.figure(figsize=(10, 6))
    plt.bar(ipg_por_ip.keys(), ipg_por_ip.values(), color='lightgreen')
    plt.title("IPG Médio por IP")
    plt.xlabel("IP")
    plt.ylabel("IPG Médio")
    plt.xticks(rotation=45)
    salvar_figura('img/barras/ipg_por_ip.png')

    # 3. Entropia dos IPs de Origem
    # entropias = dados["entropia_ips_origem"]
    # if isinstance(entropias, dict):
    #     plt.figure(figsize=(10, 6))
    #     plt.bar(entropias.keys(), entropias.values(), color='orange')
    #     plt.title("Entropia dos IPs de Origem")
    #     plt.xlabel("IP de Origem")
    #     plt.ylabel("Entropia")
    #     plt.xticks(rotation=45)
    #     salvar_figura('img/barras/entropia_ips_origem.png')
    # else:
    #     print("Erro: entropias dos IPs de origem não está no formato esperado (dicionário).")

    # 4. Bursts por IP
    anomalias = {ip: info["bursts"] for ip, info in dados["anomalias_por_ip"].items()}
    plt.figure(figsize=(10, 6))
    plt.bar(anomalias.keys(), anomalias.values(), color='salmon')
    plt.title("Bursts por IP")
    plt.xlabel("IP")
    plt.ylabel("Número de Bursts")
    plt.xticks(rotation=45)
    salvar_figura('img/barras/bursts_por_ip.png')

    # 5. Top 10 IPs com maior número de destinos únicos (horizontal scan)
    if "top_10_horizon_scan" in dados:
        top_hscan = dados["top_10_horizon_scan"]
        ips = [item[1] for item in top_hscan]
        destinos = [item[0] for item in top_hscan]
        plt.figure(figsize=(10, 6))
        plt.barh(ips, destinos, color='mediumslateblue')
        plt.xlabel("Nº de destinos únicos")
        plt.title("Top 10 IPs com maior nº de destinos (possível scan horizontal)")
        plt.gca().invert_yaxis()
        salvar_figura('img/barras/top_10_horizon_scan.png')

    # 6. Top 10 IPs com maior tamanho médio de pacotes
    if "top_10_tamanhos_medios_por_ip" in dados:
        tamanhos_medios = dados["top_10_tamanhos_medios_por_ip"]
        ips = list(tamanhos_medios.keys())
        tamanhos = list(tamanhos_medios.values())
        plt.figure(figsize=(10, 6))
        plt.bar(ips, tamanhos, color='darkorange')
        plt.xticks(rotation=45)
        plt.ylabel('Tamanho médio dos pacotes (bytes)')
        plt.title('Top 10 IPs com maior tamanho médio de pacotes')
        salvar_figura('img/barras/top_10_tamanhos_medios_por_ip.png')

    # 7. Top 10 IPs com maior volume de bytes trocados
    if "volume_bytes_por_ip" in dados:
        volume = dados["volume_bytes_por_ip"]
        ips = list(volume.keys())
        bytes_trocados = list(volume.values())
        plt.figure(figsize=(10, 6))
        plt.bar(ips, bytes_trocados, color='teal')
        plt.xticks(rotation=45)
        plt.ylabel('Volume de Bytes Trocados')
        plt.title('Top 10 IPs por Volume de Bytes')
        salvar_figura('img/barras/volume_bytes_por_ip.png')



def second_step():
    # Carregar os dados de stats.json
    dados = carregar_dados_json('stats.json')

    # Gerar e salvar os gráficos
    gerar_graficos2(dados)
