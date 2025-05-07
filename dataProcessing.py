import pandas as pd
from collections import Counter
import numpy as np
from scipy.stats import skew, kurtosis
from math import log2
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates

def calcular_entropia(distribuicao):
    total = sum(distribuicao.values())
    if total == 0:
        return 0
    probabilidades = [v / total for v in distribuicao.values()]
    return -sum(p * log2(p) for p in probabilidades if p > 0)

def calcular_cdf(valores):
    """Calcula a Função de Distribuição Cumulativa (CDF) de um conjunto de valores"""
    valores_ordenados = sorted(valores)
    y = np.arange(1, len(valores_ordenados) + 1) / len(valores_ordenados)
    return valores_ordenados, y

def analisar_estatisticas(caminho_csv="data.csv", top_n=10):
    protocolos = Counter()
    tamanhos = []
    ip_origem = Counter()
    ip_destino = Counter()
    pacotes_por_tempo = Counter()
    ipg_por_ip = {}
    destinos_por_ip_origem = {}

    dados_corr = []
    colunas_numericas = None

    last_timestamps = {}

    volume_bytes_por_ip = Counter()
    pacotes_por_ip = Counter()  # Contador para calcular tamanho médio por IP
    ip_minuto_src = []
    ip_minuto_dst = []
    volume_por_ip_minuto_src = {}
    volume_por_ip_minuto_dst = {}
    pacotes_por_segundo = {}  # Para calcular variação de tráfego por segundo

    scatter_tamanho_frequencia = []

    chunks = pd.read_csv(caminho_csv, chunksize=100_000)
    
    for chunk in chunks:
        chunk = chunk.copy()
        
        # Verificar colunas obrigatórias
        colunas_esperadas = {'timestamp', 'length', 'src_ip', 'dst_ip', 'protocol'}
        falta = colunas_esperadas - set(chunk.columns)
        if falta:
            raise ValueError(f"Faltam colunas no CSV: {falta}")

        
        # Filtra e converte o timestamp para float (apenas com um ponto)
        chunk['timestamp'] = chunk['timestamp'].apply(
            lambda x: float(x) if isinstance(x, str) and x.count('.') == 1 else None
        )
        # Converte os timestamps para datetime
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s', errors='coerce')
        # Se houver valores NaT (não conversíveis), removemos as linhas
        chunk = chunk.dropna(subset=['timestamp'])
        chunk['minuto'] = chunk['timestamp'].dt.floor('min')
        chunk['segundo'] = chunk['timestamp'].dt.floor('S')  # Para análise por segundo

        # Contagem de pacotes por segundo
        for segundo, grupo in chunk.groupby('segundo'):
            pacotes_por_segundo[segundo] = len(grupo)

        grupo_src = chunk.groupby(['minuto', 'src_ip'])['length'].sum()
        for (minuto, ip), volume in grupo_src.items():
            ip_minuto_src.append((minuto, ip))
            volume_por_ip_minuto_src.setdefault(ip, {}).setdefault(minuto, 0)
            volume_por_ip_minuto_src[ip][minuto] += volume

        grupo_dst = chunk.groupby(['minuto', 'dst_ip'])['length'].sum()
        for (minuto, ip), volume in grupo_dst.items():
            ip_minuto_dst.append((minuto, ip))
            volume_por_ip_minuto_dst.setdefault(ip, {}).setdefault(minuto, 0)
            volume_por_ip_minuto_dst[ip][minuto] += volume

        pacotes_por_tempo.update(chunk['minuto'])

        protocolos.update(chunk['protocol'])
        tamanhos.extend(chunk['length'].dropna().tolist())
        ip_origem.update(chunk['src_ip'])
        ip_destino.update(chunk['dst_ip'])

        src_ips = chunk['src_ip'].values
        dst_ips = chunk['dst_ip'].values
        timestamps = chunk['timestamp'].values
        lengths = chunk['length'].values

        for i in range(len(src_ips)):
            src_ip = src_ips[i]
            dst_ip = dst_ips[i]
            timestamp = timestamps[i]
            length = lengths[i]

            # Contador para calcular o tamanho médio por IP
            pacotes_por_ip[src_ip] += 1

            # IPG com precisão de segundos flutuantes
            if src_ip in last_timestamps:
                delta = timestamp - last_timestamps[src_ip]
                ipg = delta / np.timedelta64(1, 's')  # converte para segundos como float

                ipg_por_ip.setdefault(src_ip, []).append(ipg)
            last_timestamps[src_ip] = timestamp

            destinos_por_ip_origem.setdefault(src_ip, set()).add(dst_ip)
            volume_bytes_por_ip[src_ip] += length

            # Scatter tamanho vs IPG
            if src_ip in ipg_por_ip and ipg_por_ip[src_ip]:
                scatter_tamanho_frequencia.append((length, ipg_por_ip[src_ip][-1]))

        # Matriz de correlação
        if colunas_numericas is None:
            colunas_numericas = chunk.select_dtypes(include='number').columns.tolist()
        dados_corr.append(chunk[colunas_numericas])

    # Cálculo do tamanho médio por IP
    tamanho_medio_por_ip = {ip: volume_bytes_por_ip[ip] / pacotes_por_ip[ip] 
                           for ip in volume_bytes_por_ip if pacotes_por_ip[ip] > 0}

    # Cálculo da CDF para tamanho de pacotes
    x_cdf, y_cdf = calcular_cdf(tamanhos)
    cdf_tamanho_pacotes = {"x": x_cdf, "y": y_cdf}

    tamanhos_serie = pd.Series(tamanhos)
    estatisticas_tamanho = {
        "media": round(tamanhos_serie.mean(), 2),
        "desvio_padrao": round(tamanhos_serie.std(), 2),
        "maximo": tamanhos_serie.max(),
        "minimo": tamanhos_serie.min(),
    }

    if dados_corr:
        dados_corr_df = pd.concat(dados_corr, ignore_index=True)
        matriz_correlacao = dados_corr_df.corr(numeric_only=True)
    else:
        matriz_correlacao = pd.DataFrame()

    total_protocolos = dict(protocolos)
    total_pacotes = sum(total_protocolos.values())
    estatisticas_protocolos = {protocol: (count / total_pacotes) * 100 for protocol, count in total_protocolos.items()}

    estatisticas_ipg = {}
    skewness_kurtosis = {}
    for ip, ipgs in ipg_por_ip.items():
        ipgs_validos = [ipg for ipg in ipgs if isinstance(ipg, (int, float)) and not np.isnan(ipg)]
        if len(ipgs_validos) >= 3:
            skewness_kurtosis[ip] = {
                "skewness": round(skew(ipgs_validos), 3),
                "kurtosis": round(kurtosis(ipgs_validos), 3)
            }
            estatisticas_ipg[ip] = {
                "media_ipg": round(np.mean(ipgs_validos), 2),
                "maximo_ipg": round(np.max(ipgs_validos), 2),
                "minimo_ipg": round(np.min(ipgs_validos), 2),
                "desvio_padrao_ipg": round(np.std(ipgs_validos), 2),
            }
        else:
            estatisticas_ipg[ip] = {
                "media_ipg": None,
                "maximo_ipg": None,
                "minimo_ipg": None,
                "desvio_padrao_ipg": None,
            }

    top_ips = [ip for ip, _ in ip_origem.most_common(top_n)]
    horizontal_scan = {ip: len(destinos_por_ip_origem.get(ip, [])) for ip in top_ips}
    entropia_ips_origem = calcular_entropia(ip_origem)
    entropia_ips_destino = calcular_entropia(ip_destino)
    entropia_temporal = calcular_entropia(pacotes_por_tempo)

    anomalias_por_ip = {}
    for ip, ipgs in ipg_por_ip.items():
        if len(ipgs) < 2:
            continue
        ipgs_seconds = [v for v in ipgs if isinstance(v, (int, float))]
        burst_count = sum(1 for v in ipgs_seconds if v < 0.01)
        silencio_count = sum(1 for v in ipgs_seconds if v > 1)
        anomalias_por_ip[ip] = {"bursts": burst_count, "silencios": silencio_count}

    top_destinos_por_ip = {
        ip: list(destinos_por_ip_origem.get(ip, []))[:top_n] for ip in top_ips
    }

    desvio_destinos_por_ip = np.std([len(d) for d in destinos_por_ip_origem.values()])

    return {
        "protocolos": total_protocolos,
        "estatisticas_tamanho": estatisticas_tamanho,
        "top_ips_origem": dict(ip_origem.most_common(top_n)),
        "top_ips_destino": dict(ip_destino.most_common(top_n)),
        "tamanhos": tamanhos,
        "pacotes_por_tempo": dict(pacotes_por_tempo),
        "pacotes_por_segundo": dict(pacotes_por_segundo),
        "matriz_correlacao": matriz_correlacao,
        "ipg_por_ip": {ip: ipg_por_ip[ip] for ip in top_ips if ip in ipg_por_ip},
        "estatisticas_ipg": {ip: estatisticas_ipg[ip] for ip in top_ips if ip in estatisticas_ipg},
        "skewness_kurtosis_ipg": {ip: skewness_kurtosis[ip] for ip in top_ips if ip in skewness_kurtosis},
        "horizontal_scan": horizontal_scan,
        "estatisticas_protocolos": estatisticas_protocolos,
        "entropia_ips_origem": entropia_ips_origem,
        "entropia_ips_destino": entropia_ips_destino,
        "entropia_temporal": entropia_temporal,
        "volume_por_ip": dict(volume_bytes_por_ip),
        "tamanho_medio_por_ip": tamanho_medio_por_ip, 
        "cdf_tamanho_pacotes": cdf_tamanho_pacotes,   
        "scatter_tamanho_frequencia": scatter_tamanho_frequencia,
        "anomalias_por_ip": {ip: anomalias_por_ip[ip] for ip in top_ips if ip in anomalias_por_ip},
        "top_destinos_por_ip": top_destinos_por_ip,
        "desvio_destinos_por_ip": desvio_destinos_por_ip,
        "volume_por_ip_minuto_origem": volume_por_ip_minuto_src,
        "volume_por_ip_minuto_destino": volume_por_ip_minuto_dst,
        "tempo_ip_origem": ip_minuto_src,
        "tempo_ip_destino": ip_minuto_dst,
    }