import pandas as pd
from collections import Counter
import numpy as np
from scipy.stats import skew, kurtosis
from math import log2
import json
import pandas as pd

def salvar_stats_json(stats, caminho="stats.json"):
    def converte(value):
        if isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif isinstance(value, dict):
            return {converte(k): converte(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [converte(v) for v in value]
        else:
            return value

    # IPs mais ativos
    top_ips = set(stats["top_ips_mais_ativos"].keys())

    stats_convertido = {
        "top_ips_origem": stats.get("top_ips_origem", {}),
        "top_ips_mais_ativos": stats["top_ips_mais_ativos"],
        "ipg_por_ip": {ip: stats["ipg_por_ip"][ip] for ip in top_ips if ip in stats["ipg_por_ip"]},
        "entropia_ips_origem": stats["entropia_ips_origem"],
        "entropia_ips_origem_individual": {ip: stats["entropia_ips_origem_individual"][ip] for ip in top_ips if ip in stats["entropia_ips_origem_individual"]},
        "volume_bytes_por_ip": {ip: stats["volume_por_ip"][ip] for ip in top_ips if ip in stats["volume_por_ip"]},
        "pacotes_por_tempo": stats["pacotes_por_tempo"],
        "anomalias_por_ip": {ip: stats["anomalias_por_ip"][ip] for ip in top_ips if ip in stats["anomalias_por_ip"]}
    }

    stats_convertido = converte(stats_convertido)

    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(stats_convertido, f, indent=4, ensure_ascii=False)


def calcular_entropia(counter):
    total_pacotes = sum(counter.values())
    probabilidades = [count / total_pacotes for count in counter.values()]
    entropia = -sum(p * log2(p) for p in probabilidades if p > 0)
    return entropia

def calcular_cdf(valores):
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
    pacotes_por_ip = Counter()
    ip_minuto_src = []
    ip_minuto_dst = []
    volume_por_ip_minuto_src = {}
    volume_por_ip_minuto_dst = {}
    pacotes_por_segundo = {}
    scatter_tamanho_frequencia = []

    chunks = pd.read_csv(caminho_csv, chunksize=100_000)
    
    for chunk in chunks:
        chunk = chunk.copy()
        
        colunas_esperadas = {'timestamp', 'length', 'src_ip', 'dst_ip', 'protocol'}
        falta = colunas_esperadas - set(chunk.columns)
        if falta:
            raise ValueError(f"Faltam colunas no CSV: {falta}")
        
        # Converter timestamp para datetime
        chunk['timestamp'] = pd.to_numeric(chunk['timestamp'], errors='coerce')
        chunk = chunk[(chunk['timestamp'] > 0) & (chunk['timestamp'] < 1e10)]
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s', errors='coerce')
        chunk.dropna(subset=['timestamp'], inplace=True)

        chunk['minuto'] = chunk['timestamp'].dt.floor('min')
        chunk['segundo'] = chunk['timestamp'].dt.floor('s')

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

        pacotes_por_tempo.update(chunk['minuto'].value_counts().to_dict())

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

            pacotes_por_ip[src_ip] += 1

            if src_ip in last_timestamps:
                delta = timestamp - last_timestamps[src_ip]
                ipg = delta / np.timedelta64(1, 's')
                if ipg >= 0:
                    ipg_por_ip.setdefault(src_ip, []).append(ipg)
            last_timestamps[src_ip] = timestamp

            destinos_por_ip_origem.setdefault(src_ip, set()).add(dst_ip)
            volume_bytes_por_ip[src_ip] += length

            # Scatter: só se IPG válido
            if src_ip in ipg_por_ip and ipg_por_ip[src_ip]:
                ipg_value = ipg_por_ip[src_ip][-1]
                if isinstance(ipg_value, (int, float)) and not np.isnan(ipg_value) and ipg_value >= 0:
                    scatter_tamanho_frequencia.append((length, ipg_value))

        if colunas_numericas is None:
            colunas_numericas = chunk.select_dtypes(include='number').columns.tolist()
        dados_corr.append(chunk[colunas_numericas])

    tamanho_medio_por_ip = {ip: volume_bytes_por_ip[ip] / pacotes_por_ip[ip] 
                            for ip in volume_bytes_por_ip if pacotes_por_ip[ip] > 0}

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

    # Calcular entropia geral de todos os IPs de origem
    entropia_ips_origem_geral = calcular_entropia(ip_origem)

    # Top IPs de origem
    top_ips_origem = [ip for ip, _ in ip_origem.most_common(top_n)]
    
    # Calcular entropia individual para cada IP dos 10 mais ativos
    entropia_ips_origem_individual = {}
    for ip in top_ips_origem:
        entropia_ips_origem_individual[ip] = calcular_entropia(Counter({ip: ip_origem[ip]}))

    entropia_ips_destino = calcular_entropia(ip_destino)

    anomalias_por_ip = {}
    for ip, ipgs in ipg_por_ip.items():
        if len(ipgs) < 2:
            continue
        ipgs_seconds = [v for v in ipgs if isinstance(v, (int, float)) and not np.isnan(v)]
        burst_count = sum(1 for v in ipgs_seconds if v < 0.01)
        silencio_count = sum(1 for v in ipgs_seconds if v > 1)
        anomalias_por_ip[ip] = {"bursts": burst_count, "silencios": silencio_count}

    # Preparar os dados para o JSON
    stats_json = {
        "top_ips_origem": dict(ip_origem.most_common(top_n)),  # Top IPs de origem
        "top_ips_mais_ativos": dict(ip_origem.most_common(top_n)),  # Top 10 IPs mais ativos
        "ipg_por_ip": estatisticas_ipg,
        "entropia_ips_origem_geral": entropia_ips_origem_geral,  # Entropia geral
        "entropia_ips_origem_individual": entropia_ips_origem_individual,  # Entropia individual para cada IP
        "volume_por_ip": volume_bytes_por_ip,
        "pacotes_por_tempo": {
            str(k): v for k, v in pacotes_por_tempo.items()
            if isinstance(k, pd.Timestamp) and k.year == 2025
        },
        "relacao_tamanho_frequencia": scatter_tamanho_frequencia,
        "anomalias_por_ip": anomalias_por_ip,
        "cdf_tamanho_pacotes": cdf_tamanho_pacotes
    }

    # Salvar em um arquivo JSON
    salvar_stats_json(stats_json)
    print("Métricas salvas em stats.json")
    return stats_json
