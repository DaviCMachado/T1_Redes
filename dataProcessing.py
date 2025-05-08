import pandas as pd
import numpy as np
from collections import Counter, defaultdict
from math import log2
import heapq
import json

LIMITE_TIMESTAMP = 1e12 

def salvar_stats_json(stats, caminho="stats.json"):
    def converte(value):
        # Se for timestamp (pandas ou numpy), converter para string
        if isinstance(value, (pd.Timestamp, np.datetime64)):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        # Se for um numpy.ndarray, converter para lista
        elif isinstance(value, np.ndarray):
            return value.tolist()
        # Se for um dicionário, aplicar recursivamente
        elif isinstance(value, dict):
            return {converte(k): converte(v) for k, v in value.items()}
        # Se for uma lista, aplicar recursivamente
        elif isinstance(value, list):
            return [converte(v) for v in value]
        # Se for np.int64 ou np.float64, converter para int ou float
        elif isinstance(value, (np.int64, np.float64)):
            return int(value) if isinstance(value, np.int64) else float(value)
        else:
            return value
    
    stats_convertido = {
        "top_ips_origem": (
            dict(sorted(stats["top_ips_origem"].items(), key=lambda item: item[1], reverse=True)[:10])
            if not isinstance(stats["top_ips_origem"], Counter) 
            else stats["top_ips_origem"]
        ),
        "top_ips_mais_ativos": (
            dict(sorted(stats["top_ips_mais_ativos"].items(), key=lambda item: item[1], reverse=True)[:10])
            if not isinstance(stats["top_ips_mais_ativos"], Counter) 
            else stats["top_ips_mais_ativos"]
        ),
        "ipg_por_ip": (
            stats["ipg_por_ip"][:10] if isinstance(stats["ipg_por_ip"], list) 
            else dict(
                sorted(
                    stats["ipg_por_ip"].items(),
                    key=lambda item: item[1].get("media_ipg", 0) or 0,  # usa media_ipg como critério
                    reverse=True
                )[:10]
            )
        ),
        "entropia_ips_origem": stats["entropia_ips_origem_geral"],
        "volume_bytes_por_ip": (
            stats["volume_por_ip"][:10] if isinstance(stats["volume_por_ip"], list) 
            else dict(sorted(stats["volume_por_ip"].items(), key=lambda item: item[1], reverse=True)[:10])
        ),
        "pacotes_por_tempo": (
            stats["pacotes_por_tempo"][:10] if isinstance(stats["pacotes_por_tempo"], list) 
            else dict(sorted(stats["pacotes_por_tempo"].items(), key=lambda item: item[1], reverse=True)[:10])
        ),
        "anomalias_por_ip": dict(
            sorted(
                stats["anomalias_por_ip"].items(),
                key=lambda item: item[1]["bursts"],  # Ordenar pelos "bursts", ou mude para "silencios" conforme necessário
                reverse=True
            )[:10]
        ),
        "horizontal_scan": (
            stats["horizontal_scan"][:10] if isinstance(stats["horizontal_scan"], list) 
            else dict(sorted(stats["horizontal_scan"].items(), key=lambda item: item[1], reverse=True)[:10])
        ),
        "top_10_horizon_scan": 
            stats["top_10_horizon_scan"]
        ,
        "top_10_tamanhos_medios_por_ip": stats ["top_10_tamanhos_medios_por_ip"],
        "trafego_por_minuto": (
            stats["trafego_por_minuto"][:10] if isinstance(stats["trafego_por_minuto"], list) 
            else dict(sorted(stats["trafego_por_minuto"].items(), key=lambda item: item[1], reverse=True)[:10])
        )
    }

    stats_convertido = converte(stats_convertido)

    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(stats_convertido, f, indent=4, ensure_ascii=False)

def calcular_entropia(counter):
    total_pacotes = sum(counter.values())
    if total_pacotes == 0:
        return 0  # Caso não haja pacotes, a entropia é zero (sem tráfego)

    # Calcular as probabilidades de cada valor
    probabilidades = np.array([count / total_pacotes for count in counter.values()])
    
    # Substituir zeros por um valor pequeno para evitar erro no logaritmo
    probabilidades = np.maximum(probabilidades, 1e-10)

    # Calcular a entropia como a soma de -p * log2(p)
    entropia = -np.sum(probabilidades * np.log2(probabilidades))
    return entropia

    
def ajustar_timestamp(timestamp):
    # Se o valor estiver em milissegundos (muito grande), converta para segundos
    if timestamp > 1e10:  # Se for maior que 10 bilhões, provavelmente é milissegundo
        timestamp = timestamp / 1000  # Convertendo milissegundos para segundos
    
    # Verificar se o timestamp é razoável (evitar overflows)
    if timestamp > 1e9:  # Timestamps em segundos não devem ser maiores que 1 bilhão (aproximadamente 2030)
        return int(timestamp)
    else:
        return None  # Caso o timestamp seja inválido, retornamos None

def analisar_estatisticas(caminho_csv="data.csv", top_n=10, limite_burst=0.01, limite_silencio=1):
    # Inicializações
    protocolos = Counter()
    tamanhos = []
    ip_origem = Counter()
    ip_destino = Counter()
    pacotes_por_tempo = Counter()
    ipg_por_ip = defaultdict(list)
    destinos_por_ip_origem = defaultdict(Counter)
    trafego_por_minuto = Counter()
    last_timestamps = {}
    volume_bytes_por_ip = Counter()
    pacotes_por_ip = Counter()
    scatter_tamanho_frequencia = []
    heatmap_ips_tempo = {
        "matriz": defaultdict(lambda: defaultdict(int)),
        "ips": set(),
        "tempos": set(),
    }

    dados_corr = []
    colunas_numericas = None

    chunks = pd.read_csv(caminho_csv, chunksize=100_000)

    # Processamento dos pacotes
    for chunk in chunks:
        chunk = chunk.copy()
        chunk['timestamp'] = chunk['timestamp'].apply(ajustar_timestamp)
        chunk.dropna(subset=['timestamp'], inplace=True)
        
        # Agora, podemos aplicar a conversão para datetime
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s', errors='coerce')
        
        # Verifique se a conversão foi bem-sucedida
        chunk.dropna(subset=['timestamp'], inplace=True)
        
        # Agora você pode usar .dt para manipulação de datas
        chunk['minuto'] = chunk['timestamp'].dt.floor('min')
        chunk['segundo'] = chunk['timestamp'].dt.floor('s')

        # Atualização de contadores
        pacotes_por_tempo.update(chunk['minuto'].value_counts().to_dict())
        trafego_minuto = chunk.groupby('minuto')['length'].sum()
        trafego_por_minuto.update(trafego_minuto.to_dict())
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
                if ipg >= 0 and ipg > 0.00001:
                    ipg_por_ip[src_ip].append(ipg)
            last_timestamps[src_ip] = timestamp
            destinos_por_ip_origem[src_ip][dst_ip] += 1
            volume_bytes_por_ip[src_ip] += length

            if src_ip in ipg_por_ip and ipg_por_ip[src_ip]:
                ipg_value = ipg_por_ip[src_ip][-1]
                if isinstance(ipg_value, (int, float)) and not np.isnan(ipg_value) and ipg_value >= 0:
                    ipg_value_normalizado = np.log(ipg_value + 1)
                    scatter_tamanho_frequencia.append((length, ipg_value_normalizado))

        # Atualização de colunas numéricas
        if colunas_numericas is None:
            colunas_numericas = chunk.select_dtypes(include='number').columns.tolist()
        dados_corr.append(chunk[colunas_numericas])

    # Processamento para Heatmap
        top_ips_heatmap = set([ip for ip, _ in ip_origem.most_common(top_n)])
        for chunk in pd.read_csv(caminho_csv, chunksize=100_000):
            chunk = chunk.copy()
        
            # Garantir que os timestamps estão em segundos, arredondando quando necessário
            chunk['timestamp'] = chunk['timestamp'].apply(lambda ts: int(round(float(ts))) if pd.notna(ts) else ts)
            
            # Limitar os timestamps para não exceder o valor máximo aceitável
            chunk['timestamp'] = chunk['timestamp'].clip(upper=LIMITE_TIMESTAMP)
            
            # Convertendo para datetime (com segurança)
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s', errors='coerce')
            
            # Remover qualquer linha com timestamp inválido
            chunk.dropna(subset=['timestamp'], inplace=True)
            
            # Adicionar o minuto para o cálculo do heatmap
            chunk['minuto'] = chunk['timestamp'].dt.floor('min')
            
            # Processar as linhas e preencher o heatmap
            for ip, minuto in zip(chunk['src_ip'], chunk['minuto']):
                if ip in top_ips_heatmap:
                    heatmap_ips_tempo["matriz"][ip][minuto] += 1
                    heatmap_ips_tempo["ips"].add(ip)
                    heatmap_ips_tempo["tempos"].add(minuto)

    # Estatísticas de tamanhos
    tamanhos_serie = pd.Series(tamanhos)
    estatisticas_tamanho = {
        "media": round(tamanhos_serie.mean(), 2),
        "desvio_padrao": round(tamanhos_serie.std(), 2),
        "maximo": tamanhos_serie.max(),
        "minimo": tamanhos_serie.min(),
    }

    # Estatísticas de protocolos
    total_protocolos = dict(protocolos)
    total_pacotes = sum(total_protocolos.values())
    estatisticas_protocolos = {protocol: (count / total_pacotes) * 100 for protocol, count in total_protocolos.items()}

    # Estatísticas de IPG
    estatisticas_ipg = {}
    for ip, ipgs in ipg_por_ip.items():
        ipgs_validos = [ipg for ipg in ipgs if isinstance(ipg, (int, float)) and not np.isnan(ipg)]
        if len(ipgs_validos) >= 3:
            estatisticas_ipg[ip] = {
                "media_ipg": round(np.mean(ipgs_validos), 2),
                "maximo_ipg": round(np.max(ipgs_validos), 2),
                "minimo_ipg": round(np.min(ipgs_validos), 2),
                "desvio_padrao_ipg": round(np.std(ipgs_validos), 2),
            }
        else:
            estatisticas_ipg[ip] = {key: None for key in ["media_ipg", "maximo_ipg", "minimo_ipg", "desvio_padrao_ipg"]}

    # Estatísticas de Entropia
    entropia_ips_origem_geral = calcular_entropia(ip_origem)


    # Anomalias por IP
    anomalias_por_ip = {}
    for ip, ipgs in ipg_por_ip.items():
        if len(ipgs) < 2:
            continue
        ipgs_seconds = [v for v in ipgs if isinstance(v, (int, float)) and not np.isnan(v)]
        burst_count = sum(1 for v in ipgs_seconds if v < limite_burst)
        silencio_count = sum(1 for v in ipgs_seconds if v > limite_silencio)
        anomalias_por_ip[ip] = {"bursts": burst_count, "silencios": silencio_count}

    # Analisando Conexões Horizontais
    maiores_conexoes = [
        (destino_mais_acessado[1], origem, destino_mais_acessado[0])
        for origem, destinos in destinos_por_ip_origem.items()
        if destinos and (destino_mais_acessado := max(destinos.items(), key=lambda x: x[1]))
    ]
    top_maiores = heapq.nlargest(top_n, maiores_conexoes)
    top_destinos_por_unicoip = [destino for _, _, destino in top_maiores]

    tamanho_medio_por_ip = {}
    for ip in volume_bytes_por_ip:
        pacotes = pacotes_por_ip[ip]  # Número total de pacotes para o IP
        volume = volume_bytes_por_ip[ip]  # Volume total de bytes para o IP
        
        # Verificar se o número de pacotes é maior que zero
        if pacotes > 0:
            # Cálculo do tamanho médio por pacote para o IP
            tamanho_medio = volume / pacotes
            tamanho_medio_por_ip[ip] = tamanho_medio
        else:
            # Caso haja zero pacotes, atribuimos 0 ao tamanho médio (evita divisão por zero)
            tamanho_medio_por_ip[ip] = 0

    # Ordenando os IPs com o maior tamanho médio
    top_10_tamanhos_medios_por_ip = dict(
        sorted(tamanho_medio_por_ip.items(), key=lambda item: item[1], reverse=True)[:top_n]
    )



    

    # Preparando os dados finais
    stats_json = {
        "top_ips_origem": dict(ip_origem.most_common(top_n)),
        "top_ips_destino": dict(ip_destino.most_common(top_n)),
        "top_ips_mais_ativos": dict(ip_origem.most_common(top_n)),
        "estatisticas_tamanho": estatisticas_tamanho,
        "estatisticas_protocolos": estatisticas_protocolos,
        "heatmap_ips_tempo": heatmap_ips_tempo,
        "ipg_por_ip": estatisticas_ipg,
        "entropia_ips_origem_geral": entropia_ips_origem_geral,
        "volume_por_ip": volume_bytes_por_ip,
        "pacotes_por_tempo": dict(pacotes_por_tempo),
        "relacao_tamanho_frequencia": scatter_tamanho_frequencia,
        "anomalias_por_ip": anomalias_por_ip,
        "horizontal_scan": top_destinos_por_unicoip,
        "top_10_horizon_scan": top_maiores,
        "top_10_tamanhos_medios_por_ip": top_10_tamanhos_medios_por_ip,
        "trafego_por_minuto": {str(k): int(v) for k, v in trafego_por_minuto.items()}
    }

    salvar_stats_json(stats_json)
    print("Métricas salvas em stats.json")
    return stats_json
