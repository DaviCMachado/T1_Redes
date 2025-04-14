import pandas as pd
from collections import Counter

def analisar_estatisticas(caminho_csv="data.csv", top_n=5):
    protocolos = Counter()
    tamanhos = []
    ip_origem = Counter()
    ip_destino = Counter()

    for chunk in pd.read_csv(caminho_csv, chunksize=100_000):
        protocolos.update(chunk['protocol'])
        tamanhos.extend(chunk['length'].tolist())
        ip_origem.update(chunk['src_ip'])
        ip_destino.update(chunk['dst_ip'])

    # Estatísticas adicionais
    estatisticas_tamanho = {
        "media": round(pd.Series(tamanhos).mean(), 2),
        "desvio_padrao": round(pd.Series(tamanhos).std(), 2),
        "maximo": max(tamanhos),
        "minimo": min(tamanhos),
    }

    return {
        "protocolos": dict(protocolos),
        "estatisticas_tamanho": estatisticas_tamanho,
        "top_ips_origem": dict(ip_origem.most_common(top_n)),
        "top_ips_destino": dict(ip_destino.most_common(top_n)),
        "tamanhos": tamanhos,
    }

def obter_top_protocolos(caminho_csv="T1_Redes/data.csv", top_n=5):
    stats = analisar_estatisticas(caminho_csv, top_n)
    return stats["protocolos"]

def obter_top_ips_origem(caminho_csv="T1_Redes/data.csv", top_n=5):
    stats = analisar_estatisticas(caminho_csv, top_n)
    return stats["top_ips_origem"]
