import os
import matplotlib.pyplot as plt
import dataProcessing


def gerar_graficos(stats):
    os.makedirs("img/barras", exist_ok=True)
    os.makedirs("img/histograms", exist_ok=True)
    os.makedirs("img/pizza", exist_ok=True)

    # Gráficos de Barras
    if "protocolos" in stats:
        gerar_barra(stats["protocolos"], "Frequência de Protocolos", "img/barras/protocolos.png")

    if "top_ips_origem" in stats:
        gerar_barra(stats["top_ips_origem"], "Top IPs de Origem", "img/barras/ip_origem.png")

    if "top_ips_destino" in stats:
        gerar_barra(stats["top_ips_destino"], "Top IPs de Destino", "img/barras/ip_destino.png")

    # Histograma de Tamanhos de Pacotes
    if "tamanhos" in stats:
        gerar_histograma(stats["tamanhos"], "Distribuição dos Tamanhos dos Pacotes", "img/histograms/tamanhos.png")

    # Gráficos de Pizza
    if "protocolos" in stats:
        gerar_pizza(stats["protocolos"], "Proporção de Protocolos", "img/pizza/protocolos_pizza.png")

    if "top_ips_origem" in stats:
        gerar_pizza(stats["top_ips_origem"], "Proporção dos Principais IPs de Origem", "img/pizza/ip_origem_pizza.png")

    if "top_ips_destino" in stats:
        gerar_pizza(stats["top_ips_destino"], "Proporção dos Principais IPs de Destino", "img/pizza/ip_destino_pizza.png")


def gerar_barra(dados, titulo, caminho):
    nomes = list(dados.keys())
    valores = list(dados.values())
    plt.figure()
    plt.bar(nomes, valores, color='skyblue')
    plt.title(titulo)
    plt.ylabel("Frequência")
    plt.xlabel("Categoria")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(caminho)
    plt.close()

def gerar_histograma(valores, titulo, caminho):
    plt.figure()
    plt.hist(valores, bins=20, color='orange')
    plt.title(titulo)
    plt.ylabel("Frequência")
    plt.xlabel("Tamanho (bytes)")
    plt.tight_layout()
    plt.savefig(caminho)
    plt.close()

def gerar_pizza(dados, titulo, caminho):
    nomes = list(dados.keys())
    valores = list(dados.values())
    plt.figure()
    plt.pie(valores, labels=nomes, autopct='%1.1f%%', startangle=140)
    plt.title(titulo)
    plt.tight_layout()
    plt.savefig(caminho)
    plt.close()


if __name__ == "__main__":
    stats = dataProcessing.analisar_estatisticas("data.csv")
    gerar_graficos(stats)
