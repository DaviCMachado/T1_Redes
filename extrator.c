

// como usar
// editcap -c 100000 original.pcap batches/parte.pcap     -> divide o arquivo original em partes de 100.000 pacotes e salva na pasta batches
// gcc extrator.c -lpcap -lpthread -o extrator  -> compilar
// ./extrator batches/parte_000*.pcap   -> rodar para todas as partes



#include <pcap.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <netinet/ip.h>
#include <arpa/inet.h>

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

const char *protocolo_para_str(uint8_t protocolo) {
    switch (protocolo) {
        case IPPROTO_TCP: return "TCP";
        case IPPROTO_UDP: return "UDP";
        case IPPROTO_ICMP: return "ICMP";
        default: return "OUTRO";
    }
}

void manipular_pacote(unsigned char *args, const struct pcap_pkthdr *cabecalho, const unsigned char *pacote) {
    FILE *data = (FILE *)args;

    struct iphdr *ip_header = (struct iphdr *)(pacote + 14);
    char src_ip[INET_ADDRSTRLEN], dst_ip[INET_ADDRSTRLEN];
    struct in_addr src_addr = {.s_addr = ip_header->saddr};
    struct in_addr dst_addr = {.s_addr = ip_header->daddr};

    inet_ntop(AF_INET, &src_addr, src_ip, INET_ADDRSTRLEN);
    inet_ntop(AF_INET, &dst_addr, dst_ip, INET_ADDRSTRLEN);
    double timestamp = cabecalho->ts.tv_sec + cabecalho->ts.tv_usec / 1000000.0;

    pthread_mutex_lock(&lock);
    fprintf(data, "%.6f,%s,%s,%s,%d\n", timestamp, src_ip, dst_ip, protocolo_para_str(ip_header->protocol), cabecalho->len);
    pthread_mutex_unlock(&lock);
}

void *processar_pcap(void *arg) {
    char *nome_arquivo = (char *)arg;

    char errbuf[PCAP_ERRBUF_SIZE];
    pcap_t *handle = pcap_open_offline(nome_arquivo, errbuf);
    if (!handle) {
        fprintf(stderr, "Erro ao abrir %s: %s\n", nome_arquivo, errbuf);
        return NULL;
    }

    FILE *data = fopen("data.csv", "a");
    if (!data) {
        perror("Erro ao abrir data.csv");
        pcap_close(handle);
        return NULL;
    }

    pcap_loop(handle, 0, manipular_pacote, (unsigned char *)data);
    fclose(data);
    pcap_close(handle);

    printf("Thread finalizou: %s\n", nome_arquivo);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Uso: %s parte1.pcap parte2.pcap ...\n", argv[0]);
        return 1;
    }

    // Criar arquivo CSV com cabeçalho
    FILE *data = fopen("data.csv", "w");
    fprintf(data, "timestamp,src_ip,dst_ip,protocol,length\n");
    fclose(data);

    int num_arquivos = argc - 1;
    pthread_t threads[num_arquivos];

    for (int i = 0; i < num_arquivos; i++) {
        pthread_create(&threads[i], NULL, processar_pcap, argv[i + 1]);
    }

    for (int i = 0; i < num_arquivos; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("Processamento multithread finalizado.\n");
    return 0;
}
