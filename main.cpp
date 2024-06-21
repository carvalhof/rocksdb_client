#include <numeric>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <vector>
#include <algorithm>

#define BUFFER_SIZE 1024

std::mutex mtx;
int core_list[] = {2,4,6,8,10,12,14,16};

int connect_to_server(const std::string& server_ip, int server_port) {
    int sock = 0;
    struct sockaddr_in serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("sock:");
        exit(-1);
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) {
        perror("inet_pton:");
        exit(-1);
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect:");
        exit(-1);
    }

    return sock;
}

long long send_command_and_measure_latency(int sock, const std::string& command) {
    char buffer[BUFFER_SIZE] = {0};
    auto start = std::chrono::high_resolution_clock::now();

    send(sock, command.c_str(), command.size(), 0);
    int ret __attribute__((unused)) = read(sock, buffer, BUFFER_SIZE);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<long long, std::micro> duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    return duration.count();
}

std::vector<long long> calculate_percentiles(std::vector<long long>& latencies) {
    std::sort(latencies.begin(), latencies.end());
    std::vector<long long> percentiles;

    percentiles.push_back(latencies[latencies.size() * 0.50]); // p50
    percentiles.push_back(latencies[latencies.size() * 0.75]); // p75
    percentiles.push_back(latencies[latencies.size() * 0.90]); // p90
    percentiles.push_back(latencies[latencies.size() * 0.99]); // p99
    percentiles.push_back(latencies[latencies.size() * 0.999]); // p99.9
    percentiles.push_back(latencies[latencies.size() * 0.9999]); // p99.99

    return percentiles;
}

double calculate_avg(const std::vector<long long>& latencias_micros) {
    if (latencias_micros.empty()) {
        return 0.0;
    }

    long long soma = 0;
    for (auto latencia : latencias_micros) {
        soma += latencia;
    }

    return static_cast<double>(soma) / latencias_micros.size();
}

void set_thread_affinity(std::thread& thread, int core_idx) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_list[core_idx], &cpuset);

    int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        perror("pthread_setaffinity_np:");
        exit(-1);
    }
}

// Função para processar comandos SET e GET em uma thread
void process_commands(int thread_id, int num_requests_per_thread, const std::string& server_ip, int server_port, std::vector<long long>& latencies) {
    int sock = connect_to_server(server_ip, server_port);
    
    std::string key_base = "key";
    std::string value_base = "value";

    for (int i = 0; i < num_requests_per_thread; ++i) {
        std::string key = key_base + std::to_string(thread_id * num_requests_per_thread + i);
        std::string value = value_base + std::to_string(thread_id * num_requests_per_thread + i);

        // std::string set_command = "SET " + key + " " + value;
        // long long set_latency = send_command_and_measure_latency(sock, set_command);

        std::string get_command = "GET " + key;
        long long get_latency = send_command_and_measure_latency(sock, get_command);

        mtx.lock();
        // latencies.push_back(set_latency);
        latencies.push_back(get_latency);
        mtx.unlock();
    }

    close(sock);
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        fprintf(stderr, "USE: %s <server_ip> <server_port> <num_requests> <num_threads>\n", argv[0]);
        exit(-1);
    }

    std::string server_ip = argv[1];
    int server_port = std::stoi(argv[2]);
    int num_requests = std::stoi(argv[3]);
    int num_threads = std::stoi(argv[4]);

    int num_requests_per_thread = num_requests / num_threads;

    std::vector<long long> latencies;

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_commands, i, num_requests_per_thread, std::ref(server_ip), server_port + i, std::ref(latencies));
        set_thread_affinity(threads.back(), i);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto avg = calculate_avg(latencies);
    auto percentiles = calculate_percentiles(latencies);
    double total_time = std::accumulate(latencies.begin(), latencies.end(), 0LL) / 1e6;
    double throughput = (num_requests * num_threads) / total_time;

    printf("Latency (us):\n");
    printf("avg: %lf\n", avg);
    printf("p50: %lld\n", percentiles[0]);
    printf("p75: %lld\n", percentiles[1]);
    printf("p90: %lld\n", percentiles[2]);
    printf("p99: %lld\n", percentiles[3]);
    printf("p99.9: %lld\n", percentiles[4]);
    printf("p99.99: %lld\n", percentiles[5]);
    printf("Throughput: %lf RPS\n", throughput);

    return 0;
}
