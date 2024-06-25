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
#include <random>
#include <sstream>
#include <iomanip>
#include <stdint.h>

#define BUFFER_SIZE 2048

unsigned seed;
std::mutex mtx;
std::vector<int> core_list;

int key_size = 8;
int value_size = 16;

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

void send_command(int sock, const std::string& command) {
    char buffer[BUFFER_SIZE] = {0};

    send(sock, command.c_str(), command.size(), 0);
    int ret __attribute__((unused)) = read(sock, buffer, BUFFER_SIZE);
}

long long send_scan_and_measure_latency(int sock, const std::string& command) {
    int ret __attribute__((unused)) = 0;
    uint64_t remaining = 0;
    uint64_t response_len = 0;
    char buffer[BUFFER_SIZE] = {0};
    auto start = std::chrono::high_resolution_clock::now();

    send(sock, command.c_str(), command.size(), 0);
    ret = read(sock, &response_len, sizeof(uint64_t));

    remaining = response_len;
    while(remaining > 0) {
        remaining -= read(sock, buffer, BUFFER_SIZE);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<long long, std::micro> duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    return duration.count();
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

void process_commands(int thread_id, int warming_up_requests_per_thread, int num_requests_per_thread, const std::string& server_ip, int server_port, std::vector<long long>& latencies, double get_ratio) {
    int sock = connect_to_server(server_ip, server_port + thread_id);
    
    std::mt19937 gen(seed + thread_id);
    std::uniform_real_distribution<> dis(0.0, 1.0);

    std::string key_base = "key";
    std::string value_base = "value";

    /* Warming up */
    for (int i = 0; i < warming_up_requests_per_thread; i++) {
        std::ostringstream key_stream;
        key_stream << key_base << std::setw(key_size - key_base.length()) << std::setfill('0') << (thread_id * num_requests_per_thread + i);
        std::string key = key_stream.str();

        std::ostringstream value_stream;
        value_stream << value_base << std::setw(value_size - value_base.length()) << std::setfill('0') << (thread_id * num_requests_per_thread + i);
        std::string value = value_stream.str();

        std::string set_command = "SET " + key + " " + value;
        send_command(sock, set_command);
    }

    for (int i = 0; i < num_requests_per_thread; ++i) {
        double rand_value = dis(gen);
        std::ostringstream key_stream;
        key_stream << key_base << std::setw(key_size - key_base.length()) << std::setfill('0') << (thread_id * num_requests_per_thread + i);
        std::string key = key_stream.str();

        std::ostringstream value_stream;
        value_stream << value_base << std::setw(value_size - value_base.length()) << std::setfill('0') << (thread_id * num_requests_per_thread + i);
        std::string value = value_stream.str();

        long long latency;
        std::string command;

        if (rand_value < get_ratio) {
            command = "GET " + key;
            latency = send_command_and_measure_latency(sock, command);
        } else {
            // command = "SET " + key + " " + value;
            command = "SCAN";
            latency = send_scan_and_measure_latency(sock, command);
        }
        // latency = send_command_and_measure_latency(sock, command);

        mtx.lock();
        latencies.push_back(latency);
        mtx.unlock();
    }

    close(sock);
}

void usage(char *app) {
    fprintf(stderr, 
        "Usage: %s \
        -h <server_ip> \
        -p <server_port> \
        -n <num_requests> \
        -t <num_threads> \
        -l <list_of_cores> \
        -m <GET/SCAN proportion> \
        [-f] \
        [-s <seed>] \
        [-w <warming up>] \
        [-k <key size>] \
        [-v <value size>]\n", app
    );
}

int main(int argc, char* argv[]) {
    std::string server_ip;
    int server_port = 0;
    int num_requests = 0;
    int num_threads = 0;
    bool direct_output = false;
    double get_ratio = 2.0;
    int warming_up_requests = 0;
    seed = std::chrono::system_clock::now().time_since_epoch().count();

    int opt;
    while ((opt = getopt(argc, argv, "h:p:n:t:l:fm:s:w:k:v:")) != -1) {
        switch (opt) {
            case 'h':
                server_ip = optarg;
                break;
            case 'p':
                server_port = std::stoi(optarg);
                break;
            case 'n':
                num_requests = std::stoi(optarg);
                break;
            case 't':
                num_threads = std::stoi(optarg);
                break;
            case 'l': {
                std::string list_str = optarg;
                std::stringstream ss(list_str);
                std::string item;
                while (std::getline(ss, item, ',')) {
                    core_list.push_back(std::stoi(item));
                }
                break;
            }
            case 'f':
                direct_output = true;
                break;
            case 'm':
                get_ratio = std::stod(optarg);
                if (get_ratio < 0.0 || get_ratio > 1.0) {
                    fprintf(stderr, "-m <proportion>: should be between 0.0 and 1.0\n");
                    exit(EXIT_FAILURE);
                }
                break;
            case 's':
                seed = std::stoul(optarg);
                break;
            case 'w':
                warming_up_requests = std::stoi(optarg);
                break;
            case 'k':
                key_size = std::stoi(optarg);
                break;
            case 'v':
                value_size = std::stoi(optarg);
                break;
            default:
                usage(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (server_ip.empty() || server_port == 0 || num_requests == 0 || num_threads == 0 || core_list.empty() || get_ratio > 1.0) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    int warming_up_requests_per_thread = warming_up_requests / num_threads;
    int num_requests_per_thread = num_requests / num_threads;

    std::vector<long long> latencies;

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_commands, i, warming_up_requests_per_thread, num_requests_per_thread, std::ref(server_ip), server_port, std::ref(latencies), get_ratio);
        set_thread_affinity(threads.back(), i);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto avg = calculate_avg(latencies);
    auto percentiles = calculate_percentiles(latencies);
    double total_time = std::accumulate(latencies.begin(), latencies.end(), 0LL) / 1e6;
    double throughput = (num_requests * num_threads) / total_time;

    if (direct_output) {
        printf("%lf,%lld,%lld,%lf\n", 
            avg, 
            percentiles[0], 
            percentiles[4], 
            throughput);
    } else {
        printf("Latency (us):\n");
        printf("avg: %lf\n", avg);                      // AVG
        printf("p50: %lld\n", percentiles[0]);          // p50
        // printf("p75: %lld\n", percentiles[1]);       // p75
        // printf("p90: %lld\n", percentiles[2]);       // p90
        // printf("p99: %lld\n", percentiles[3]);       // p99
        printf("p99.9: %lld\n", percentiles[4]);        // p99.9
        // printf("p99.99: %lld\n", percentiles[5]);    // p99.99
        printf("Throughput: %lf RPS\n", throughput);
    }

    return 0;
}
