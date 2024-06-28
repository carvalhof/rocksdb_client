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

#define BUFFER_SIZE 32768

unsigned seed;
std::mutex mtx;
long total_latencies;
double global_throughput;
std::vector<int> core_list;
std::vector<long long> global_latency;

int key_size = 8;
int value_size = 16;

int connect_to_server(const std::string& server_ip, int server_port) {
    int sock = 0;
    struct sockaddr_in serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) {
        perror("inet_pton");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        return -1;
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

double calculate_throughput() {
    return global_throughput;
}

std::vector<long long> calculate_percentiles(std::vector<std::vector<long long>>& latencies) {
    for (auto latency_i : latencies) {
        for (auto latency : latency_i) {
            global_latency.push_back(latency);
        }
    }

    std::vector<long long> percentiles;
    if (global_latency.size()) {
        std::sort(global_latency.begin(), global_latency.end());

        percentiles.push_back(global_latency[global_latency.size() * 0.50]); // p50
        percentiles.push_back(global_latency[global_latency.size() * 0.75]); // p75
        percentiles.push_back(global_latency[global_latency.size() * 0.90]); // p90
        percentiles.push_back(global_latency[global_latency.size() * 0.99]); // p99
        percentiles.push_back(global_latency[global_latency.size() * 0.999]); // p99.9
        percentiles.push_back(global_latency[global_latency.size() * 0.9999]); // p99.99
    }

    return percentiles;
}

double calculate_avg(const std::vector<std::vector<long long>>& latencies) {
    if (latencies.empty()) {
        return 0.0;
    }

    double avg = 0;
    for (auto latency_i : latencies) {
        long long sum = 0;
        for (auto latency : latency_i) {
            sum += latency;
        }
        avg += (static_cast<double>(sum) / latency_i.size());
    }

    return avg / latencies.size();
}

void set_thread_affinity(pthread_t thread, int core_idx) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_list[core_idx], &cpuset);

    int rc = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        perror("pthread_setaffinity_np");
        exit(-1);
    }
}

void inner_thread_function(int thread_id, int idx, int num_requests_per_thread, const std::string& server_ip, int server_port, std::vector<long long>& latencies, double get_ratio) {
    set_thread_affinity(pthread_self(), thread_id);

    int sock = connect_to_server(server_ip, server_port + thread_id);

    if (sock == -1) {
        return;
    }

    std::mt19937 gen(seed + thread_id + (idx + 1));
    std::uniform_real_distribution<> dis(0.0, 1.0);

    std::string key_base = "key";
    std::string value_base = "value";

    for (int i = 0; i < num_requests_per_thread; i++) {
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
            command = "SCAN";
            latency = send_scan_and_measure_latency(sock, command);
        }

        latencies.push_back(latency);
    }

    close(sock);
}

void outer_thread_function(int thread_id, int warming_up_requests_per_conn, int num_requests_per_thread, int num_threads_per_conn, const std::string& server_ip, int server_port, std::vector<long long>& latencies, double get_ratio) {
    set_thread_affinity(pthread_self(), thread_id);

    int sock = connect_to_server(server_ip, server_port + thread_id);

    if (sock == -1) {
        return;
    }
    
    std::mt19937 gen(seed + thread_id);
    std::uniform_real_distribution<> dis(0.0, 1.0);

    std::string key_base = "key";
    std::string value_base = "value";

    /* Warming up */
    for (int i = 0; i < warming_up_requests_per_conn; i++) {
        std::ostringstream key_stream;
        key_stream << key_base << std::setw(key_size - key_base.length()) << std::setfill('0') << (thread_id * num_requests_per_thread + i);
        std::string key = key_stream.str();

        std::ostringstream value_stream;
        value_stream << value_base << std::setw(value_size - value_base.length()) << std::setfill('0') << (thread_id * num_requests_per_thread + i);
        std::string value = value_stream.str();

        std::string set_command = "SET " + key + " " + value;
        send_command(sock, set_command);
    }
    for (int i = 0; i < warming_up_requests_per_conn; i++) {
        std::ostringstream key_stream;
        key_stream << key_base << std::setw(key_size - key_base.length()) << std::setfill('0') << (thread_id * num_requests_per_thread + i);
        std::string key = key_stream.str();

        std::string get_command = "GET " + key;
        send_command(sock, get_command);
    }

    std::vector<std::thread> inner_threads;
    std::vector<std::vector<long long>> latencies_i(num_threads_per_conn - 1);

    for (int i = 0; i < num_threads_per_conn - 1; i++) {
        inner_threads.emplace_back(inner_thread_function, thread_id, i, num_requests_per_thread, server_ip, server_port, std::ref(latencies_i[i]), get_ratio);
    }

    for (int i = 0; i < num_requests_per_thread; i++) {
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
            command = "SCAN";
            latency = send_scan_and_measure_latency(sock, command);
        }

        latencies.push_back(latency);
    }

    for (auto& thread : inner_threads) {
        thread.join();
    }

    double throughput = 0.0;
    for(auto ll : latencies_i) {
        std::sort(ll.begin(), ll.end());
        double total_time_i = std::accumulate(ll.begin(), ll.end(), 0LL) / 1e6;
        throughput += (num_requests_per_thread / total_time_i);
    }

    std::sort(latencies.begin(), latencies.end());
    double total_time_i = std::accumulate(latencies.begin(), latencies.end(), 0LL) / 1e6;
    throughput += (num_requests_per_thread / total_time_i);

    for(auto ll : latencies_i) {
        for(auto l : ll) {
            latencies.push_back(l);
        }
    }

    mtx.lock();
    total_latencies += latencies.size();
    global_throughput += throughput;
    mtx.unlock();

    close(sock);
}

void usage(char *app) {
    fprintf(stderr, 
        "Usage: %s -h <server_ip> -p <server_port> -n <num_requests> -c <num_conn> -l <list_of_cores> -o <output file> -m <GET/SCAN proportion> [-f] [-s <seed>] [-w <warming up>] [-k <key size>] [-v <value size>]\n", app
    );
}

int main(int argc, char* argv[]) {
    std::string server_ip;
    int server_port = 0;
    int num_requests = 0;
    int num_conn = 0;
    bool direct_output = false;
    double get_ratio = 2.0;
    int warming_up_requests = 0;
    int num_threads_per_conn = 1;
    FILE *fp = stdout;
    seed = std::chrono::system_clock::now().time_since_epoch().count();

    int opt;
    while ((opt = getopt(argc, argv, "h:p:n:c:t:l:fm:s:o:w:k:v:")) != -1) {
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
            case 'c':
                num_conn = std::stoi(optarg);
                break;
            case 't':
                num_threads_per_conn = std::stoi(optarg);
                if(num_threads_per_conn < 1)
                    exit(-1);
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
            case 'o':
                fp = fopen(optarg, "w");
                if (!fp) {
                    perror("fopen");
                    exit(EXIT_FAILURE);
                }
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

    if (server_ip.empty() || server_port == 0 || num_requests == 0 || num_conn == 0 || core_list.empty() || get_ratio > 1.0) {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    int warming_up_requests_per_conn = warming_up_requests / num_threads_per_conn;
    int num_requests_per_thread = num_requests / num_threads_per_conn;

    std::vector<std::thread> threads;
    std::vector<std::vector<long long>> latencies(num_conn);

    for (int i = 0; i < num_conn; i++) {
        threads.emplace_back(outer_thread_function, i, warming_up_requests_per_conn, num_requests_per_thread, num_threads_per_conn, std::ref(server_ip), server_port, std::ref(latencies[i]), get_ratio);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto percentiles = calculate_percentiles(latencies);
    if (percentiles.size()) {
        auto avg = calculate_avg(latencies);
        auto throughput = calculate_throughput();

        if (direct_output) {
            fprintf(fp, "%lf,%lld,%lld,%lf,%ld\n",
                avg, 
                percentiles[0], 
                percentiles[4], 
                throughput,
                total_latencies);
        } else {
            fprintf(fp, "Latency (us):\n");
            fprintf(fp, "avg: %lf\n", avg);                      // AVG
            fprintf(fp, "p50: %lld\n", percentiles[0]);          // p50
            // fprintf(fp, "p75: %lld\n", percentiles[1]);       // p75
            // fprintf(fp, "p90: %lld\n", percentiles[2]);       // p90
            // fprintf(fp, "p99: %lld\n", percentiles[3]);       // p99
            fprintf(fp, "p99.9: %lld\n", percentiles[4]);        // p99.9
            // fprintf(fp, "p99.99: %lld\n", percentiles[5]);    // p99.99
            fprintf(fp, "Throughput: %lf RPS\n", throughput);
            fprintf(fp, "#Latency: %ld\n", total_latencies);
        }
    }
    
    return 0;
}
