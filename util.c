#include "util.h"

uint32_t key_size;
uint32_t value_size;

int distribution;
double get_proportion;
char output_file[MAXSTRLEN];

// Sample the value using Exponential Distribution
double sample_exponential(double lambda) {
	double u = (double)rand() / RAND_MAX; // Uniform random number [0,1]
	return -log(1 - u) / lambda;
}

// Convert string type into int type
static uint32_t process_int_arg(const char *arg) {
	char *end = NULL;

	return strtoul(arg, &end, 10);
}

// Convert string type into double type
static double process_double_arg(const char *arg) {
	char *end;

	return strtod(arg, &end);
}

static void generate_key(char *command, const char *key_base, int key) {
    int num_zeros = key_size - strlen(key_base);
    sprintf(command, "%s%0*d", key_base, num_zeros, key);
}

static void generate_value(char *command, const char *value_base, int value) {
    int num_zeros = value_size - strlen(value_base);
    sprintf(command, "%s%0*d", value_base, num_zeros, value);
}

void create_warming_array() {
	if(nr_warming == 0)
		return;

	warming_array = (application_node_t*) rte_malloc(NULL, nr_warming * sizeof(application_node_t), 64);
	if(warming_array == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot alloc the warming array.\n");
	}

	flow_indexes_for_warming = (uint16_t*) rte_malloc(NULL, nr_warming * sizeof(uint16_t), 64);
	if(flow_indexes_for_warming == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot alloc the flow_indexes_for_warming array.\n");
	}

	for(uint32_t i = 0; i < nr_warming; i++) {
		char key[key_size + 1];
		char value[value_size + 1];

		generate_key(key, "key", i);
		generate_value(value, "value", i);

		uint32_t size = strlen("SET ") + key_size + 1 + value_size + 1;
		warming_array[i].size = size;
		warming_array[i].command = (char*) malloc(size);

		memcpy(warming_array[i].command, "SET ", 4);
		memcpy(warming_array[i].command + 4, key, key_size);
		memcpy(warming_array[i].command + 4 + key_size, " ", 1);
		memcpy(warming_array[i].command + 4 + key_size + 1, value, value_size);
		memcpy(warming_array[i].command + 4 + key_size + 1 + value_size, "\n", 1);

		flow_indexes_for_warming[i] = i % nr_flows;
	}
}

// Allocate and create all application nodes
void create_application_array() {
	char key[key_size + 1];
	uint64_t nr_elements = rate * duration;

	application_array = (application_node_t*) rte_malloc(NULL, nr_elements * sizeof(application_node_t), 64);
	if(application_array == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot alloc the application array.\n");
	}

	for(uint32_t i = 0; i < nr_elements; i++) {
		double u = rte_drand();
		if(u < get_proportion) {
			// GET
			uint32_t key_to_get = nr_warming ? rte_rand() % nr_warming : i;
			generate_key(key, "key", key_to_get);

			uint32_t size = strlen("GET ") + key_size + 1;
			application_array[i].size = size;
			application_array[i].command = (char*) malloc(size);

			memcpy(application_array[i].command, "GET ", 4);
			memcpy(application_array[i].command + 4, key, key_size);
			memcpy(application_array[i].command + 4 + key_size, "\n", 1);
		} else {
			// SCAN
			uint32_t size = strlen("SCAN\n");
			application_array[i].size = size;
			application_array[i].command = (char*) malloc(size);
			memcpy(application_array[i].command, "SCAN\n", size);
		}
	}
}

// Allocate and create all nodes for incoming packets
void create_incoming_array() {
	incoming_array = (node_t*) rte_malloc(NULL, rate * duration * sizeof(node_t), 0);
	if(incoming_array == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot alloc the incoming array.\n");
	}
} 

// Allocate and create an array for all interarrival packets for rate specified.
void create_interarrival_array() {
	uint64_t nr_elements = rate * duration;

	interarrival_gap = (uint32_t*) rte_malloc(NULL, nr_elements * sizeof(uint32_t), 64);
	if(interarrival_gap == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot alloc the interarrival_gap array.\n");
	}

	if(distribution == UNIFORM_VALUE) {
		// Uniform
		double mean = (1.0/rate) * 1000000.0;
		for(uint64_t j = 0; j < nr_elements; j++) {
			interarrival_gap[j] = mean * TICKS_PER_US;
		}
	} else if(distribution == EXPONENTIAL_VALUE) {
		// Exponential
		double lambda = 1.0/(1000000.0/rate);
		for(uint64_t j = 0; j < nr_elements; j++) {
			interarrival_gap[j] = sample_exponential(lambda) * TICKS_PER_US;
		}
	} else {
		exit(-1);
	}
}

// Allocate and create an array for all flow indentier to send to the server
void create_flow_indexes_array() {
	uint64_t nr_elements = rate * duration;

	flow_indexes_array = (uint16_t*) rte_malloc(NULL, nr_elements * sizeof(uint16_t), 64);
	if(flow_indexes_array == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot alloc the flow_indexes array.\n");
	}

	uint32_t last = 0;
	for(uint64_t i = 0; i < nr_flows; i++) {
		flow_indexes_array[last++] = i;
	}

	for(uint64_t i = last; i < nr_elements; i++) {
		flow_indexes_array[i] = i % nr_flows;
	}
}

// Clean up all allocate structures
void clean_heap() {
	rte_free(incoming_array);
	rte_free(flow_indexes_array);
	rte_free(interarrival_gap);
	rte_free(application_array);
}

// Usage message
static void usage(const char *prgname) {
	printf("%s [EAL options] -- \n"
		"  -d DISTRIBUTION: <uniform|exponential>\n"
		"  -r RATE: rate in pps\n"
		"  -f FLOWS: number of flows\n"
		"  -k SIZE: KEY size in bytes\n"
		"  -v SIZE: VALUE size in bytes\n"
		"  -w WARMING: number of keys to warming up\n"
		"  -t TIME: time in seconds to send packets\n"
		"  -s SEED: seed\n"
		"  -m GET_PROPORTION: proportion for GET/SCAN\n"
		"  -c FILENAME: name of the configuration file\n"
		"  -o FILENAME: name of the output file\n",
		prgname
	);
}

// Parse the argument given in the command line of the application
int app_parse_args(int argc, char **argv) {
	int opt, ret;
	char **argvopt;
	char *prgname = argv[0];

	key_size = 8;
	value_size = 16;
	nr_warming = 0;

	argvopt = argv;
	while ((opt = getopt(argc, argvopt, "d:r:f:k:v:w:t:s:m:c:o:")) != EOF) {
		switch (opt) {
		// distribution on the client
		case 'd':
			if(strcmp(optarg, "uniform") == 0) {
				// Uniform distribution
				distribution = UNIFORM_VALUE;
			} else if(strcmp(optarg, "exponential") == 0) {
				// Exponential distribution
				distribution = EXPONENTIAL_VALUE;
			} else {
				usage(prgname);
				rte_exit(EXIT_FAILURE, "Invalid arguments.\n");
			}
			break;

		// rate (pps)
		case 'r':
			rate = process_int_arg(optarg);
			assert(rate > 0);
			break;

		// flows
		case 'f':
			nr_flows = process_int_arg(optarg);
			assert(nr_flows > 0);
			break;

		// key size (in bytes)
		case 'k':
			key_size = process_int_arg(optarg);
			assert(key_size > 0);
			break;

		// value size (in bytes)
		case 'v':
			value_size = process_int_arg(optarg);
			assert(value_size > 0);
			break;
		
		// number of warming (number of keys)
		case 'w':
			nr_warming = process_int_arg(optarg);
			break;

		// duration (in seconds)
		case 't':
			duration = process_int_arg(optarg);
			assert(duration > 0);
			break;
		
		// seed
		case 's':
			seed = process_int_arg(optarg);
			break;

		// GET proportion
		case 'm':
			get_proportion = process_double_arg(optarg);
			break;

		// config file name
		case 'c':
			process_config_file(optarg);
			break;
		
		// output mode
		case 'o':
			strcpy(output_file, optarg);
			break;

		default:
			usage(prgname);
			rte_exit(EXIT_FAILURE, "Invalid arguments.\n");
		}
	}

	if(optind >= 0) {
		argv[optind - 1] = prgname;
	}

	ret = optind-1;
	optind = 1;

	return ret;
}

// Wait for the duration parameter
void wait_timeout() {
	uint32_t remaining_in_s = 5;
	rte_delay_us_sleep((duration + remaining_in_s) * 1000000);

	// set quit flag for all internal cores
	quit_rx = 1;
	quit_tx = 1;
	quit_rx_ring = 1;
}

// Compare two double values (for qsort function)
int cmp_func(const void * a, const void * b) {
	double da = (*(double*)a);
	double db = (*(double*)b);

	return (da - db) > ( (fabs(da) < fabs(db) ? fabs(db) : fabs(da)) * EPSILON);
}

// Print stats into output file
void print_stats_output() {
	uint64_t total_never_sent = nr_never_sent;
	
	if((incoming_idx + total_never_sent) != rate * duration) {
		printf("ERROR: received %d and %ld never sent\n", incoming_idx, total_never_sent);
		return;
	}
	
	// open the file
	FILE *fp = fopen(output_file, "w");
	if(fp == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot open the output file.\n");
	}

	printf("\nincoming_idx = %d -- never_sent = %ld\n", incoming_idx, total_never_sent);

	// print the RTT latency in (ns)
	node_t *cur;
	for(uint64_t j = 0; j < incoming_idx; j++) {
		cur = &incoming_array[j];

		fprintf(fp, "%lu\n", ((uint64_t)((cur->timestamp_rx - cur->timestamp_tx)/((double)TICKS_PER_US/1000))));
	}

	// close the file
	fclose(fp);
}

// Process the config file
void process_config_file(char *cfg_file) {
	// open the file
	struct rte_cfgfile *file = rte_cfgfile_load(cfg_file, 0);
	if(file == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot load configuration profile %s\n", cfg_file);
	}

	// load ethernet addresses
	char *entry = (char*) rte_cfgfile_get_entry(file, "ethernet", "src");
	if(entry) {
		rte_ether_unformat_addr((const char*) entry, &src_eth_addr);
	}
	entry = (char*) rte_cfgfile_get_entry(file, "ethernet", "dst");
	if(entry) {
		rte_ether_unformat_addr((const char*) entry, &dst_eth_addr);
	}

	// load ipv4 addresses
	entry = (char*) rte_cfgfile_get_entry(file, "ipv4", "src");
	if(entry) {
		uint8_t b3, b2, b1, b0;
		sscanf(entry, "%hhd.%hhd.%hhd.%hhd", &b3, &b2, &b1, &b0);
		src_ipv4_addr = IPV4_ADDR(b3, b2, b1, b0);
	}
	entry = (char*) rte_cfgfile_get_entry(file, "ipv4", "dst");
	if(entry) {
		uint8_t b3, b2, b1, b0;
		sscanf(entry, "%hhd.%hhd.%hhd.%hhd", &b3, &b2, &b1, &b0);
		dst_ipv4_addr = IPV4_ADDR(b3, b2, b1, b0);
	}

	// load TCP destination port
	entry = (char*) rte_cfgfile_get_entry(file, "tcp", "dst");
	if(entry) {
		uint16_t port;
		sscanf(entry, "%hu", &port);
		dst_tcp_port = port;
	}

	// close the file
	rte_cfgfile_close(file);
}

// Fill the data into packet payload properly
inline void fill_payload_pkt(struct rte_mbuf *pkt, uint32_t idx, uint64_t value) {
	uint8_t *payload = (uint8_t*) rte_pktmbuf_mtod_offset(pkt, uint8_t*, PAYLOAD_OFFSET);

	((uint64_t*) payload)[idx] = value;
}

// Fill the data into packet payload properly
inline void fill_payload_data(struct rte_mbuf *pkt, uint32_t i) {
	uint8_t *payload = (uint8_t*) rte_pktmbuf_mtod_offset(pkt, uint8_t*, PAYLOAD_OFFSET);

	memcpy(payload + 2*sizeof(uint64_t), application_array[i].command, application_array[i].size);
}

// Fill the data into packet payload properly
inline void fill_warming_data(struct rte_mbuf *pkt, uint32_t i) {
	uint8_t *payload = (uint8_t*) rte_pktmbuf_mtod_offset(pkt, uint8_t*, PAYLOAD_OFFSET);

	memcpy(payload + 2*sizeof(uint64_t), warming_array[i].command, warming_array[i].size);
}