#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>

#include <maxminddb.h>

#define PATH_COUNT 3

int lookup_all(const char *const filename) {
    MMDB_s mmdb;
    int status = MMDB_open(filename, MMDB_MODE_MMAP, &mmdb);
    if (status != MMDB_SUCCESS) {
        fprintf(stderr, "cannot open maxminddb file %s - %s\n", filename, MMDB_strerror(status));
        if (status == MMDB_IO_ERROR) {
            fprintf(stderr, "    IO error: %s\n", strerror(errno));
        }
        return 1;
    }
    
    const char *country_lookup_paths[PATH_COUNT][3] = {
        {"country", "iso_code", NULL},
        {"registered_country", "iso_code", NULL},
        {"represented_country", "iso_code", NULL},
    };

    printf("%s,%s,%s,%s\n", "ip", "country", "registered_country", "represented_country");
    struct sockaddr_in sockaddr;
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = 0;
    uint32_t i = 0;
    do {
        sockaddr.sin_addr.s_addr = ntohl(i);
        char *str_addr = inet_ntoa(sockaddr.sin_addr);
        MMDB_lookup_result_s result = MMDB_lookup_sockaddr(&mmdb,
            (const struct sockaddr *) &sockaddr, &status);
        if (status != MMDB_SUCCESS) {
            fprintf(stderr, "lookup error for ip=%s - %s\n", str_addr, MMDB_strerror(status));
            return 1;
        }

        const char *country[PATH_COUNT] = {"", "", ""};
        uint32_t country_len[PATH_COUNT] = {0, 0, 0};
        MMDB_entry_data_list_s *entry_data_list[PATH_COUNT] = {NULL, NULL, NULL};
        if (result.found_entry) {
            MMDB_entry_data_s entry_data[PATH_COUNT];
            for (int j = 0; j < PATH_COUNT; j++) {
                status = MMDB_aget_value(&result.entry, &entry_data[j], country_lookup_paths[j]);
                if (status == MMDB_SUCCESS && entry_data[j].offset) {
                    MMDB_entry_s entry = {
                        .mmdb = &mmdb,
                        .offset = entry_data[j].offset,
                    };
                    status = MMDB_get_entry_data_list(&entry, &entry_data_list[j]);
                    if (status != MMDB_SUCCESS) {
                        fprintf(stderr, "get_entry_data_list error for ip=%s - %s\n", str_addr, MMDB_strerror(status));
                        return 1;
                    }
                    if (entry_data_list[j] != NULL) {
                        MMDB_entry_data_s *entry_data = &entry_data_list[j]->entry_data;
                        if (entry_data->has_data && entry_data->type == MMDB_DATA_TYPE_UTF8_STRING) {
                            country[j] = entry_data->utf8_string;
                            country_len[j] = entry_data->data_size;
                        }
                    }
                }
            }
        }
        printf("%s,%.*s,%.*s,%.*s\n", str_addr,
            country_len[0], country[0],
            country_len[1], country[1],
            country_len[2], country[2]);
        for (int j = 0; j < PATH_COUNT; j++) {
            if (entry_data_list[j] != NULL) {
                MMDB_free_entry_data_list(entry_data_list[j]);
            }
        }

        i++;
    } while (i != 0xFFFFFFFF);
    MMDB_close(&mmdb);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: lookup_all mmdb_country_filename\n");
        return 1;
    }
    return lookup_all(argv[1]);
}
