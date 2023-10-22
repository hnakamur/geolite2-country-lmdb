#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>

#include <maxminddb.h>

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

    const char *lookup_path[] = {
        "country",
        "iso_code",
        NULL,
    }; 

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

        const char *iso_code = "-";
        uint32_t data_size = 1;
        MMDB_entry_data_list_s *entry_data_list = NULL;
        if (result.found_entry) {
            MMDB_entry_data_s entry_data;
            status = MMDB_aget_value(&result.entry, &entry_data, lookup_path);
            if (status == MMDB_SUCCESS && entry_data.offset) {
                MMDB_entry_s entry = {
                    .mmdb = &mmdb,
                    .offset = entry_data.offset,
                };
                status = MMDB_get_entry_data_list(&entry, &entry_data_list);
                if (status != MMDB_SUCCESS) {
                    fprintf(stderr, "get_entry_data_list error for ip=%s - %s\n", str_addr, MMDB_strerror(status));
                    return 1;
                }
                if (entry_data_list != NULL) {
                    MMDB_entry_data_s *entry_data = &entry_data_list->entry_data;
                    if (entry_data->has_data && entry_data->type == MMDB_DATA_TYPE_UTF8_STRING) {
                        iso_code = entry_data->utf8_string;
                        data_size = entry_data->data_size;
                    }
                }
            }
        }
        printf("%s\t%.*s\n", str_addr, data_size, iso_code);
        if (entry_data_list != NULL) {
            MMDB_free_entry_data_list(entry_data_list);
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
