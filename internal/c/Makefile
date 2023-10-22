lookup_all_result.txt: lookup_all
	./lookup_all ../../GeoLite2-Country.mmdb > $@

lookup_all: lookup_all.c
	$(CC) -o $@ $^ -lmaxminddb
