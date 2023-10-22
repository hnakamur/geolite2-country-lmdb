lookup_all: lookup_all.c
	$(CC) -o $@ $^ -lmaxminddb
