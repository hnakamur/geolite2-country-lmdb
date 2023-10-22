package comparetoofficialimpl

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/hnakamur/geolite2countrylmdb"
)

func TestParallelCompareToOfficial(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	fr, err := os.Open("../c/lookup_all_result.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer fr.Close()

	r := csv.NewReader(fr)
	// skip header
	_, err = r.Read()
	if err != nil {
		t.Fatal(err)
	}

	lmdbPath := t.TempDir()
	env, dbi := setupLMDB(t, lmdbPath)
	defer env.Close()

	if err := env.Update(func(txn *lmdb.Txn) (err error) {
		return geolite2countrylmdb.SetupCountry(mmdbPath, dbi)(txn)
	}); err != nil {
		t.Fatal(err)
	}

	type job struct {
		ip                     string
		wantCountry            string
		wantRegisteredCountry  string
		wantRepresentedCountry string
	}

	runJob := func(job job) error {
		ip := netip.MustParseAddr(job.ip)
		var country, registeredCountry, representedCountry string
		if err := env.View(func(txn *lmdb.Txn) (err error) {
			if err := geolite2countrylmdb.LookupCountry(dbi, ip,
				&country, &registeredCountry, &representedCountry)(txn); err != nil {
				if !lmdb.IsNotFound(err) {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("lookup failed: ip=%s, %s", job.ip, err)
		}

		if country != job.wantCountry ||
			registeredCountry != job.wantRegisteredCountry ||
			representedCountry != job.wantRepresentedCountry {
			return fmt.Errorf("result mismatch: ip=%s, "+
				"gotCountry=%s, wantCountry=%s, "+
				"gotRegisteredCountry=%s, wantRegisteredCountry=%s, "+
				"gotRepresentedCountry=%s, wantRepresentedCountry=%s",
				job.ip,
				country, job.wantCountry,
				registeredCountry, job.wantRegisteredCountry,
				representedCountry, job.wantRepresentedCountry)
		}
		return nil
	}

	workerCount := getWorkerCount(t)
	log.Printf("workerCount=%d", workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	jobC := make(chan job, workerCount)
	errC := make(chan error, workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for job := range jobC {
				errC <- runJob(job)
			}
		}()
	}

	fatalErrC := make(chan error, 1)
	go func() {
		defer close(jobC)
		for {
			record, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				fatalErrC <- err
				return
			}

			if len(record) != 4 {
				fatalErrC <- fmt.Errorf("unexpected record field count, records=%v, got=%d, want=%d",
					strings.Join(record, ","), len(record), 4)
				return
			}
			jobC <- job{
				ip:                     record[0],
				wantCountry:            record[1],
				wantRegisteredCountry:  record[2],
				wantRepresentedCountry: record[3],
			}
		}
	}()

	t0 := time.Now()
	var n uint32
loop:
	for {
		select {
		case err := <-errC:
			if err != nil {
				t.Error(err)
			}
			n++
			if n&0x00FFFFFF == 0 {
				log.Printf("%.2f%% done, elapsed=%s", float64(n)*100.0/math.MaxUint32, time.Since(t0))
			}
			if n == math.MaxUint32 {
				break loop
			}
		case err := <-fatalErrC:
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func getWorkerCount(t *testing.T) int {
	numCPU := runtime.NumCPU()
	s := os.Getenv("WORKER_COUNT")
	if s == "" {
		return numCPU
	}
	c, err := strconv.ParseInt(s, 10, 64)
	if err != nil || c <= 0 {
		t.Fatalf("WORKER_COUNT environment variable must be positive integer, but got=%s", s)
	}
	return int(c)
}
