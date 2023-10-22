package comparetoofficialimpl

import (
	"encoding/csv"
	"io"
	"log"
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/hnakamur/geolite2countrylmdb"
)

const mmdbPath = "../../GeoLite2-Country.mmdb"

func TestCompareToOfficial(t *testing.T) {
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

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}

		if len(record) != 4 {
			t.Fatalf("unexpected record field count, records=%v, got=%d, want=%d",
				strings.Join(record, ","), len(record), 4)
		}
		ip := netip.MustParseAddr(record[0])
		var country, registeredCountry, representedCountry string
		if err := env.View(func(txn *lmdb.Txn) (err error) {
			if err := geolite2countrylmdb.LookupCountry(dbi, ip, &country, &registeredCountry, &representedCountry)(txn); err != nil {
				if !lmdb.IsNotFound(err) {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if want := record[1]; country != want {
			t.Errorf("country mismatch for IP=%s, got=%s, want=%s", ip, country, want)
		}
		if want := record[2]; registeredCountry != want {
			t.Errorf("registeredCountry mismatch for IP=%s, got=%s, want=%s", ip, registeredCountry, want)
		}
		if want := record[3]; representedCountry != want {
			t.Errorf("representedCountry mismatch for IP=%s, got=%s, want=%s", ip, representedCountry, want)
		}

		// show progress
		ipSlice := ip.AsSlice()
		if ipSlice[1] == 0 && ipSlice[2] == 0 && ipSlice[3] == 0 {
			log.Printf("ip %s done, continuing...", ip)
		}
	}
}

func setupLMDB(t *testing.T, lmdbPath string) (*lmdb.Env, lmdb.DBI) {
	env, err := lmdb.NewEnv()
	if err != nil {
		t.Fatal(err)
	}

	if err = env.SetMaxDBs(1); err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxReaders(64); err != nil {
		t.Fatal(err)
	}
	if err := env.SetMapSize(1 << 30); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(lmdbPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err = env.Open(lmdbPath, 0, 0600); err != nil {
		t.Fatal(err)
	}

	var dbi lmdb.DBI
	if err := env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("country")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return env, dbi
}
