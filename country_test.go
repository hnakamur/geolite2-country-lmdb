package geolite2countrylmdb

import (
	"bytes"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/oschwald/maxminddb-golang"
)

const mmdbPath = "GeoLite2-Country.mmdb"

func TestLookupCountry(t *testing.T) {
	type testCase struct {
		ip                 string
		country            string
		registeredCountry  string
		representedCountry string
	}
	testCases := []testCase{
		{ip: "0.0.0.0", country: "", registeredCountry: "", representedCountry: ""},
		{ip: "0.0.0.255", country: "", registeredCountry: "", representedCountry: ""},
		{ip: "1.0.0.0", country: "AU", registeredCountry: "AU", representedCountry: ""},
		{ip: "1.0.0.1", country: "AU", registeredCountry: "AU", representedCountry: ""},
		{ip: "1.0.16.0", country: "JP", registeredCountry: "JP", representedCountry: ""},
		{ip: "1.1.1.1", country: "", registeredCountry: "AU", representedCountry: ""},
		{ip: "8.8.4.4", country: "US", registeredCountry: "US", representedCountry: ""},
		{ip: "8.8.8.8", country: "US", registeredCountry: "US", representedCountry: ""},
		{ip: "212.47.235.82", country: "FR", registeredCountry: "FR", representedCountry: ""},
		{ip: "223.255.255.255", country: "AU", registeredCountry: "AU", representedCountry: ""},
		{ip: "224.0.0.0", country: "", registeredCountry: "", representedCountry: ""},
		{ip: "255.255.255.255", country: "", registeredCountry: "", representedCountry: ""},
	}

	mustParseIP := func(t *testing.T, ip string) net.IP {
		t.Helper()
		parsed := net.ParseIP(ip)
		if parsed == nil || parsed.To4() == nil {
			t.Fatalf("bad test case IP: %s", ip)
		}
		return parsed.To4()
	}

	verify := func(t *testing.T, country, registeredCountry, representedCountry string, tc *testCase) {
		t.Helper()
		if country != tc.country {
			t.Errorf("country mismatch for IP=%s, got=%s, want=%s", tc.ip, country, tc.country)
		}
		if registeredCountry != tc.registeredCountry {
			t.Errorf("registeredCountry mismatch for IP=%s, got=%s, want=%s", tc.ip, registeredCountry, tc.registeredCountry)
		}
		if representedCountry != tc.representedCountry {
			t.Errorf("representedCountry mismatch for IP=%s, got=%s, want=%s", tc.ip, representedCountry, tc.representedCountry)
		}
	}

	t.Run("LMDB", func(t *testing.T) {
		lmdbPath := t.TempDir()
		env, dbi := setupLMDB(t, lmdbPath)
		defer env.Close()

		t0 := time.Now()
		if err := env.Update(func(txn *lmdb.Txn) (err error) {
			return SetupCountry(mmdbPath, dbi)(txn)
		}); err != nil {
			t.Fatal(err)
		}
		log.Printf("UpdateCountry done, elapsed=%s", time.Since(t0))

		for _, tc := range testCases {
			var country, registeredCountry, representedCountry string
			if err := env.View(func(txn *lmdb.Txn) (err error) {
				ip := mustParseIP(t, tc.ip)
				if err := LookupCountry(dbi, ip, &country, &registeredCountry, &representedCountry)(txn); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			verify(t, country, registeredCountry, representedCountry, &tc)
		}
	})
	t.Run("MaxMind", func(t *testing.T) {
		db, err := maxminddb.Open(mmdbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for _, tc := range testCases {
			ip := mustParseIP(t, tc.ip)
			var record mmdbCountryRecord
			err := db.Lookup(ip, &record)
			if err != nil {
				t.Fatal(err)
			}
			country := record.Country.ISOCode
			registeredCountry := record.RegisteredCountry.ISOCode
			representedCountry := record.RepresentedCountry.ISOCode
			verify(t, country, registeredCountry, representedCountry, &tc)
		}
	})
}

func setupLMDB(t *testing.T, lmdbPath string) (*lmdb.Env, lmdb.DBI) {
	env, err := lmdb.NewEnv()
	if err != nil {
		t.Fatal(err)
	}

	if err = env.SetMaxDBs(1); err != nil {
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

func TestBroadcastAddr(t *testing.T) {
	_, subnet, err := net.ParseCIDR("10.0.0.2/8")
	if err != nil {
		t.Fatal(err)
	}

	var endIP [4]byte
	broadcastAddr(subnet, &endIP)
	want := net.IPv4(10, 255, 255, 255).To4()
	got := net.IP(endIP[:])
	if !bytes.Equal(got, want) {
		t.Errorf("result mismatch, got=%s, want=%s", got, want)
	}
}

func TestNextAddr(t *testing.T) {
	src := [4]byte{10, 255, 255, 255}
	var dest [4]byte
	nextAddr(src, &dest)
	want := net.IPv4(11, 0, 0, 0).To4()
	got := net.IP(dest[:])
	if !bytes.Equal(got, want) {
		t.Errorf("result mismatch, got=%s, want=%s", got, want)
	}
}
