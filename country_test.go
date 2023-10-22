package geolite2countrylmdb

import (
	"bytes"
	"net"
	"os"
	"testing"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/oschwald/maxminddb-golang"
	"pgregory.net/rapid"
)

const mmdbPath = "GeoLite2-Country.mmdb"

func TestLookupCountry_Property(t *testing.T) {
	// set up LMDB
	lmdbPath := t.TempDir()
	env, dbi := setupLMDB(t, lmdbPath)
	defer env.Close()

	if err := env.Update(func(txn *lmdb.Txn) (err error) {
		return UpdateCountry(mmdbPath, dbi)(txn)
	}); err != nil {
		t.Fatal(err)
	}

	// set up MaxMindDB
	db, err := maxminddb.Open(mmdbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rapid.Check(t, func(t *rapid.T) {
		ip := net.IP(rapid.SliceOfN(rapid.Byte(), 4, 4).Draw(t, "ip"))

		// Lookup LMDB
		var got string
		if err := env.View(func(txn *lmdb.Txn) (err error) {
			if err := LookupCountry(dbi, ip.To4(), &got)(txn); err != nil {
				if !lmdb.IsNotFound(err) {
					return err
				}
				got = "-"
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Lookup MaxMind
		record := struct {
			Country struct {
				IsoCode string `maxminddb:"iso_code"`
			} `maxminddb:"country"`
		}{}

		record.Country.IsoCode = "-"
		err := db.Lookup(ip, &record)
		if err != nil {
			t.Fatal(err)
		}
		want := record.Country.IsoCode
		if got != want {
			t.Fatalf("LMDB result does not match to MaxMind result, ip=%s, got=%s, want=%s", ip, got, want)
		}
	})
}

func TestLookupCountry(t *testing.T) {
	mmdbPath := "GeoIP2-Country-Test.mmdb"
	testCases := []struct {
		ip   string
		want string
	}{
		// {ip: "0.255.255.255", want: "-"},
		// {ip: "1.0.0.0", want: "AU"},
		// {ip: "1.0.0.1", want: "AU"},
		// {ip: "1.0.16.0", want: "JP"},
		// {ip: "1.1.1.1", want: "US"},
		// {ip: "212.47.235.82", want: "PH"},
		{ip: "1.1.1.1", want: "-"},
		{ip: "212.47.235.82", want: "-"},
		// {ip: "223.255.255.255", want: "AU"},
		// {ip: "224.0.0.0", want: "-"},
	}

	t.Run("LMDB", func(t *testing.T) {
		lmdbPath := t.TempDir()
		env, dbi := setupLMDB(t, lmdbPath)
		defer env.Close()

		if err := env.Update(func(txn *lmdb.Txn) (err error) {
			return UpdateCountry(mmdbPath, dbi)(txn)
		}); err != nil {
			t.Fatal(err)
		}

		for _, tc := range testCases {
			var got string
			if err := env.View(func(txn *lmdb.Txn) (err error) {
				ip := net.ParseIP(tc.ip)
				if ip == nil || ip.To4() == nil {
					t.Fatalf("bad test case IP: %s", tc.ip)
				}
				if err := LookupCountry(dbi, ip.To4(), &got)(txn); err != nil {
					if !lmdb.IsNotFound(err) {
						return err
					}
					got = "-"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("result mismatch for IP=%s, got=%s, want=%s", tc.ip, got, tc.want)
			}
		}
	})
	t.Run("MaxMind", func(t *testing.T) {
		db, err := maxminddb.Open(mmdbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		record := struct {
			Country struct {
				ISOCode string `maxminddb:"iso_code"`
			} `maxminddb:"country"`
		}{}

		for _, tc := range testCases {
			ip := net.ParseIP(tc.ip)
			if ip == nil || ip.To4() == nil {
				t.Fatalf("bad test case IP: %s", tc.ip)
			}
			record.Country.ISOCode = "-"
			err := db.Lookup(ip, &record)
			if err != nil {
				t.Fatal(err)
			}
			got := record.Country.ISOCode
			if got != tc.want {
				t.Errorf("result mismatch for IP=%s, got=%s, want=%s", tc.ip, got, tc.want)
			}
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
