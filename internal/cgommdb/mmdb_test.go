package cgommdb

import (
	"log"
	"net/netip"
	"testing"
)

func TestCGOMMDB(t *testing.T) {
	db, err := Open("../../GeoLite2-Country.mmdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	country, registeredCountry, representedCountry, err := db.LookupCountry(netip.MustParseAddr("1.1.1.1"))
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%q, %q, %q", country, registeredCountry, representedCountry)
}
