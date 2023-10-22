// Package geolite2countrylmdb provides functions to setup GeoLite2 country
// entries in a LMDB subdatabase and to lookup an entry for an IP.
package geolite2countrylmdb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/oschwald/maxminddb-golang"
)

const valueByteLen = 10
const isoCodeNone = "  "
const debug = false

// LookupCountry lookups the ip in LMDB subdatabase and updates *outCountry,
// *outRegisteredCountry, and *outRepresentedCountry if an entry is found
// and the responding pointer is not nil.
//
// For difference among country, registered country, and represented country,
// see https://dev.maxmind.com/geoip/whats-new-in-geoip2/#country-registered-country-and-represented-country
func LookupCountry(dbi lmdb.DBI, ip net.IP, outCountry, outRegisteredCountry, outRepresentedCountry *string) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		endIP, val, err := cur.Get(ip, nil, lmdb.SetRange)
		if err != nil {
			return err
		}
		if len(val) != valueByteLen {
			return errors.New("unexpected value length")
		}
		startIP := val[:4]
		if debug {
			log.Printf("lookup ip=%s, start=%s, end=%s, country=%s, registeredCountry=%s, representedCountry=%s.",
				ip, net.IP(endIP), net.IP(val[:4]),
				strings.TrimSpace(string(val[4:6])),
				strings.TrimSpace(string(val[6:8])),
				strings.TrimSpace(string(val[8:])))
		}
		if bytes.Compare(ip, startIP) < 0 || bytes.Compare(ip, endIP) > 0 {
			return lmdb.NotFound
		}
		if outCountry != nil {
			*outCountry = strings.TrimSpace(string(val[4:6]))
		}
		if outRegisteredCountry != nil {
			*outRegisteredCountry = strings.TrimSpace(string(val[6:8]))
		}
		if outRepresentedCountry != nil {
			*outRepresentedCountry = strings.TrimSpace(string(val[8:]))
		}
		return nil
	}
}

type mmdbCountryRecord struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	RegisteredCountry struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"registered_country"`
	RepresentedCountry struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"represented_country"`
}

// SetupCountry first deletes all entries in the LMDB subdatabase and then
// put entries read from the mmdb file.
func SetupCountry(srcMMDBPath string, destDBI lmdb.DBI) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		db, err := maxminddb.Open(srcMMDBPath)
		if err != nil {
			return err
		}
		defer db.Close()

		_, network, err := net.ParseCIDR("0.0.0.0/0")
		if err != nil {
			return err
		}

		if err := deleteAllEntries(destDBI)(txn); err != nil {
			return err
		}

		var startIP net.IP
		var endIP, endNextIP [4]byte
		var country, registeredCountry, representedCountry string
		if debug {
			fmt.Println("start,end,country,registeredCountry,representedCountry")
		}
		putEntry := func() error {
			var val [valueByteLen]byte
			copy(val[:4], startIP)
			copy(val[4:6], []byte(country))
			copy(val[6:8], []byte(registeredCountry))
			copy(val[8:], []byte(representedCountry))
			if err := txn.Put(destDBI, endIP[:], val[:], 0); err != nil {
				return err
			}
			if debug {
				fmt.Printf("%s,%s,%s,%s,%s\n",
					startIP,
					net.IP(endIP[:]),
					strings.TrimSpace(country),
					strings.TrimSpace(registeredCountry),
					strings.TrimSpace(representedCountry))
			}
			return nil
		}

		var record mmdbCountryRecord
		networks := db.NetworksWithin(network, maxminddb.SkipAliasedNetworks)
		for networks.Next() {
			record.Country.ISOCode = isoCodeNone
			record.RegisteredCountry.ISOCode = isoCodeNone
			record.RepresentedCountry.ISOCode = isoCodeNone
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Panic(err)
			}
			if record.Country.ISOCode == country &&
				record.RegisteredCountry.ISOCode == registeredCountry &&
				record.RepresentedCountry.ISOCode == representedCountry &&
				bytes.Equal(subnet.IP.To4(), endNextIP[:]) {
				broadcastAddr(subnet, &endIP)
				nextAddr(endIP, &endNextIP)
			} else {
				if startIP != nil {
					if err := putEntry(); err != nil {
						return err
					}
				}
				country = record.Country.ISOCode
				registeredCountry = record.RegisteredCountry.ISOCode
				representedCountry = record.RepresentedCountry.ISOCode
				startIP = subnet.IP.To4()
				broadcastAddr(subnet, &endIP)
				nextAddr(endIP, &endNextIP)
			}
		}
		if err := putEntry(); err != nil {
			return err
		}
		return networks.Err()
	}
}

func deleteAllEntries(dbi lmdb.DBI) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			_, _, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			if err := cur.Del(0); err != nil {
				return err
			}
		}
	}
}

func broadcastAddr(subnet *net.IPNet, dest *[4]byte) {
	for i := 0; i < 4; i++ {
		dest[i] = subnet.IP[i] | ^subnet.Mask[i]
	}
}

func nextAddr(src [4]byte, dest *[4]byte) {
	a := byte(1)
	for i := 3; i >= 0; i-- {
		dest[i] = src[i] + a
		if dest[i] != 0 {
			a = 0
		}
	}
}
