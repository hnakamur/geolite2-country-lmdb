package geolite2countrylmdb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/oschwald/maxminddb-golang"
)

const valueByteLen = 6

func LookupCountry(dbi lmdb.DBI, ip net.IP, outIsoCode *string) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		log.Printf("calling Get for ip=%s", ip)
		endIP, val, err := cur.Get(ip, nil, lmdb.SetRange)
		if err != nil {
			return err
		}
		if len(val) != valueByteLen {
			return errors.New("unexpected value length")
		}
		startIP := val[:4]
		// log.Printf("ip=%s, start=%s, end=%s, isoCode=%s", ip, net.IP(endIP), net.IP(val[:4]), string(val[4:]))
		if bytes.Compare(ip, startIP) < 0 || bytes.Compare(ip, endIP) > 0 {
			return lmdb.NotFound
		}
		*outIsoCode = string(val[4:])
		return nil
	}
}

func UpdateCountry(mmdbPath string, dbi lmdb.DBI) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		db, err := maxminddb.Open(mmdbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		if err := deleteAllEntries(dbi)(txn); err != nil {
			return err
		}

		_, network, err := net.ParseCIDR("0.0.0.0/0")
		if err != nil {
			return err
		}

		var startIP net.IP
		var endIP, endNextIP [4]byte
		var isoCode string
		const debug = true
		var prevSubnet *net.IPNet

		putEntry := func() error {
			if startIP == nil || isoCode == "" {
				if debug {
					if isoCode == "" {
						log.Printf("skip because isoCode is empty, subnet=%s", prevSubnet)
					}
				}
				return nil
			}
			var val [valueByteLen]byte
			copy(val[:], startIP)
			if len(isoCode) != 2 {
				return fmt.Errorf("unexpected length of country isoCode of %q, got=%d, want=%d", isoCode, len(isoCode), 2)
			}
			copy(val[4:], []byte(isoCode))
			if err := txn.Put(dbi, endIP[:], val[:], 0); err != nil {
				return err
			}
			if debug {
				log.Printf("put subnet=%s, strat=%s, end=%s, isoCode=%s", prevSubnet, startIP, net.IP(endIP[:]), isoCode)
			}
			return nil
		}

		record := struct {
			// Country struct {
			// 	ISOCode string `maxminddb:"iso_code"`
			// } `maxminddb:"country"`
			RepresentedCountry struct {
				ISOCode string `maxminddb:"iso_code"`
			} `maxminddb:"represented_country"`
		}{}

		networks := db.NetworksWithin(network, maxminddb.SkipAliasedNetworks)
		for networks.Next() {
			subnet, err := networks.Network(&record)
			if err != nil {
				log.Panic(err)
			}
			if record.RepresentedCountry.ISOCode == isoCode && bytes.Equal(subnet.IP.To4(), endNextIP[:]) {
				broadcastAddr(subnet, &endIP)
				nextAddr(endIP, &endNextIP)
				if debug {
					log.Printf("expanded ip range prevSubnet=%s, subnet=%s, isoCode=%s", prevSubnet, subnet, isoCode)
				}
			} else {
				if err := putEntry(); err != nil {
					return err
				}
				prevSubnet = subnet
				isoCode = record.RepresentedCountry.ISOCode
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
