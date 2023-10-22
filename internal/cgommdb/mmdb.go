package cgommdb

// #cgo pkg-config: libmaxminddb
// #include <maxminddb.h>
// #include <netinet/in.h>
// #include <stdlib.h>
import "C"
import (
	"errors"
	"fmt"
	"net/netip"
	"runtime"
	"unsafe"
)

const mmdbSuccess = 0

var ErrNotFound = errors.New("not found")

type MMDB struct {
	pinner                 runtime.Pinner
	db                     *C.struct_MMDB_s
	cStrISOCode            *C.char
	cStrCountry            *C.char
	cStrRegisteredCountry  *C.char
	cStrRepresentedCountry *C.char
}

func Open(filename string) (*MMDB, error) {
	db := &MMDB{
		db:                     new(C.struct_MMDB_s),
		cStrISOCode:            C.CString("iso_code"),
		cStrCountry:            C.CString("country"),
		cStrRegisteredCountry:  C.CString("registered_country"),
		cStrRepresentedCountry: C.CString("represented_country"),
	}
	db.pinner.Pin(unsafe.Pointer(db.db))
	db.pinner.Pin(unsafe.Pointer(db.cStrISOCode))
	db.pinner.Pin(unsafe.Pointer(db.cStrCountry))
	db.pinner.Pin(unsafe.Pointer(db.cStrRegisteredCountry))
	db.pinner.Pin(unsafe.Pointer(db.cStrRepresentedCountry))

	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	const mmdbNodeMmap = 1
	status := C.MMDB_open(cFilename, mmdbNodeMmap, db.db)
	if status != mmdbSuccess {
		db.pinner.Unpin()
		return nil, fmt.Errorf("open mmdb file %q, %s", filename, mmdbStrerror(status))
	}
	return db, nil
}

func (db *MMDB) LookupCountry(ip netip.Addr) (country, registeredCountry, representedCountry string, err error) {
	if !ip.Is4() {
		return "", "", "", errors.New("non-IPv4 address is not supported")
	}
	const afInet = 2
	sockaddr := C.struct_sockaddr_in{
		sin_family: afInet,
		sin_port:   0,
		sin_addr: C.struct_in_addr{
			s_addr: ip.As4(),
		},
	}
	var status C.int
	result := C.MMDB_lookup_sockaddr(db.db,
		(*C.struct_sockaddr)(unsafe.Pointer(&sockaddr)), &status)
	if status != mmdbSuccess {
		return "", "", "", fmt.Errorf("lookup ip=%s, %s", ip, mmdbStrerror(status))
	}
	if !result.found_entry {
		return "", "", "", ErrNotFound
	}

	const pathCount = 3
	lookupPaths := [pathCount][]*C.char{
		{db.cStrCountry, db.cStrISOCode, nil},
		{db.cStrRepresentedCountry, db.cStrISOCode, nil},
		{db.cStrRegisteredCountry, db.cStrISOCode, nil},
	}
	var isoCodes [pathCount]string
	for i, lookupPath := range lookupPaths {
		var entryData C.struct_MMDB_entry_data_s
		status = C.MMDB_aget_value(&result.entry, &entryData, &lookupPath[0])
		if status != mmdbSuccess || entryData.offset == 0 {
			return "", "", "", ErrNotFound
		}
		entry := C.struct_MMDB_entry_s{
			mmdb:   db.db,
			offset: entryData.offset,
		}
		var entryDataList *C.struct_MMDB_entry_data_list_s
		status = C.MMDB_get_entry_data_list(&entry, &entryDataList)
		if status != mmdbSuccess {
			return "", "", "", fmt.Errorf("get entry data list, ip=%s, %s", ip, mmdbStrerror(status))
		}
		entryData = entryDataList.entry_data
		const mmdbDataTypeUtf8String = 2
		if entryDataList != nil {
			if entryData.has_data && entryData._type == mmdbDataTypeUtf8String {
				isoCodes[i] = C.GoStringN(entryData.utf8_string, C.int(entryData.data_size))
			}
			C.MMDB_free_entry_data_list(entryDataList)
		}
	}
	return isoCodes[0], isoCodes[1], isoCodes[2], nil
}

func (db *MMDB) Close() {
	C.MMDB_close(db.db)
	C.free(unsafe.Pointer(db.cStrISOCode))
	C.free(unsafe.Pointer(db.cStrCountry))
	C.free(unsafe.Pointer(db.cStrRegisteredCountry))
	C.free(unsafe.Pointer(db.cStrRepresentedCountry))
	db.pinner.Unpin()
}

func mmdbStrerror(status C.int) string {
	return C.GoString(C.MMDB_strerror(status))
}
