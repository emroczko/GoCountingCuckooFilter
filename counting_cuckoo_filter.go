package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"os"
)

type CountingCuckooFilter struct {
	uniqueElements   int32
	capacity         int32
	bucketSize       int32
	maxSwaps         int32
	expansionRate    int32
	autoExpand       bool
	fingerSize       int32
	buckets          [][]*CountingCuckooBin
	hashFunction     hash.Hash64
	insertedElements int32
	filename         *string
}

func NewCountingCuckooFilter(
	capacity int32,
	bucketSize int32,
	maxSwaps int32,
	expansionRate int32,
	autoExpand bool,
	buckets [][]*CountingCuckooBin,
	fingerSize int32,
) *CountingCuckooFilter {
	return &CountingCuckooFilter{
		uniqueElements:   0,
		capacity:         capacity,
		bucketSize:       bucketSize,
		maxSwaps:         maxSwaps,
		expansionRate:    expansionRate,
		autoExpand:       autoExpand,
		fingerSize:       fingerSize,
		hashFunction:     fnv.New64a(),
		insertedElements: 0,
		buckets:          buckets,
	}
}

func (ccf *CountingCuckooFilter) InitErrorRate(errorRate float64, hashFunction hash.Hash64) *CountingCuckooFilter {
	ccf.hashFunction = hashFunction
	return ccf
}

func (ccf *CountingCuckooFilter) LoadErrorRate(errorRate float64, filepath string, hashFunction hash.Hash64) (*CountingCuckooFilter, error) {
	ccf.hashFunction = hashFunction
	return ccf.LoadFromPath(filepath)
}

func (ccf *CountingCuckooFilter) FromBytes(b []byte, errorRate float64, hashFunction hash.Hash64) *CountingCuckooFilter {
	ccf.hashFunction = hashFunction
	ccf.loadFromBytes(b)
	ccf.setErrorRate(errorRate)
	return ccf
}

func (ccf *CountingCuckooFilter) Contains(val string) bool {
	return ccf.Check(val) > 0
}

func (ccf *CountingCuckooFilter) UniqueElements() int32 {
	return ccf.uniqueElements
}

func (ccf *CountingCuckooFilter) LoadFactor() float64 {
	return float64(ccf.uniqueElements) / float64(ccf.capacity*ccf.bucketSize)
}

func (ccf *CountingCuckooFilter) Add(key string) error {
	idx1, idx2, fingerprint := ccf.generateFingerprintInfo(key)
	isPresent := ccf.checkIfPresent(idx1, idx2, fingerprint)
	if isPresent != nil {
		for i := range ccf.buckets[*isPresent] {
			if ccf.buckets[*isPresent][i].Fingerprint == fingerprint {
				ccf.buckets[*isPresent][i].Increment()
				ccf.insertedElements++
				return nil
			}
		}
	}

	//fmt.Println("UNIQUE:", key, idx1, idx2, Fingerprint)
	if prvBin, err := ccf.insertFingerprintAlt(fingerprint, idx1, idx2); err != nil {
		if ccf.autoExpand {
			err := ccf.expand(prvBin)
			if err != nil {
				return err
			}
			return ccf.Add(key)
		}
		return err
	}
	return nil
}

func (ccf *CountingCuckooFilter) Check(key string) int32 {
	idx1, idx2, fingerprint := ccf.generateFingerprintInfo(key)
	isPresent := ccf.checkIfPresent(idx1, idx2, fingerprint)
	var val int32 = 0
	if isPresent != nil {
		for _, bucket := range ccf.buckets[*isPresent] {

			if bucket.Fingerprint == fingerprint {
				val = bucket.Count
				break
			}
		}
	}
	return val
}

func (ccf *CountingCuckooFilter) Remove(key string) bool {
	idx1, idx2, fingerprint := ccf.generateFingerprintInfo(key)
	idx := ccf.checkIfPresent(idx1, idx2, fingerprint)
	if idx == nil {
		return false
	}
	for i := range ccf.buckets[*idx] {
		if ccf.buckets[*idx][i].Fingerprint == fingerprint {
			ccf.buckets[*idx][i].Count--
			ccf.uniqueElements--
			if ccf.buckets[*idx][i].Count == 0 {
				ccf.buckets[*idx] = append(ccf.buckets[*idx][:i], ccf.buckets[*idx][i+1:]...)
				ccf.uniqueElements--
			}
			return true
		}
	}
	return false
}

// // Expand expands the cuckoo filter.
func (ccf *CountingCuckooFilter) expand(finger *CountingCuckooBin) error {
	fingerprints := ccf.setupExpand(finger)

	for _, fingerprint := range fingerprints {
		idx1, idx2 := ccf.indicesFromFingerprint(fingerprint.Fingerprint)
		_, err := ccf.insertFingerprintAlt(fingerprint.Fingerprint, idx1, idx2)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ccf *CountingCuckooFilter) setupExpand(extraFingerprint *CountingCuckooBin) []*CountingCuckooBin {
	var fingerprints []*CountingCuckooBin

	fingerprints = append(fingerprints, extraFingerprint)

	for _, bucket := range ccf.buckets {
		fingerprints = append(fingerprints, bucket...)
	}

	ccf.capacity *= ccf.expansionRate
	ccf.buckets = make([][]*CountingCuckooBin, ccf.capacity)
	ccf.insertedElements = 0
	ccf.uniqueElements = 0

	return fingerprints
}

func (ccf *CountingCuckooFilter) generateFingerprintInfo(data string) (int32, int32, int32) {
	ccf.hashFunction.Reset()
	_, _ = ccf.hashFunction.Write([]byte(data))
	hashVal := ccf.hashFunction.Sum64()

	fingerprint := int32(getBits(uint32(hashVal), uint32(ccf.fingerSize)))

	idx1, idx2 := ccf.indicesFromFingerprint(fingerprint)
	return idx1, idx2, fingerprint
}

func (ccf *CountingCuckooFilter) indicesFromFingerprint(fingerprint int32) (int32, int32) {
	idx1 := fingerprint % ccf.capacity
	ccf.hashFunction.Reset()
	_, _ = ccf.hashFunction.Write([]byte(string(rune(fingerprint))))
	hashFingerprint := ccf.hashFunction.Sum64()
	idx2 := hashFingerprint % uint64(ccf.capacity)
	return idx1, int32(idx2)
}

func (ccf *CountingCuckooFilter) insertFingerprintAlt(fingerprint int32, idx1 int32, idx2 int32) (*CountingCuckooBin, error) {
	if ccf.insertElement(fingerprint, idx1, 1) {
		ccf.insertedElements++
		ccf.uniqueElements++
		return nil, nil
	}
	if ccf.insertElement(fingerprint, idx2, 1) {
		ccf.insertedElements++
		ccf.uniqueElements++
		return nil, nil
	}

	idx := idx1
	if rand.Intn(2) == 0 {
		idx = idx2
	}
	prvBin := NewCountingCuckooBin(fingerprint, 1)
	for i := 0; i < int(ccf.maxSwaps); i++ {
		swapElm := rand.Int31n(ccf.bucketSize)
		swapFinger := ccf.buckets[idx][swapElm]
		prvBin, ccf.buckets[idx][swapElm] = swapFinger, prvBin

		idx1, idx2 = ccf.indicesFromFingerprint(prvBin.Fingerprint)

		if idx == idx1 {
			idx = idx2
		} else {
			idx = idx1
		}

		if ccf.insertElement(prvBin.Fingerprint, idx, prvBin.Count) {
			ccf.insertedElements++
			ccf.uniqueElements++
			return nil, nil
		}
	}
	return prvBin, errors.New("failed to insert Fingerprint")
}

func (ccf *CountingCuckooFilter) PrintFilterParameters() {
	fmt.Println("CCF params: fingerprintSize:", ccf.fingerSize, "capacity:", ccf.capacity, "bucketSize", ccf.bucketSize)
}

func (ccf *CountingCuckooFilter) checkIfPresent(idx1 int32, idx2 int32, fingerprint int32) *int32 {
	for _, finger := range ccf.buckets[idx1] {
		if finger.Fingerprint == fingerprint {
			return &idx1
		}
	}
	for _, finger := range ccf.buckets[idx2] {
		if finger.Fingerprint == fingerprint {
			return &idx2
		}
	}
	return nil
}

func (ccf *CountingCuckooFilter) Export(filepath string) error {
	var buffer bytes.Buffer

	// Write the metadata
	if err := binary.Write(&buffer, binary.LittleEndian, ccf.capacity); err != nil {
		log.Fatal("capacity", err)
		return err
	}

	if err := binary.Write(&buffer, binary.LittleEndian, ccf.bucketSize); err != nil {
		log.Fatal("bucketSize", err)
		return err
	}

	if err := binary.Write(&buffer, binary.LittleEndian, ccf.maxSwaps); err != nil {
		log.Fatal("swaps", err)
		return err
	}

	var counter = 0
	// Zapisujemy koszyki i biny
	for i, bucket := range ccf.buckets {
		if len(bucket) > 0 {
			counter++
			if err := binary.Write(&buffer, binary.LittleEndian, int32(i)); err != nil {
				return err
			}

			if err := binary.Write(&buffer, binary.LittleEndian, int32(len(bucket))); err != nil {
				return err
			}

			for _, bin := range bucket {
				if err := binary.Write(&buffer, binary.LittleEndian, bin.Fingerprint); err != nil {
					return err
				}
				if err := binary.Write(&buffer, binary.LittleEndian, bin.Count); err != nil {
					return err
				}
			}
		}
	}

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Fatalf("failed to close file: %v", err)
		}
	}()

	w := bufio.NewWriter(file)
	_, err = buffer.WriteTo(w)
	if err != nil {
		return err
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	return nil
}

func LoadFromReader(file io.ReadCloser) (*CountingCuckooFilter, error) {
	// Read the metadata
	var capacity, bucketSize, maxSwaps, fingerSize int32
	if err := binary.Read(file, binary.LittleEndian, &capacity); err != nil {
		return nil, err
	}

	if err := binary.Read(file, binary.LittleEndian, &bucketSize); err != nil {
		return nil, err
	}

	if err := binary.Read(file, binary.LittleEndian, &maxSwaps); err != nil {
		return nil, err
	}

	if err := binary.Read(file, binary.LittleEndian, &fingerSize); err != nil {
		return nil, err
	}

	var buckets = make([][]*CountingCuckooBin, capacity)
	var ccf = NewCountingCuckooFilter(capacity, bucketSize, maxSwaps, 2, true, buckets, fingerSize)

	var lastIndex int32 = 0
	// Read the buckets
	for i := 0; i < int(ccf.capacity); i++ {
		ccf.buckets[i] = make([]*CountingCuckooBin, 0, ccf.bucketSize)

		var idx, binCount int32

		if !(lastIndex == int32(i) && lastIndex != 0) {
			if lastIndex == 0 || lastIndex == int32(i-1) {
				if err := binary.Read(file, binary.LittleEndian, &idx); err != nil {
					if err == io.EOF {
						break
					}
					log.Println("error in ", i, idx, lastIndex, err)
				}
				lastIndex = idx

				if int(idx) != i {
					continue
				}
			} else {
				continue
			}
		}

		if err := binary.Read(file, binary.LittleEndian, &binCount); err != nil {
			log.Println("bin count error in ", i, idx, lastIndex)
			return nil, err
		}

		for j := 0; j < int(binCount); j++ {
			var finger, count int32
			if err := binary.Read(file, binary.LittleEndian, &finger); err != nil {
				log.Println("finger error in ", i, idx, lastIndex)
				return nil, err
			}
			if err := binary.Read(file, binary.LittleEndian, &count); err != nil {
				log.Println("count error in ", i, idx, lastIndex)
				return nil, err
			}

			ccf.buckets[i] = append(ccf.buckets[i], NewCountingCuckooBin(finger, count))
			ccf.insertedElements += count
			ccf.uniqueElements++
		}
	}

	return ccf, nil
}

func (ccf *CountingCuckooFilter) LoadFromPath(filepath string) (*CountingCuckooFilter, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Fatalf("failed to close file: %v", err)
		}
	}()

	return LoadFromReader(file)
}

func (ccf *CountingCuckooFilter) loadFromBytes(b []byte) {
	// Load from bytes logic
}

func (ccf *CountingCuckooFilter) setErrorRate(errorRate float64) {
	// Set error rate logic
}

func (ccf *CountingCuckooFilter) expandLogic(extraFingerprint *CountingCuckooBin) {
	// Expand logic
}

func (ccf *CountingCuckooFilter) insertElement(fingerprint int32, idx int32, count int32) bool {
	if int32(len(ccf.buckets[idx])) < ccf.bucketSize {
		ccf.buckets[idx] = append(ccf.buckets[idx], NewCountingCuckooBin(fingerprint, count))
		return true
	}
	return false
}

func getBits(value uint32, bits uint32) uint32 {
	return value & (1<<bits - 1)
}
