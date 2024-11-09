package util

import "fmt"

type CountingCuckooBin struct {
	Fingerprint int32
	Count       int32
}

func NewCountingCuckooBin(fingerprint int32, count int32) *CountingCuckooBin {
	return &CountingCuckooBin{
		Fingerprint: fingerprint,
		Count:       count,
	}
}

// Contains checks if the bin contains the given value
func (ccb *CountingCuckooBin) Contains(val int32) bool {
	return ccb.Fingerprint == val
}

// GetArray returns the bin as an array
func (ccb *CountingCuckooBin) GetArray() [2]int32 {
	return [2]int32{ccb.Fingerprint, ccb.Count}
}

// Finger returns the Fingerprint
func (ccb *CountingCuckooBin) Finger() int32 {
	return ccb.Fingerprint
}

//// Count returns the Count
//func (ccb *CountingCuckooBin) Count() int32 {
//	return ccb.Count
//}

// Increment increases the Count by one
func (ccb *CountingCuckooBin) Increment() int32 {
	ccb.Count++
	return ccb.Count
}

// Decrement decreases the Count by one
func (ccb *CountingCuckooBin) Decrement() int32 {
	ccb.Count--
	return ccb.Count
}

func (ccb *CountingCuckooBin) String() string {
	return fmt.Sprintf("(Fingerprint:%d Count:%d)", ccb.Fingerprint, ccb.Count)
}
