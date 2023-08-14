package ec

import (
	"fmt"
	"os"

	"github.com/baohan10/reedsolomon"
)

type rs struct {
	r   *(reedsolomon.ReedSolomon)
	ecN int
	ecK int
	ecP int
}

func NewRsEnc(ecK int, ecN int) *rs {
	enc := rs{
		ecN: ecN,
		ecK: ecK,
		ecP: ecN - ecK,
	}
	enc.r = reedsolomon.GetReedSolomonIns(ecK, ecN)
	return &enc
}
func (r *rs) Encode(all [][]byte) {
	r.r.Encode(all)
}

func (r *rs) Repair(all [][]byte) error {
	return r.r.Reconstruct(all)
}

func checkErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s", err.Error())
	}
}
