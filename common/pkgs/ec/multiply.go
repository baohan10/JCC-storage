package ec

import "github.com/klauspost/reedsolomon"

func GaloisMultiplier() *reedsolomon.MultipilerBuilder {
	return reedsolomon.DefaultMulOpt()
}
