package main

import (
	"testing"
)

func BenchmarkReadFileAndCalculateStats(b *testing.B) {
	for i := 0; i < b.N; i++ {

		main()
		// ch := make(chan string, 1000)
		// // Reading file and calculating stats
		// stationMeasureMap := ReadFile(ch)
		// SortAndPrint(stationMeasureMap) // assuming sortAndPrint returns something

		// Stopping the timer as we're only interested in reading and calculation time
	}
}
