package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"strings"
)

type Stats struct {
	Min      float64
	Max      float64
	Values   []float64
	CurrMean float64
}

var stationMeasureMap = make(map[string]Stats)

func main() {
	start := time.Now()

	stationMeasureMap := readFile()
	sortAndPrint(stationMeasureMap)

	elapsed := time.Since(start)
	fmt.Println("Time taken:", elapsed)
}

func readFile() map[string]Stats {

	file, err := os.Open("<file_path>")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	for scanner.Scan() {
		val := strings.Split(scanner.Text(), ";")
		station := val[0]
		newMeasureAct, err := strconv.ParseFloat(val[1], 32)
		if err != nil {
			fmt.Errorf("error parsing measurement ", val[1])
			// do something sensible
		}
		newMeasure := math.Round(newMeasureAct*10) / 10

		measure, ok := stationMeasureMap[station]
		calculateMinMeanAndMax(&measure, newMeasure, station, ok)
		stationMeasureMap[station] = measure

	}
	return stationMeasureMap

}

func sortAndPrint(stationMeasureMap map[string]Stats) {
	var keys []string

	for k := range stationMeasureMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", k, stationMeasureMap[k].Min, stationMeasureMap[k].CurrMean, stationMeasureMap[k].Max)
	}
}

func calculateMinMeanAndMax(stats *Stats, newMeasure float64, station string, isExisting bool) {
	if !isExisting {
		stats.Min = newMeasure
		stats.Max = newMeasure
	}
	if stats.Max < newMeasure {
		stats.Max = newMeasure
	}
	if stats.Min > newMeasure {
		stats.Min = newMeasure
	}

	stats.Values = append(stats.Values, newMeasure)
	var sum float64
	for _, v := range stats.Values {
		sum = sum + v
	}
	stats.CurrMean = sum / float64(len(stats.Values))
}
