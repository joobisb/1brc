package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"strings"

	"github.com/edsrzf/mmap-go"
)

type Stats struct {
	Min      float64
	Max      float64
	Count    int
	CurrMean float64
}

var stationMeasureMap = make(map[string]*Stats)

func main() {
	start := time.Now()
	ch := make(chan []string, 1000)
	// var wg sync.WaitGroup
	var workerWg sync.WaitGroup
	resultChan := make(chan map[string]*Stats)
	workerSize := 3
	batchSize := 1_000_000_00 / 100

	fmt.Println("workerSize", workerSize)
	for i := 0; i < workerSize; i++ {
		workerWg.Add(1)
		go func(ch <-chan []string, resultChan chan<- map[string]*Stats) {
			defer workerWg.Done()
			workerBatch(ch, resultChan)
		}(ch, resultChan)
	}

	go func() {
		workerWg.Wait()
		close(resultChan)
	}()

	printAlloc()

	go ReadFileBatch(ch, batchSize)

	for val := range resultChan {
		for station, v := range val {
			if existStat, ok := stationMeasureMap[station]; ok {
				if existStat.Min > v.Min {
					existStat.Min = v.Min
				}
				if existStat.Max < v.Max {
					existStat.Max = v.Max
				}

				totalCount := existStat.Count + v.Count
				existStat.Count = int(totalCount)
				existStat.CurrMean = (existStat.CurrMean*(float64(existStat.Count)) + v.CurrMean*(float64(v.Count))) / float64(totalCount)

			} else {
				stationMeasureMap[station] = v
			}
		}
	}

	printAlloc()
	SortAndPrint(stationMeasureMap)
	printAlloc()

	elapsed := time.Since(start)
	fmt.Println("Time taken:", elapsed)
}

func printAlloc() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("%d KB\n", m.Alloc/1024)
}

func ReadFileBatch(ch chan<- []string, batchSize int) {
	file, err := os.Open("<path>")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var batch []string

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}

		if len(line) > 0 {
			batch = append(batch, strings.TrimSuffix(line, "\n"))
		}

		if len(batch) >= batchSize || err == io.EOF {
			if len(batch) > 0 {
				ch <- batch
				batch = nil // reset batch
			}

			if err == io.EOF {
				break
			}
		}
	}

	close(ch)
}

func ReadFileBatchMMap(ch chan<- []string, batchSize int) {
	file, err := os.Open("<path>")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Memory-map the file
	mmap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer mmap.Unmap()

	// Convert mmap to string and split by lines
	content := string(mmap)
	lines := strings.Split(content, "\n")

	var batch []string
	for _, line := range lines {
		if len(line) > 0 {
			batch = append(batch, line)
		}

		if len(batch) >= batchSize {
			ch <- batch
			batch = nil // reset batch
		}
	}

	// Send any remaining lines in the last batch
	if len(batch) > 0 {
		ch <- batch
	}

	close(ch)
}

func ReadFile(ch chan<- string) {

	file, err := os.Open("<path>")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	for scanner.Scan() {
		ch <- scanner.Text()
	}
	close(ch)
}

func ReadFileV2(ch chan<- string) {

	file, err := os.Open("<path>")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 128)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			if len(line) > 0 {
				// Handle the case where the last line doesn't end with '\n'
				ch <- strings.TrimSuffix(line, "\n")
			}
			break
		}

		ch <- strings.TrimSuffix(line, "\n")

	}

	close(ch)
}

func worker(ch <-chan string, resultChan chan<- map[string]*Stats) {
	localStats := make(map[string]*Stats)
	for v := range ch {
		// val := strings.Split(v, ";")
		idx := strings.Index(v, ";")
		station := v[:idx]
		newMeasureAct, err := strconv.ParseFloat(v[idx+1:], 32)
		if err != nil {
			fmt.Errorf("error parsing measurement %s", v[idx+1:])
		}
		newMeasure := math.Round(newMeasureAct*10) / 10

		measure := localStats[station]
		// if !ok {
		//     measure = &Stats{} // Initialize if not exists
		// }
		updatedStats := calculateMinMeanAndMax(measure, newMeasure, station)
		localStats[station] = updatedStats
	}
	resultChan <- localStats

}

func workerBatch(ch <-chan []string, resultChan chan<- map[string]*Stats) {
	localStats := make(map[string]*Stats)
	for chVal := range ch {
		for _, v := range chVal {
			// val := strings.Split(v, ";")
			idx := strings.Index(v, ";")
			station := v[:idx]
			newMeasureAct, err := strconv.ParseFloat(v[idx+1:], 32)
			if err != nil {
				fmt.Errorf("error parsing measurement %s", v[idx+1:])
			}
			newMeasure := math.Round(newMeasureAct*10) / 10

			measure := localStats[station]
			// if !ok {
			//     measure = &Stats{} // Initialize if not exists
			// }
			updatedStats := calculateMinMeanAndMax(measure, newMeasure, station)
			localStats[station] = updatedStats
		}
	}
	resultChan <- localStats

}

func SortAndPrint(stationMeasureMap map[string]*Stats) {
	var keys []string

	for k := range stationMeasureMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var sum int64
	for _, k := range keys {
		sum = sum + int64(stationMeasureMap[k].Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f/%d, ", k, stationMeasureMap[k].Min, stationMeasureMap[k].CurrMean, stationMeasureMap[k].Max, stationMeasureMap[k].Count)
	}
	fmt.Println("length, sum ", len(keys), sum)

}

func calculateMinMeanAndMax(stats *Stats, newMeasure float64, station string) *Stats {
	if stats == nil {
		// Initialize stats with the first measurement
		return &Stats{
			Min:      newMeasure,
			Max:      newMeasure,
			CurrMean: newMeasure,
			Count:    1,
		}
	}
	if stats.Max < newMeasure {
		stats.Max = newMeasure
	}
	if stats.Min > newMeasure {
		stats.Min = newMeasure
	}

	stats.Count++
	stats.CurrMean = (stats.CurrMean*float64(stats.Count-1) + newMeasure) / float64(stats.Count)

	return stats
}
