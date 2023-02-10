package estimate

import (
	"bufio"
	"os"
	"runtime"
	"strconv"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/pbnjay/memory"
)

type estimatedRamPerWorker datasize.ByteSize

// Workers - return max workers amount based on total Memory/CPU's and estimated RAM per worker
func (r estimatedRamPerWorker) Workers() int {
	// first check if we are running under cgroup limits (docker, kubernetes, etc)
	maxMemory, err := fetchCgroupMemoryMax()
	if err != nil {
		// if error, fall back to total memory
		maxMemory = memory.TotalMemory()
	}

	// 50% of TotalMemory. Better don't count on 100% because OOM Killer may have aggressive defaults and other software may need RAM
	maxWorkersForGivenMemory := (maxMemory / 2) / uint64(r)
	return cmp.Min(AlmostAllCPUs(), int(maxWorkersForGivenMemory))
}
func (r estimatedRamPerWorker) WorkersHalf() int    { return cmp.Max(1, r.Workers()/2) }
func (r estimatedRamPerWorker) WorkersQuarter() int { return cmp.Max(1, r.Workers()/4) }

const (
	IndexSnapshot     = estimatedRamPerWorker(2 * datasize.GB)   //elias-fano index building is single-threaded
	CompressSnapshot  = estimatedRamPerWorker(1 * datasize.GB)   //1-file-compression is multi-threaded
	ReconstituteState = estimatedRamPerWorker(512 * datasize.MB) //state-reconstitution is multi-threaded
)

// AlmostAllCPUs - return all-but-one cpus. Leaving 1 cpu for "work producer", also cloud-providers do recommend leave 1 CPU for their IO software
// user can reduce GOMAXPROCS env variable
func AlmostAllCPUs() int {
	return cmp.Max(1, runtime.GOMAXPROCS(-1)-1)
}

func fetchCgroupMemoryMax() (uint64, error) {
	file, err := os.Open("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		memory, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return 0, err
		}
		return uint64(memory), nil
	}
	return 0, scanner.Err()
}
