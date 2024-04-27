package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

// MAP STUFF
func (self *MapTask) Process(tempdir string, client Interface) error {
	fmt.Printf("WHOAMI: '%v'\n", self.SourceHost)
	// fmt.Printf("WHAT WE GOT: tempdir:'%v', client:'%v'\n", tempdir, client)
	// return nil

	var pfile = tempdir + "/" + mapSourceFile(self.N)                                      //outdir
	var dl_err = download("http://"+self.SourceHost+"/data/"+mapSourceFile(self.N), pfile) //donwload sourcehost
	if dl_err != nil {
		log.Printf("DOWNLOAD FAIL! BROH: '%v'\n", dl_err)
	}
	var dl_handle, err = openDatabase(pfile) //open it

	if err != nil {
		log.Printf("DATABSE OPEN FAIL! BROH: '%v'\n", err)
		return err
	}
	defer dl_handle.Close()

	//collect outputfiles into `db_handles`
	var db_handles [](*sql.DB)
	for i := 0; i < self.R; i++ { //number of outputs is same as number of reduce tasks
		var localpath = tempdir + "/" + mapOutputFile(self.N, i)
		var db_handle, err = createDatabase(localpath)
		if err != nil {
			log.Printf("CREATE FAIL! HERES WHY BROH: '%v'\n", err)
			return err
		}
		db_handles = append(db_handles, db_handle)
		defer db_handle.Close() //for later
	}

	//loop over source key/value pairs
	rows, err := dl_handle.Query("select key, value from pairs")
	if err != nil {
		log.Printf("SELECT FAIL! REASON BROH: '%v'\n", err)
		return err
	}
	var output = make(chan Pair)
	for rows.Next() {
		var key, val string
		if err := rows.Scan(&key, &val); err != nil {
			log.Printf("SCAN ERROR BROH: '%v'\n", err)
			return err
		}
		go client.Map(key, val, output) //maybe bad
	}

	for pair := range output { //recieved is a pair
		var hash = fnv.New32()
		hash.Write([]byte(pair.Key))
		var r = int(hash.Sum32() % uint32(self.R)) //hash the recieved value

		var db_in_question = db_handles[r]
		var stmnt, err = db_in_question.Prepare("insert into pairs (key, value) values (?, ?)")
		if err != nil {
			log.Printf("PREPARERROR BROH: '%v'\n", err)
			return err
		}
		if _, err := stmnt.Exec(pair.Key, pair.Value); err != nil { //insert into database
			log.Printf("INSERT ERROR BROH: '%v'\n", err)
			return err
		}
	}
	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	return nil
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	localaddress := localAddr.IP.String()

	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return localaddress
}

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

func main() {
	m := 10
	r := 5
	source := "source.db"
	//target := "target.db"
	tmp := os.TempDir()

	tempdir := filepath.Join(tmp, fmt.Sprintf("mapreduce.%d", os.Getpid()))
	if err := os.RemoveAll(tempdir); err != nil {
		log.Fatalf("unable to delete old temp dir: %v", err)
	}
	if err := os.Mkdir(tempdir, 0700); err != nil {
		log.Fatalf("unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempdir)

	log.Printf("splitting %s into %d pieces", source, m)
	var paths []string
	for i := 0; i < m; i++ {
		paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
	}
	if err := splitDatabase(source, paths); err != nil {
		log.Fatalf("splitting database: %v", err)
	}

	myAddress := net.JoinHostPort(getLocalAddress(), "3410")
	log.Printf("starting http server at %s", myAddress)
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))

	// bind on the port before launching the background goroutine on Serve
	// to prevent race condition with call to download below
	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		log.Fatalf("Listen error on address %s: %v", myAddress, err)
	}
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("Serve error: %v", err)
		}
	}()

	// build the map tasks
	var mapTasks []*MapTask
	for i := 0; i < m; i++ {
		task := &MapTask{
			M:          m,
			R:          r,
			N:          i,
			SourceHost: myAddress,
		}
		mapTasks = append(mapTasks, task)
	}

	// build the reduce tasks
	var reduceTasks []*ReduceTask
	for i := 0; i < r; i++ {
		task := &ReduceTask{
			M:           m,
			R:           r,
			N:           i,
			SourceHosts: make([]string, m),
		}
		reduceTasks = append(reduceTasks, task)
	}

	var client Client

	// process the map tasks
	for i, task := range mapTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing map task %d: %v", i, err)
		}
		for _, reduce := range reduceTasks {
			reduce.SourceHosts[i] = myAddress
		}
	}

	// // process the reduce tasks
	fmt.Printf("Skipping reduceTasks...")
	// for i, task := range reduceTasks {
	// 	if err := task.Process(tempdir, client); err != nil {
	// 		log.Fatalf("processing reduce task %d: %v", i, err)
	// 	}
	// }

	// gather outputs into final target.db file
	fmt.Printf("Done!")
}
