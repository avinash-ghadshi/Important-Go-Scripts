package main

import (
	"fmt"
	"database/sql"
	"sync"
	"time"
	"strings"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/ClickHouse/clickhouse-go"
)

var (
	wgSrc      sync.WaitGroup
	wgDest     sync.WaitGroup
)

type rstrings struct{
	data []string
}

func (r *rstrings) appendString(s string) bool {
	r.data = append(r.data, s)
	return true
}

func GetCdbh() (db *sql.DB) {
	connect, err := sql.Open("clickhouse", "tcp://localhost:9000?database=default")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return connect
}

func destWorker(wid int, lcd *sql.DB, data chan int, stop chan bool) {

	defer wgDest.Done()
	defer fmt.Printf("DW(%d): Ending worker\n\n", wid)
	fmt.Printf("DW(%d): Starting worker\n", wid)
	var (
		ltx, _ = lcd.Begin()
		lstmt, _ = ltx.Prepare("INSERT INTO default.test (id, name, date) VALUES (?, ?, ?)")
		x = 0
		s = rstrings{}
	)
	defer lstmt.Close()
	for {
		select {
		case num := <-data:
			fmt.Printf("DW(%d): GOT VALUE: %d\n", wid, num);

			defer lstmt.Close()


			if x < 100 {
				name := "avg"+strconv.Itoa(num)
				str := strconv.Itoa(num) + "__" + name + "__2021-05-21"
				s.appendString(str)

				if _, err := lstmt.Exec(
					num,
					name,
					"2021-05-21",
				); err != nil {
					fmt.Println("DW(%d): Execution Failed\n", wid)
				}
				x++
			}

			if x == 100 {
				if err := ltx.Commit(); err != nil {
					fmt.Printf("DW(%d): Commit Failed\n", wid)
					fmt.Println("THIS WILL GET PUSH INTO REDIS: "+strings.Join(s.data,","))
				}
				x = 0;
				ltx, _   = lcd.Begin()
				lstmt, _ = ltx.Prepare("INSERT INTO default.test (id, name, date) VALUES (?, ?, ?)")
				s = rstrings{}
			}
		case <-stop:
			fmt.Printf("DW(%d): Records in Cache STOP: %d\n", wid, x)
			if err := ltx.Commit(); err != nil {
				fmt.Printf("DW(%d): Commit Failed in Stop\n", wid)
			}
			fmt.Println("THIS WILL GET PUSH INTO REDIS: "+strings.Join(s.data,","))
			return
		}
	}
}

func srcWorker(wid int, data chan int) {
	defer wgSrc.Done()
	defer fmt.Printf("SW(%d): Ending Worker\n\n", wid)
	fmt.Printf("SW(%d): Starting Worker\n", wid)
	for i := 0; i < 199; i++ {
		num := (wid * 100) +  i
		data <- num
	}
	return
}

func main() {
	cdb := GetCdbh()
	data := make(chan int, 32 * 1024)
	stop := make(chan bool)
	for i := 1; i <= 2; i++ {
		go srcWorker(i, data)
		wgSrc.Add(1)
		go destWorker(i, cdb, data, stop)
		wgDest.Add(1)
	}
	wgSrc.Wait()
	fmt.Println("Source Worker Done. Sending stop signal to Destination Worker.")
	for {
		if len(data) > 0 {
			time.Sleep(time.Second * 2)
		} else {
			break
		}
	}
	for w := 0; w < 2; w++ {
		stop <- true
	}
	wgDest.Wait()
	fmt.Println("All Done")
}
