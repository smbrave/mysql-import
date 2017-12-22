package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"

	"strings"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
)

var (
	WaitGroupwait sync.WaitGroup
	DataChan      chan []byte
	ExitChan      chan struct{}
	Total         int64 = 0
	Succ          int64 = 0
	WaitGroup     sync.WaitGroup
	FieldName     []string
)

func Reader(options *Options) error {
	defer WaitGroup.Done()
	defer close(ExitChan)

	f, err := os.Open(options.File)
	if err != nil {
		fmt.Printf("err: %s\n", err.Error())
		return err
	}

	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, _, err := rd.ReadLine()
		if err != nil || io.EOF == err {
			break
		}

		DataChan <- line
	}

	return nil
}

func Worker(options *Options) error {
	defer WaitGroup.Done()

	fieldList := strings.Join(FieldName, ",")
	for {
		select {
		case line := <-DataChan:
			values := strings.Split(string(line), options.Separate)

			for i, _ := range values {
				values[i] = `'` + values[i] + `'`
			}

			sql := fmt.Sprintf("INSERT %s (%s) VALUES(%s)", options.Table, fieldList, strings.Join(values, ","))
			o := orm.NewOrm()
			raw := o.Raw(sql)
			_, err := raw.Exec()
			if err != nil {
				fmt.Printf("%s\n err:%s\n", sql, err.Error())
			}

		default:
			select {
			case <-ExitChan:
				goto exit
			default:

			}
		}
	}

exit:
	return nil
}

func Import(options *Options) error {

	FieldName = strings.Split(options.Fields, ",")
	dbSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", options.User, options.Password, options.Host, options.Port, options.Database)
	orm.RegisterDriver("mysql", orm.DRMySQL)
	orm.RegisterDataBase("default", "mysql", dbSource)

	DataChan = make(chan []byte, 10000)
	ExitChan = make(chan struct{})
	WaitGroup.Add(1)

	go Reader(options)
	for i := 0; i < options.Worker; i++ {
		WaitGroup.Add(1)
		go Worker(options)
	}

	WaitGroup.Wait()
	return nil
}
