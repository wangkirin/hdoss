package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/vladimirvivien/gowfs"
)

func main() {
	conf := *gowfs.NewConfiguration()
	conf.Addr = "10.229.40.121:50070"
	conf.User = "hdfs"
	conf.DisableKeepAlives = false

	fs, err := gowfs.NewFileSystem(conf)
	log.Println(fs)
	if err != nil {
		log.Fatal(err)
	}
	data, err := fs.Open(gowfs.Path{Name: "/test/testfile.txt"}, 0, 512, 2048)
	if err != nil {
		fmt.Println("found error")
		log.Fatal(err)
	}
	rcvdData, _ := ioutil.ReadAll(data)
	fmt.Println(string(rcvdData))
}
