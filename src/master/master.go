package main

import(
  "fmt"
  "net"
  //"time"
  "os"
  "encoding/json"
  )
type configInfo struct{
  Servers    []string
  Clients    []string
  Banks      []string
  Lengthofbankchain1 []string
}

var config = configInfo{}

type receivePing struct{
  port uint64
  isAlive bool
}

func readConfig(){
  file, _ := os.Open("../../config/conf.json")
  decoder := json.NewDecoder(file)
  decodeErr := decoder.Decode(&config)
  if decodeErr != nil {
    fmt.Println("error:", decodeErr)
  }
  fmt.Printf("config Info %+v",config)
}

func createMap(){
  serversMap := make(map[string]bool)
  var length int
  length = len(config.Servers)
  for i:=0; i<length; i++ {
    serversMap[config.Servers[i]] = false
  }
  clientsMap := make(map[string]bool)
  length = len(config.Clients)
  for i:=0; i<length; i++ {
    clientsMap[config.Clients[i]] = false
  }
  //fmt.Printf("Servers Map %+v\n",serversMap)
  //fmt.Printf("clients Map %+v\n",clientsMap)
}

func handleConnection(conn net.Conn){
  b := make([]byte,2000)
  n, readErr := conn.Read(b)
  if(readErr != nil){

  }
  rp := receivePing{}
  marshalErr := json.Unmarshal(b[:n], &rp)
  if(marshalErr != nil){

  }

}

func main(){
  ln,lnError := net.Listen("tcp",":8090")
  //c := make(chan int)
  if(lnError != nil){
    fmt.Println("Error in correction\n")
  }
  readConfig()
  createMap()
  for{
  conn,connError := ln.Accept()
  if(connError!=nil){

  }
  go handleConnection(conn)
  }
  //fmt.Println(<-c)
}
