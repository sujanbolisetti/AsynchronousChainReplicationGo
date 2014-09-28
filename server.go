package main

import(
  "fmt"
  "net"
  //"encoding/gob"
  "encoding/json"
)


type P struct{
    M,N int64
    reqId string
}

type Params struct{
  ReqId string
  AccountNumber string
  Operation string

}

type accountInfo struct{
  custName string
  balance uint64
  accountType string
}



func (ainfo *accountInfo) processQuery() uint64{
  return ainfo.balance
}

func handleConnection(p Params,conn net.Conn,accountInfoMap map[string]accountInfo){
  //dec := gob.NewDecoder(conn)

  //dec.Decode(p)
  b := make([]byte,1000)
  n, err := conn.Read(b)

  err1 := json.Unmarshal(b[:n], &p)

  if(err!=nil || err1!=nil){
    fmt.Println(n)
  }

  s := string(b[:n])
  fmt.Printf("Received %s",s)
  fmt.Printf("%+v",p)
  ainfo := accountInfoMap[p.AccountNumber]
  balance := ainfo.processQuery()
  m := make(map[string]uint64)
  m["Balance"] =  balance
  ack,err2 := json.Marshal(m)
  if(err2!=nil){}
  conn.Write(ack)
  fmt.Printf("balance: %d",balance)
}



func main(){
  fmt.Println("start")
  accountInfoMap  := make(map[string]accountInfo)
  //ainfo := &accountInfo{}
  accountInfoMap["234235346546"] = accountInfo{
                                                "Sujan",200,"Savings",
                                              }
  //ainfo.initializeAccountInfo(accountInfoMap)
  //fmt.Printf("the map %+v",accountInfoMap)
  ln,err := net.Listen("tcp",":8080")
  if(err!=nil){
    fmt.Println("Error in connection")
      return;
  }
  for{
  var p Params
    conn, err := ln.Accept()

    if err!=nil{
      continue;
    }
    go handleConnection(p,conn,accountInfoMap)

  }

}
