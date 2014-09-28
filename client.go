package main

import(
  "fmt"
  "net"
  "encoding/gob"
  "log"
  "strconv"
  //"os"
  "encoding/json"
  //"strings"
)

type P struct{
  M,N int64
  reqId string
}



type custInfo struct{
  customerId string
  bankName string
  reqId string
  accountNumber string
  seqNumber int
}

type Params struct{
  ReqId string
  AccountNumber string
  Operation string
}


func (cust *custInfo) getReqId() string{
  seqn := strconv.Itoa(cust.seqNumber)
  cust.reqId = cust.customerId  + ":" + cust.bankName + ":" + seqn
  return cust.reqId
}

func (cust *custInfo) nextSeqNumber() int{
   cust.seqNumber = cust.seqNumber+1
   return cust.seqNumber
}

func (cust *custInfo) initializeValues(){
  cust.customerId = "123424"
  cust.bankName = "Bank1"
  cust.accountNumber = "234235346546"
  cust.seqNumber = 0
}

func (cust *custInfo) getBalance(conn net.Conn) uint64{
    fmt.Println("start client")
    encoder := gob.NewEncoder(conn)
    param := Params{"12432","234234","query"}
    encoder.Encode(param)
    fmt.Println("I am done")
    return 12
}

func main(){
    fmt.Println("start client")
    cust := &custInfo{}

    cust.initializeValues()
    fmt.Println("afdsa"+cust.customerId)
    cust.reqId = cust.getReqId()
    conn, err := net.Dial("tcp","localhost:8080")
    if(err != nil){
      log.Fatal("Connection error",err)
    }
    op := "query"

    fmt.Println("start client")
    param := Params{cust.reqId,cust.accountNumber,op}
    //encoder.Encode(p)
    fmt.Printf("%+v\n",param)
    b,err1 := json.Marshal(param)
    if(err1 != nil){
      fmt.Println("Error")
    }

    fmt.Printf("%v\n",b)
    conn.Write(b)
    rb := make([]byte,1000)
    n, err2 := conn.Read(rb)

    err1 = json.Unmarshal(rb[:n],&cust)

    if(err2!=nil || err1!=nil){
      fmt.Println(n)
    }

    s := string(rb[:n])
    fmt.Printf("Received %s",s)


}
