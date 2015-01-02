/* Server */

package main

import(
  "fmt"
  "net"
  "encoding/json"
  "log"
  "os"
  "time"
)

// In memory data structure for the storing the information of the accounts
var accountInfoMap = make(map[string]accountInfo)

//initialize the successor and predecessor values
var predecessor string
var successor = "localhost:8080"
var processedTrans = make(map[string]Hist)

type whoAmI struct{
  Identity string
}

// struct for storing the client info and this is forwarded to the succcessors in the chain
type clientProcessInfo struct{
  ClientSocketAddress string
  ReqId string
  Outcome string
  Operation string
}

// struct for storing the processed transactions
type Hist struct{
  AccountNumber string
  balance uint64
  Operation string
}


type Params struct{
  ReqId string
  AccountNumber string
  Operation string
}

// struct for storing the params from the client for the update
type ParamsUpdate struct{
  ReqId string
  AccountNumber string
  Operation string
  Amt uint64
}

// struct for Sending the processed msg to the successor
type sendMsg struct{
  ReqId string
  Outcome string
  Balance uint64
}

// struct for storing the account Info of the clients
type accountInfo struct{
  CustName string
  Balance uint64
  AccountType string
  AccNumber string
}

// Initialize the in memory map with the data
//To Do : have to read from a file rather than manually populating
func initializeAccountDetails(){
    ainfo := accountInfo{"sujan",3000,"Saving","123456789"}
    accountInfoMap["123456789"] = ainfo;
    ainfo2 := accountInfo{"Abhinav",5000,"current","123451234"}
    accountInfoMap["123451234"] = ainfo2;
}

//Implementing the deposit functionality
// To Do: Have to validate the request before performing the update
func (ainfo *accountInfo) processDeposit(amt uint64){
  ainfo.Balance+=amt
}


//Implementing the withdrawal functionality
// To Do: Have to validate the request before performing the update
func (ainfo *accountInfo) processWithdrawal(amt uint64){
  if ainfo.Balance < amt{
    fmt.Println("Insufficient funds")
  }else {
    fmt.Println("came to withdrawal request\n")
    ainfo.Balance=ainfo.Balance-amt
  }
}

//Generic function which will take the request and call the appropriate update handler
func (ainfo *accountInfo) processUpdate(p ParamsUpdate) uint64{

  switch(p.Operation){
    case "deposit":
      ainfo.processDeposit(p.Amt)
    case "withDrawal":
      ainfo.processWithdrawal(p.Amt)
    default:
      fmt.Println("Incorrect operation\n")
  }
  processedTrans[p.ReqId] = Hist{p.AccountNumber,ainfo.Balance,p.Operation}
  return ainfo.Balance
}

/*
This is a goroutine which will handle the connections from the client
Sequence of things happening in this function
-> reading the request from the client through the connection
-> getting the account struct/obj from the account info map based on the accountNumber in the client req
-> processing the request on that obj
-> updating the map with the processed obj/struct
-> making the go routine sleep until the successor reads the previous msg
-> passing the client info to the successor
*/
func handleConnection(p ParamsUpdate,conn net.Conn,accountInfoMap map[string]accountInfo,fromWhere whoAmI){
  accInfoBytes := make([]byte,1000)
  n, err := conn.Read(accInfoBytes)
  if(err != nil){

  }
  ainfo := accountInfo{}
  accInfoUnMarError := json.Unmarshal(accInfoBytes[:n], &ainfo)
  if(accInfoUnMarError != nil){
    fmt.Println("Unmarshalling Error in receiving update from predecessor\n")
  }
  // updating the tails data structures with the update from the successor
  //fmt.Println("afdas",ainfo.Balance)
  // reading the client info and state of the transaction from the predecessor
  clientProcessInfoBytes := make([]byte,1000)
  n2, cliProcReadByteError := conn.Read(clientProcessInfoBytes)
  fmt.Println("i was wrong connectingh to head taking time\n")
  if(cliProcReadByteError != nil) {
    fmt.Println("Error in Read")
  }
  cliProcessInfo := clientProcessInfo{}
  clientProcessUnMarError := json.Unmarshal(clientProcessInfoBytes[:n2], &cliProcessInfo)
  if(clientProcessUnMarError != nil) {
    fmt.Println("Error in Read")
  }
  if(cliProcessInfo.Outcome == "new"){
    accountInfoMap[ainfo.AccNumber] = ainfo
    fmt.Println("afdas",cliProcessInfo.ReqId)
    processedTrans[cliProcessInfo.ReqId] = Hist{ainfo.AccNumber,ainfo.Balance,cliProcessInfo.Operation}
  }
  connSuccessor,errSucc := net.Dial("tcp","localhost:8073")
  if errSucc != nil{
    fmt.Println("Error connecting to the successor\n")
  }
  me := fromWhere
  notifyIden,errIden := json.Marshal(me)
  if(errIden != nil){
      fmt.Println("Error in Marshalling the identity\n")
  }
  connSuccessor.Write(notifyIden)
  time.Sleep(100 * time.Millisecond)
  succ,errSuccUpdate := json.Marshal(ainfo)
  fmt.Println("Address",connSuccessor.LocalAddr().String())
  connSuccessor.Write(succ)
  time.Sleep(100 * time.Millisecond)
  //cliProceeInfo := clientProcessInfo{"predecessor",p.ReqId,"Processed"}
  succClientInfo,errSuccClientInfo := json.Marshal(cliProcessInfo)
  if(errSuccUpdate!=nil || errSuccClientInfo!=nil){
    fmt.Println("Error in Sending\n")
  }
  connSuccessor.Write(succClientInfo)
  fmt.Println("Written to successor\n")
}

/*
  this will identify the requested node
*/
func identifyTheRequestor(conn net.Conn) whoAmI{
  //fmt.Println("hiiii\n")
  b := make([]byte,1000)
  n, err := conn.Read(b)
  fmt.Println("reached 1\n")
  if(err != nil){
    fmt.Println("Error in read Identity\n")
  }
  fmt.Println("reached 2\n")
  fromWhere := whoAmI{}
  err1 := json.Unmarshal(b[:n],&fromWhere)
  if(err1!=nil){
    fmt.Println("Error in Identity\n")
  }
  fmt.Println("reached\n",fromWhere)
  return fromWhere
}


func logMessage(msg string){
  f, fileOpenError := os.OpenFile("../../log/IntermediateLog", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  if fileOpenError != nil {
    log.Fatalf("error opening file: %v", fileOpenError)
  }
  defer f.Close()
  log.SetOutput(f)
  log.Println(msg)
}

func startTheServer(){
  fmt.Println("starting the Intermediate server")
  lnInter,errInter := net.Listen("tcp",":8072")
  if(errInter != nil){
    fmt.Println("Error in the Intermediate Server\n")
      return;
  }
  var p ParamsUpdate
  for{
    conn, errInterAccept := lnInter.Accept()
    if errInterAccept!=nil {
      fmt.Println()
    }
    fromWhere := identifyTheRequestor(conn)
    go handleConnection(p,conn,accountInfoMap,fromWhere)
  }
}

func main(){
  initializeAccountDetails()
  startTheServer()
}
