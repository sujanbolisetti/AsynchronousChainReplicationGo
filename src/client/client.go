package main

import(
  "fmt"
  "net"
  "log"
  "strconv"
  "math/rand"
  "os"
  "encoding/json"
  "time"
)


var chn = make(chan int)
var clientIndex int
var sendSeq int
var receiveSeq int
var connTail *net.UDPConn
var clientConfigs = make(map[string][]clientService, 0)
var serverConfigs = make(map[string][]service, 0)
var clientReqs = make(map[string][]clientRequests, 0)
var myAddress string
var myQueryAddress string
var headAddress string
var tailAddress string
var connUpdRep *net.UDPConn
var tailConnError error
var outputChannel = make(chan []byte)
var failureChannel = make(chan int)
var index int



type Notification struct{
  FailureType string
  NewServerAddress string
  IsExtendingTheChain bool
}
// struct for storing the customers account info
type custInfo struct{
  customerId string
  bankName string
  reqId string
  accountNumber string
  seqNumber int
}


type clientService struct{
ClientId string
ClientHost string
ClientPort string
ClientSendPort string
BankName string
ClientMasPort string
}

type service struct{
ServerId string
Host string
Port string
UDPPort string
ServerType string
BankName string
}

type whoAmI struct{
  MySocketAddress string
  Identity string
}



// type GlobalNotification struct{
//   FromWhere whoAmI
//   RoleChange changeRole
//   Notif Notification
//   AccInfo accountInfo
//   ClientInfo clientProcessInfo
//   Ack ackToPred
// }

// struct format for receiving the message from the client
type receiveMsg struct{
  ReqId string
  Outcome string
  Balance uint64
  AccountNumber string
}

// struct for query request parameters
type Params struct{
  ReqId string
  AccountNumber string
  Operation string
}

type clientRequests struct{
  ReqId string
  AccountNumber string
  Operation string
  Amt string
}

// struct for update request parameters
type ParamsUpdate struct{
  ReqId string
  AccountNumber string
  Operation string
  Amt uint64
}


func initializeAddresses(){
  headAddress = serverConfigs["CitiBank"][0].Host + ":" + serverConfigs["CitiBank"][0].Port
  fmt.Println(headAddress)
}

func getTailAddress(){
  for i:=0;i<len(serverConfigs["CitiBank"]);i++ {
    if(serverConfigs["CitiBank"][i].ServerType == "TAIL"){
      tailAddress = serverConfigs["CitiBank"][i].Host + ":" + serverConfigs["CitiBank"][i].UDPPort
    }
  }
}

// this function will generate the unique request id
func (cust *custInfo) getReqId() string{
  rand.Seed(time.Now().UTC().UnixNano())
  //fmt.Println("seqn",seqn)
  seqn := strconv.Itoa(rand.Intn(500))
  cust.reqId = cust.customerId  + ":" + "1" + ":" + seqn
  return cust.reqId
}

// sequence number generator
func (cust *custInfo) nextSeqNumber() int{
   cust.seqNumber = cust.seqNumber+1
   return cust.seqNumber
}

// function to initialize values for the client's account
func (cust *custInfo) initializeValues(){
  cust.customerId = "123424"
  cust.bankName = "Bank1"
  cust.accountNumber = "123456789"
  cust.seqNumber = 0
}

func receiveFailNotiFromMaster(conn net.Conn){
  //fmt.Println("Hurray!!!!!!!!!!\n")
  failureInfo := make([]byte,1024)
  //fmt.Println("Here Again I Again")
  leng,err := conn.Read(failureInfo)
//  fmt.Println("Here Again I Again")
  if(err!=nil){
    fmt.Println("errr",err)
  }
  var fnObj Notification
  errUnmarshal := json.Unmarshal(failureInfo[:leng],&fnObj)
  if(errUnmarshal != nil){
    fmt.Println("Error in Notification from Master\n",errUnmarshal)
  }
  fmt.Println("New Addr",fnObj.NewServerAddress)
  fmt.Println("Change Type",fnObj.FailureType)
  if(fnObj.FailureType == "HEAD"){
    headAddress = fnObj.NewServerAddress
  }else if (fnObj.FailureType == "TAIL" || fnObj.FailureType == "Extending The Chain"){
    fmt.Println("Failure Type",fnObj.FailureType,fnObj.NewServerAddress)
    tailAddress = fnObj.NewServerAddress
  }
  logMessage("Received","Received Notification from Master")
  logMessage("Received",fnObj)
  failureChannel<-0
  conn.Close()
}


func receiveMsgFromTail(tailConn *net.UDPConn,op string){
  fmt.Println("reading msg\n")
  replyMsgBytes := make([]byte,1000)
  length,errReadingFromTail := tailConn.Read(replyMsgBytes)
  logMessage("Status","Reading from the tail")
  fmt.Println("after read\n")
  if(errReadingFromTail != nil){
    fmt.Println("Error reading from the tail",errReadingFromTail)
  }
  rem := receiveMsg{}
  unMarshalError := json.Unmarshal(replyMsgBytes[:length], &rem)
  if(unMarshalError!=nil){
    fmt.Println("Error when Unmarshalling the tails reply",unMarshalError)
  }
  fmt.Printf("%+v\n",rem)
  logMessage("Received",rem)
  if(op=="Query"){
    tailConn.Close()
  }
  chn <- 0
}


// this method will contact the head with the deposit update request
func (cust *custInfo) doDeposit(amt uint64){
    //cAddr, err3 := net.ResolveUDPAddr("udp", ":0")
    //sAddr, err4 := net.ResolveUDPAddr("udp", headAddress)
    connHead, errHead := net.Dial("udp",headAddress)
    if(errHead != nil){
      fmt.Println(" dfs",headAddress)
      fmt.Println("Error Connecting to the Head\n",errHead,headAddress)
    }
    // if(err3!=nil || err4!=nil){
    //   log.Fatal("Connection error",err4)
    // }
    notifyMe(connHead)
    fmt.Println("asda\n")
    param := ParamsUpdate{cust.reqId,cust.accountNumber,"deposit",amt}
    b,err1 := json.Marshal(param)
    if(err1 != nil){
      fmt.Println("Error")
    }
    connHead.Write(b)
    go receiveMsgFromTail(connTail,"Withdrawal")
    fmt.Println("Written to Head")
    //defer connHead.Close()
}

func resendRequest(i int,){

}


// this method will contact the head with the withdrawal update request
func (cust *custInfo) doWithdrawal(amt uint64){
    cAddr, err3 := net.ResolveUDPAddr("udp",":0")
    sAddr, err4 := net.ResolveUDPAddr("udp", headAddress)
    connHead, errHead := net.DialUDP("udp",cAddr,sAddr)
    logMessage("Status","Connecting to Head for Withdrawal")
    if(errHead != nil){
      fmt.Println("Err",errHead)
    }
    if(err3!=nil || err4!=nil){
      log.Fatal("Connection error",err4)
    }
    notifyMe(connHead)
    param := ParamsUpdate{cust.reqId,cust.accountNumber,"withDrawal",amt}
    b,err1 := json.Marshal(param)
    if(err1 != nil){
      fmt.Println("Error")
    }
    connHead.Write(b)
    logMessage("Send",param)
    fmt.Println("Withdrawal Request Sent\n")
    go receiveMsgFromTail(connTail,"Withdrawal")
    defer connHead.Close()
}

func notifyMe(conn net.Conn){
  //notify that i am the client
  me := whoAmI{myAddress,"client"}
  notifyIden,errIden := json.Marshal(me)
  if(errIden != nil){
    fmt.Println("Error in Marshalling the identity\n")
  }
  conn.Write(notifyIden)
}

// this method will contact the head with the query request
func (cust *custInfo) getBalance(ReqId string){
    cAddr, err3 := net.ResolveUDPAddr("udp",":0")
    /* I have to update the server config and read the value by looping */

    fmt.Println("tail Address, myQuery",tailAddress,myQueryAddress)
    sAddr, err4 := net.ResolveUDPAddr("udp", tailAddress)
    if(err3!=nil || err4!=nil){
      log.Fatal("Connection error",err4)
    }
    connToTail, err := net.DialUDP("udp",cAddr,sAddr)
    logMessage("Status","Connecting to the tail")
    if(err!=nil){
      log.Fatal("Connection error",err)
    }
    // notifyMe(connToTail)
    param := Params{ReqId,cust.accountNumber,"query"}
    fmt.Println("Params in client",param)
    b,err1 := json.Marshal(param)
    if(err1 != nil){
      fmt.Println("Error")
    }
    connToTail.Write(b)
    go receiveMsgFromTail(connToTail,"Query")
    // connToTail.Close()
}

// this function will send the requests to the head/tail servers based on the user input.
// To Do : have to make this function read the set of requests in the configuration file.
func sendRequests(cust custInfo){

  /*
  Scenario for Extending the Chain this is if you want to do at the starting of the process

  time.Sleep(30*time.Second)

  */
  fmt.Println("start client")
  cliReqs := clientReqs["Requests"]
  var count =0
  var opCode int
  var i int;
  var amt uint64
  fmt.Println("as\n")
  for i=0;i<len(cliReqs);i++ {
    cust.reqId = cliReqs[i].ReqId
    if(cliReqs[i].Amt != ""){
      amount,errReadingAmt := strconv.Atoi(cliReqs[i].Amt)
      amt = uint64(amount)

    if(errReadingAmt != nil){
      fmt.Println("error reading the amount from request",errReadingAmt)
    }
    }
    op := cliReqs[i].Operation
    cust.accountNumber = cliReqs[i].AccountNumber
    fmt.Println("operation",op)
    switch op {
      case "Query":
          fmt.Println("********")
          go cust.getBalance(cust.reqId)
          opCode = 1
          break
      case "Deposit":
          go cust.doDeposit(amt)
          opCode = 2
          break
      case "Withdrawal":
          go cust.doWithdrawal(amt)
          opCode = 3
          break
      default:
          fmt.Println("Wrong input!!!!!!",opCode)
       }
       /* For Internal We wont need delay here */
        time.Sleep(10*time.Second)
        select{
          case <-chn:
              fmt.Println("in Channel\n")
              continue
          case <- time.After(50*time.Second):
              logMessage("Status","Opps time out")
              fmt.Println("Time Out!!!!!!!!")
              if(count < 3){
                count = count + 1
                i=i-1
              }
          }
          count = 0
          fmt.Println("pass",i,os.Args[1])
  }

}

func startClientMasServer(){
  clientIndex,_ = strconv.Atoi(os.Args[1])
  masAddr := clientConfigs["clientConfig"][clientIndex].ClientHost + ":" + clientConfigs["clientConfig"][clientIndex].ClientMasPort
  ln,Err := net.Listen("tcp",masAddr)
  if(Err!=nil){
    fmt.Println("Error in Connecting",Err)
  }
  for{
    conn,errCon := ln.Accept()
    if(errCon != nil){
      fmt.Println("Error in conn",errCon)
    }
    go receiveFailNotiFromMaster(conn)
  }
}




/*
  this function will start the client server for accepting the updates from the tail
*/
func startClientServer(){
  var convError error
  clientIndex,convError = strconv.Atoi(os.Args[1])
  if(convError!=nil){
    fmt.Println("Conversion Error")
  }
  myAddress = clientConfigs["clientConfig"][clientIndex].ClientHost + ":" + clientConfigs["clientConfig"][clientIndex].ClientPort
  myQueryAddress = clientConfigs["clientConfig"][clientIndex].ClientHost + ":" + clientConfigs["clientConfig"][clientIndex].ClientSendPort
  cliAddr, err4 := net.ResolveUDPAddr("udp", myAddress)
  if(err4!=nil){
    fmt.Println("cliAddr",err4)
  }
  var tailConnError error
  connTail,tailConnError = net.ListenUDP("udp",cliAddr)
  logMessage("Status","Started the client server")
  if(tailConnError != nil){
    fmt.Println("Error in Connecting the client\n")
  }
}

func readClientsConfig(){
  f, fileOpenError := os.OpenFile("../../config/clientConfig.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  err := json.NewDecoder(f).Decode(&clientConfigs)
  if(fileOpenError!=nil || err!=nil){
    fmt.Println("file open error",fileOpenError,err)
  }
}

func readClientRequestsConfig(){
  f, fileOpenError := os.OpenFile("../../config/client"+os.Args[1]+"ReqInfo.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  logMessage("Status","Reading the clients Request Configuration configuration")
  err := json.NewDecoder(f).Decode(&clientReqs)
  if(fileOpenError!=nil || err!=nil){
    fmt.Println("file open error",fileOpenError,err)
  }
}

func readServerConfig(){
f, fileOpenError := os.OpenFile("../../config/Bank1Servers.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
err := json.NewDecoder(f).Decode(&serverConfigs)
if(fileOpenError!=nil || err!=nil){
  fmt.Println("file open error",fileOpenError,err)
  }
}



func logMessage(msgType string,msg interface{}){
  temp := 0
  configData := clientConfigs["clientConfig"][clientIndex]
  logFileName:= configData.BankName + configData.ClientId + ".log"
  f, fileOpenError := os.OpenFile("../../logs/"+logFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  if fileOpenError != nil {
    log.Fatalf("error opening file: %v", fileOpenError)
  }
  if(msgType == "Received"){
    receiveSeq++
    temp=receiveSeq
  }else if(msgType == "Send") {
    sendSeq++
    temp=sendSeq
  }
  defer f.Close()
  log.SetOutput(f)
  if(temp!=0){
    log.Println(msgType," Sequence Number:",temp," ",msg)
  }else {
    log.Println(msg)
  }
}

func main(){
    readClientsConfig()
    readClientRequestsConfig()
    readServerConfig()
    initializeAddresses()
    getTailAddress()
    cust := custInfo{}
    cust.initializeValues()
    startClientServer()
    go startClientMasServer()
    sendRequests(cust)
    <-chn
}
