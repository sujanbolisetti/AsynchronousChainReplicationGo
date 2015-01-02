/* Server */

package main

import(
"fmt"
"net"
"encoding/json"
"log"
"os"
"time"
"strconv"
)

var p interface{}
var chn = make(chan int)
const (
     HEAD = 1
     INTERNAL = 2
     TAIL = 3
  )
var indexInServerList int
var myAddress string
var serverConfigs = make(map[string][]service, 0)
var clientConfigs = make(map[string][]clientService, 0)

var serverType int
var tailUdpAddress string
// In memory data structure for the storing the information of the accounts
var accountInfoMap = make(map[string]accountInfo)
var sendSeq int
var receiveSeq int
var outcome string
//initialize the successor and predecessor values
var predecessor string
var successor string
var processedTrans = make(map[string]Hist)


type whoAmI struct{
MySocketAddress string
Identity string
}

type clientService struct{
ClientId string
ClientHost string
ClientPort string
ClientSendPort string
BankName string
}

type service struct{
ServerId string
Host string
Port string
UDPPort string
ServerType string
BankName string
}

// struct for storing the client info and this is forwarded to the succcessors in the chain
type clientProcessInfo struct{
ClientSocketAddress string
ReqId string
Outcome string
Operation string
}

// struct for the reply msg to the client
type replyMsg struct{
ReqId string
Outcome string
Balance uint64
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
  outcome= "Insufficient Funds"
  fmt.Println("Insufficient funds")
}else {
  fmt.Println("came to withdrawal request\n")
  ainfo.Balance=ainfo.Balance-amt
}
}

func (ainfo *accountInfo) validateRequest(p ParamsUpdate) string{
val,keyExists := processedTrans[p.ReqId]
fmt.Println(keyExists)
if(!keyExists){
  return "new"
}else if(keyExists){
  if(val.AccountNumber == p.AccountNumber && val.balance == p.Amt && val.Operation == p.Operation){
    return "doNothing/Processed"
  }else {
    return "InconsistentWithHistory"
  }
}
return ""
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

func createNewAccount(accNumber string){
  accountInfoMap[accNumber] = accountInfo{"xx",0,"Saving","accNumber"}
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
func handleHeadUdpConnection(p ParamsUpdate,conn *net.UDPConn,accountInfoMap map[string]accountInfo){
for{
    fromWhere := whoAmI{}
    IdentifyTheClient := make([]byte,1000)
    IdentityLength,cliAddr,readIdentityErr := conn.ReadFromUDP(IdentifyTheClient)
    if(readIdentityErr!=nil){
      fmt.Println("Error In reading the identity\n",readIdentityErr)
    }
    unMarshalClientIdentityErr := json.Unmarshal(IdentifyTheClient[:IdentityLength],&fromWhere)
    if(unMarshalClientIdentityErr!=nil){
      fmt.Println("unMarshalClientIdentity Error\n",unMarshalClientIdentityErr)
    }
    logMessage("Received",fromWhere)
    fmt.Printf("From Where %+v,%s",fromWhere,cliAddr)
    clientRequestBytes := make([]byte,1000)
    fmt.Println("came")
    length,Addr,readErr := conn.ReadFromUDP(clientRequestBytes)
    if(readErr != nil){
      fmt.Println("Read Error from the client connection\n")
    }
    unmarshalClientRequestErr := json.Unmarshal(clientRequestBytes[:length], &p)
    logMessage("Received",p)
    fmt.Println("unMarshalled\n")
    if(unmarshalClientRequestErr != nil) {
      fmt.Println("error while unmarshal\n")
    }
    _,isAccountExists := accountInfoMap[p.AccountNumber]
    if(!isAccountExists){
      createNewAccount(p.AccountNumber)
    }
    ainfo := accountInfoMap[p.AccountNumber]
    transInfo := ainfo.validateRequest(p)

    if(transInfo== "new"){
      balance := ainfo.processUpdate(p)
      fmt.Println("Balance",balance,Addr)
      accountInfoMap[p.AccountNumber] = ainfo
      if(outcome == ""){
        outcome = "Processed"
      }
    }else if(transInfo == "processed"){
      outcome = "resend"
    }else {
      outcome = "Inconsistent With History"
    }

    logMessage("Status","Connecting to the succesor\n")
    connSuccessor,errSucc := net.Dial("tcp",successor)
    if errSucc != nil{
      fmt.Println("Error connecting to the successor\n")
    }
    me := whoAmI{fromWhere.MySocketAddress,"predecessor"}
    notifyIden,errIden := json.Marshal(me)
    if(errIden != nil){
        fmt.Println("Error in Marshalling the identity\n")
    }
    connSuccessor.Write(notifyIden)
    logMessage("Send",me)
    time.Sleep(100 * time.Millisecond)
    succ,errSuccUpdate := json.Marshal(ainfo)
    fmt.Println("Address",connSuccessor.LocalAddr().String())
    connSuccessor.Write(succ)
    logMessage("Send",ainfo)
    time.Sleep(100 * time.Millisecond)
    cliProceeInfo := clientProcessInfo{fromWhere.MySocketAddress,p.ReqId,outcome,p.Operation}
    succClientInfo,errSuccClientInfo := json.Marshal(cliProceeInfo)
    if(errSuccUpdate!=nil || errSuccClientInfo!=nil){
      fmt.Println("Error in Sending\n")
    }
    connSuccessor.Write(succClientInfo)
    logMessage("Send",cliProceeInfo)
    fmt.Println("Written to successor\n")
}
}

func handleInternalConnection(p ParamsUpdate,conn net.Conn,accountInfoMap map[string]accountInfo,fromWhere whoAmI){
  accInfoBytes := make([]byte,1000)
  n, err := conn.Read(accInfoBytes)
  if(err != nil){
    fmt.Println("Error In reading Account Info",err)
  }
  ainfo := accountInfo{}
  accInfoUnMarError := json.Unmarshal(accInfoBytes[:n], &ainfo)
  if(accInfoUnMarError != nil){
    fmt.Println("Unmarshalling Error in receiving update from predecessor\n")
  }
  logMessage("Received",string(accInfoBytes[:n]))
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
    logMessage("Updated the history",processedTrans[cliProcessInfo.ReqId])
  }
  connSuccessor,errSucc := net.Dial("tcp",successor)
  logMessage("Status","Connecting to the Succesor :"+successor)
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
  logMessage("Send",ainfo)
  time.Sleep(100 * time.Millisecond)
  //cliProceeInfo := clientProcessInfo{"predecessor",p.ReqId,"Processed"}
  succClientInfo,errSuccClientInfo := json.Marshal(cliProcessInfo)
  if(errSuccUpdate!=nil || errSuccClientInfo!=nil){
    fmt.Println("Error in Sending\n")
  }
  connSuccessor.Write(succClientInfo)
  logMessage("Send",cliProcessInfo)
  fmt.Println("Written to successor\n")
}

func handleConnectionTailForQuery(p Params,conn *net.UDPConn,accountInfoMap map[string]accountInfo){
  set := make(map[string]bool)
  fmt.Println("status","Processing the Query by the Tail")
  for{
  requestBytes := make([]byte,1000)
  length,Addr,errInReading := conn.ReadFromUDP(requestBytes)
  if(errInReading != nil){
      fmt.Println("Error in reading the request from the client\n")
  }
  fmt.Println("Bytes",string(requestBytes),Addr)
  err1 := json.Unmarshal(requestBytes[:length], &p)
  if(err1 != nil) {
    fmt.Println("error while unmarshal in query\n")
  }
  fmt.Println("Params in Query",p)
  logMessage("Received",p)
  _,isAccountExists := accountInfoMap[p.AccountNumber]
  if(!isAccountExists){
    fmt.Println("came")
    createNewAccount(p.AccountNumber)
  }
  ainfo := accountInfoMap[p.AccountNumber]
  balance := ainfo.processQuery()
  fmt.Println("Balance:\n",balance)
  rm := replyMsg{}
  rm.ReqId = p.ReqId
  rm.Outcome = "Processed"
  rm.Balance = ainfo.Balance
  ack,err2 := json.Marshal(rm)
  if(err2 !=nil){}
  ra, err := net.ResolveUDPAddr("udp", Addr.String())
  fmt.Println("resolveAddr",ra)
  //time.Sleep(100 * time.Millisecond)
  _, isExists := set[p.ReqId];
  if !isExists{
  set[p.ReqId] = true
  n,err2 := conn.WriteToUDP(ack,ra)
  //conn.Write([]byte("fsdkjnf\n"))
  logMessage("Send",rm)
  logMessage("Status",conn.LocalAddr())
  fmt.Printf("Written to client %d %+v\n",n,rm)
  if(err2!=nil || err!=nil){
    fmt.Println("sdfs")
  }
  }else {
    fmt.Println("duplicate request")
  }
  //time.Sleep(100 * time.Millisecond)

}
}

/*
handleConnectionForUpdate : This function will handle the update requests from the predecessors.
                            and reply to the client
*/
func handleConnectionTailForUpdate(conn net.Conn){
  requesterInfo := make([]byte,1000)
  n2,err1 := conn.Read(requesterInfo)
  fmt.Println(string(requesterInfo[:n2]))
  logMessage("Received",string(requesterInfo[:n2]))
  // reading the update state of the object from the predecessor
  accInfoBytes := make([]byte,1000)
  n, err := conn.Read(accInfoBytes)
  if(err != nil || err1!=nil){
    fmt.Println("Error In Reading",err,err1)
  }
  ainfo := accountInfo{}
  accInfoUnMarError := json.Unmarshal(accInfoBytes[:n], &ainfo)
  if(accInfoUnMarError != nil){
    fmt.Println("Unmarshalling Error in receiving update from predecessor\n")
  }
  logMessage("Received",ainfo)
  accountInfoMap[ainfo.AccNumber] = ainfo
  logMessage("Status","Updated the accInfoMap")
  // reading the client info and state of the transaction from the predecessor
  clientProcessInfoBytes := make([]byte,1000)
  n2, cliProcReadByteError := conn.Read(clientProcessInfoBytes)
  fmt.Println("i was wrong connectingh to head taking time\n")
  if(cliProcReadByteError != nil) {
    fmt.Println("Error in Read")
  }
  //fmt.Println("clientProcess",ainfo,string(accInfoBytes[:n]))
  cliProcessInfo := clientProcessInfo{}
  clientProcessUnMarError := json.Unmarshal(clientProcessInfoBytes[:n2], &cliProcessInfo)
  if(clientProcessUnMarError != nil) {
    fmt.Println("Error in Read")
  }
  processedTrans[cliProcessInfo.ReqId] = Hist{ainfo.AccNumber,ainfo.Balance,cliProcessInfo.Operation}
  fmt.Printf("process Trans %+v\n",processedTrans)
  logMessage("Status",processedTrans)
  connClient,connClientErr := net.Dial("udp",cliProcessInfo.ClientSocketAddress)
  if(connClientErr!=nil){
    fmt.Println("Error in connecting client in the tail\n")
  }
  rm := replyMsg{}
  rm.ReqId = cliProcessInfo.ReqId
  rm.Outcome = cliProcessInfo.Outcome
  rm.Balance = ainfo.Balance
  ack,err2 := json.Marshal(rm)
  if(err2 !=nil){
    fmt.Println("Error ")
  }
  connClient.Write(ack)
  logMessage("Send",rm)
}

func (ainfo *accountInfo) processQuery() uint64{
  return ainfo.Balance
}

func logMessage(msgType string,msg interface{}){
  temp := 0
  configData := serverConfigs["serverConfig"][indexInServerList]
  logFileName:= configData.ServerType + configData.BankName + configData.ServerId + ".log"
  // if(err!=nil){
  //   fmt.Println("Error in reading arguments",err)
  // }
  f, fileOpenError := os.OpenFile("../../log/"+logFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
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

/*
this will identify the requested node
*/
func identifyTheRequestorForTcp(conn net.Conn) whoAmI{
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

func startTailTcpServer(){
  fmt.Println("Start a Server\n")
  lnTail,errTail := net.Listen("tcp",myAddress)
  if(errTail != nil){
    fmt.Println("Error in connection")
      return;
  }
  var errAcceptTail error
  var conn net.Conn
  for {
      conn, errAcceptTail = lnTail.Accept()
      fmt.Println("inside the connection\n")
      if(errAcceptTail!=nil) {
        fmt.Println("Error in Connection\n")
      }
      handleConnectionTailForUpdate(conn)
    }
}

func startTailUdpServer(){
  fmt.Println("Start Udp a Server\n")
  sAddr, err4 := net.ResolveUDPAddr("udp",tailUdpAddress)
  var p Params
  conn,errAcceptTail := net.ListenUDP("udp",sAddr)
  fromWhere := identifyTheRequestorForUdp(conn)
  fmt.Println(fromWhere)
  fmt.Println("inside the connection\n")
  if(errAcceptTail!=nil || err4!=nil) {
      fmt.Println("Error in UDP Connection\n")
  }

  handleConnectionTailForQuery(p,conn,accountInfoMap)
}


func startInternalTcpServer(){
  fmt.Println("starting the Intermediate server")
  lnInter,errInter := net.Listen("tcp",myAddress)
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
    fromWhere := identifyTheRequestorForTcp(conn)
    go handleInternalConnection(p,conn,accountInfoMap,fromWhere)
  }
}


func startHeadTcpServer(){
fmt.Println("starting the head server")
lnHead,errHead := net.Listen("tcp",myAddress)
if(errHead != nil){
  fmt.Println("Error in the Head Server\n")
  return;
}
//var p ParamsUpdate
for{
  conn, errAcceptHead := lnHead.Accept()
  if errAcceptHead!=nil {
    fmt.Println(conn)
  //handleConnection(p,conn,accountInfoMap)
  }
}
}

func identifyTheRequestorForUdp(conn *net.UDPConn) whoAmI{
//fmt.Println("hiiii\n")
  b := make([]byte,1000)
  n,Addr,err := conn.ReadFromUDP(b)
  fmt.Println("reached 1\n")
  if(err != nil){
    fmt.Println("Error in read Identity\n")
  }
  fmt.Println("reached 2\n")
  fromWhere := whoAmI{}
  fmt.Println(string(b[:n]))
  err1 := json.Unmarshal(b[:n],&fromWhere)
  if(err1!=nil){
    fmt.Println("Error in Marshal\n")
  }
  fmt.Println("reached\n",fromWhere,Addr)
  return fromWhere
}

func startServers(){
switch(serverType){
  case HEAD:
                go startHeadUdpServer()
                go startHeadTcpServer()
  case INTERNAL:
                startInternalTcpServer()
  case TAIL:
                go startTailUdpServer()
                go startTailTcpServer()
}
}



func startHeadUdpServer(){
  fmt.Println("Start Udp a Server\n")
  sAddr, err4 := net.ResolveUDPAddr("udp", myAddress)
  var p ParamsUpdate
  conn,errAcceptTail := net.ListenUDP("udp",sAddr)
  //fromWhere := identifyTheRequestorForUdp(conn)
  fmt.Println("inside the connection\n")
  if(errAcceptTail!=nil || err4!=nil) {
      fmt.Println("Error in UDP Connection\n")
  }
  handleHeadUdpConnection(p,conn,accountInfoMap)
}

func assignDesignationToServer(){
  var err error
  indexInServerList,err = strconv.Atoi(os.Args[1])
  if(err!=nil){
    fmt.Println("str conv error",err)
  }
  myAddress=serverConfigs["serverConfig"][indexInServerList].Host + ":" + serverConfigs["serverConfig"][indexInServerList].Port
  fmt.Println("myAddress",myAddress)
  if(serverConfigs["serverConfig"][indexInServerList].ServerType=="HEAD"){
    serverType=HEAD
    predecessor=""
    successor=serverConfigs["serverConfig"][indexInServerList+1].Host + ":" + serverConfigs["serverConfig"][indexInServerList+1].Port
  } else if (serverConfigs["serverConfig"][indexInServerList].ServerType=="INTERNAL"){
    serverType=INTERNAL
    predecessor=serverConfigs["serverConfig"][indexInServerList-1].Host + ":" + serverConfigs["serverConfig"][indexInServerList-1].Port
    successor=serverConfigs["serverConfig"][indexInServerList+1].Host + ":" + serverConfigs["serverConfig"][indexInServerList+1].Port
  } else {
    serverType=TAIL
    tailUdpAddress = serverConfigs["serverConfig"][indexInServerList].Host + ":" + serverConfigs["serverConfig"][indexInServerList].UDPPort
    predecessor=serverConfigs["serverConfig"][indexInServerList-1].Host + ":" + serverConfigs["serverConfig"][indexInServerList-1].Port
  }
}

func readClientConfig(){
  f, fileOpenError := os.OpenFile("../../config/clientConfig.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  err := json.NewDecoder(f).Decode(&clientConfigs)
  if(fileOpenError!=nil || err!=nil){
    fmt.Println("file open error",fileOpenError,err)
  }
  //fmt.Printf("yeah client servers %+v\n",clientConfigs)
}

func readServerConfig(){
  f, fileOpenError := os.OpenFile("../../config/Bank1Servers.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  err := json.NewDecoder(f).Decode(&serverConfigs)
  if(fileOpenError!=nil || err!=nil){
    fmt.Println("file open error",fileOpenError,err)
  }
//fmt.Printf("yeah servers %+v\n",serverConfigs)
}

func main(){
  readServerConfig()
  readClientConfig()
  assignDesignationToServer()
  initializeAccountDetails()
  startServers()
  <-chn
}
