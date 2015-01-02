/* Server */

// TO DO : Have to write all the helper functions in a different file or in one method

package main

import(
"fmt"
"net"
"encoding/json"
"log"
"os"
"time"
"sync"
"strconv"
)

var p interface{}
var chn = make(chan int)
const (
     HEAD = 1
     INTERNAL = 2
     TAIL = 3
     NEWNODE=4
  )


/* -------------------- Global Variables and Global DataStructures --------------------*/

var failedIndexNum int
var sentReqList []string
var masterConn net.Conn
var timeToFail time.Duration
var headServerId string
var indexInServerList int
var newNodeCame bool
var myAddress string
var headUdpAddr string
var serverConfigs = make(map[string][]service, 0)
var failureConfigsMap = make(map[string][]FailureConfigs,0)
var clientConfigs = make(map[string][]clientService, 0)
var timeout = make(chan bool, 0)
var serverType int
var tailUdpAddress string
// In memory data structure for the storing the information of the accounts
var accountInfoMap = make(map[string]accountInfo)
var sendSeq int
var receiveSeq int
var outcome string
var shouldIDie bool
var mySecondRequest bool

//initialize the successor and predecessor values
var predecessor string
var lifetime int
var successor string
var processedTrans = make(map[string]Hist)
var mutex = &sync.Mutex{}
var isNewServerFailDurExt bool
var haltTheSystem bool
var clientInfo = make(map[string]clientProcessInfo)

type ackToPred struct{
  ReqId string
}

type Notification struct{
  FailureType string
  NewServerAddress string
  IsExtendingTheChain bool
  SendSeq int
}

type FailureConfigs struct{
  FailureType string
  SendSeqNum string
  Index string
}


type GlobalNotification struct{
  FromWhere whoAmI
  RoleChange changeRole
  Notif Notification
  AccInfo accountInfo
  ClientInfo clientProcessInfo
  Ack ackToPred
  TransHist transferStruct
  isNewSerGoingOn bool
}



type PingMsg struct{
  BankName string
  ServerId string
  IsAlive bool
  IsNewServer bool
}


type transferStruct struct{
  Trans map[string]Hist
  AccMap map[string]accountInfo
  CliReqInfo map[string]clientProcessInfo
}


type whoAmI struct{
MySocketAddress string
Identity string
}

type changeRole struct{
    NewRole string
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
  AccountNumber string
}

// struct for storing the processed transactions
type Hist struct{
  AccountNumber string
  Balance uint64
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

/* ---------------------End Of Global Structures declaration ---------------- */

/* ---------------------Helper functions for updates and Queries ---------------- */

// Initialize the in memory map with the data

func initializeAccountDetails(){
  ainfo := accountInfo{"sujan",3000,"Saving","123456789"}
  accountInfoMap["123456789"] = ainfo;
  ainfo2 := accountInfo{"Abhinav",5000,"current","123451234"}
  accountInfoMap["123451234"] = ainfo2;
  isNewServerFailDurExt = false
  lifetime =1
}

/*
  Helper Method to reply to the client
*/

func replyToClient(gn GlobalNotification){
  connClient,connClientErr := net.Dial("udp",gn.ClientInfo.ClientSocketAddress)
  if(connClientErr!=nil){
    fmt.Println("Error in connecting client in the tail",connClientErr,gn.ClientInfo.ClientSocketAddress)
  }
  fmt.Println("return Message\n")
  rm := replyMsg{}
  rm.ReqId = gn.ClientInfo.ReqId
  rm.Outcome = gn.ClientInfo.Outcome
  rm.Balance = gn.AccInfo.Balance
  rm.AccountNumber = gn.AccInfo.AccNumber
  reply,err2 := json.Marshal(rm)
  if(err2 !=nil){
    fmt.Println("Error ",err2)
  }
  connClient.Write(reply)
  logMessage("Send",rm)
}


/*
  This Method is called incase of the internal failures
  Scenario : Successor will send the seqNumber in the sent List to the predecessor
*/

func sendSendSeq(masConn net.Conn,gn GlobalNotification){
  predecessor = gn.Notif.NewServerAddress
  fmt.Println("Got the Predexessor Address",predecessor)
  seqNum := len(sentReqList)
  notifSeq := Notification{"","",false,seqNum}
  marNotif,ErrNotif := json.Marshal(notifSeq)
  if(ErrNotif!=nil){
    fmt.Println("Error in Marshalling the Noification of Seqnum",ErrNotif)
  }
  // if(indexInServerList == 4){
  //   fmt.Println("Predecessor>>>>>")
  //   time.Sleep(142342342*time.Second)
  // }

  // if(indexInServerList == failedIndexNum && shouldIDie){
  //     fmt.Println("Wow Came inside this",myAddress)
  //     time.Sleep(time.Second*453634645)
  // }

  masConn.Write(marNotif)
  logMessage("Send",seqNum)
  //fmt.Println("seqNum",seqNum,sentReqList)
}


func receiveSeqNum(masConn net.Conn,gn GlobalNotification){
  go sendRemainingTransToSucc(masConn,gn)
}

/*
  sendRemainingTransToSucc : Here the predecessor will send the remaining transactions to the successoe
                              and make it upto date

*/

func sendRemainingTransToSucc(conn net.Conn,gn GlobalNotification) {
  logMessage("Send","Connecting to the Successor for Transferring Remaining Updates")
  fmt.Println("Index Server List in Pred",indexInServerList)
  // if(indexInServerList == 2){
  //   fmt.Println("Predecessor>>>>>")
  //   time.Sleep(142342342*time.Second)
  // }

  if(indexInServerList == failedIndexNum && shouldIDie){
      fmt.Println("Wow Came inside this",myAddress)
      time.Sleep(time.Second*453634645)
  }
  var index int
  notif := gn.Notif
  successor = notif.NewServerAddress
  connNewSucc,ErrSuc := net.Dial("tcp",notif.NewServerAddress)
  fmt.Println("Updated the Successor Info",notif.NewServerAddress)
  if(ErrSuc!=nil){
    fmt.Println("Error In Connecting to the New Successor",ErrSuc)
  }
  me := whoAmI{myAddress,"NewPredecessor"}
  fmt.Println("Sap dsf",notif.SendSeq)
  fmt.Println("Sending the Updates to New Succ")
  if(notif.SendSeq >= 0){
    if(notif.SendSeq > 0){
      index = notif.SendSeq-1
    }
    if(index < len(sentReqList)){
    fmt.Println("seq",notif.SendSeq,len(sentReqList))
    temp := sentReqList[index:]
    unProcessedTrans := make(map[string]Hist)
    for i:=0;i<len(temp);i++ {
      val,keyExists := processedTrans[temp[i]]
      if(keyExists){
        unProcessedTrans[temp[i]] = val
      }
    }
    //fmt.Println("client socket address",clientInfo)
    //fmt.Println("")
    temp2 := make(map[string]accountInfo)
    tf := transferStruct{unProcessedTrans,temp2,clientInfo}
    gn := GlobalNotification{me,changeRole{},Notification{},accountInfo{},clientProcessInfo{},ackToPred{},tf,false}
    gnMar,ErrMargn := json.Marshal(gn)
    if(ErrMargn != nil){
      fmt.Println("Error",ErrMargn)
    }
    connNewSucc.Write(gnMar)
    logMessage("Send",gn)
  }
  }else{
    fmt.Println("Surprisingly We have upto date info")
  }
}

/*
  receiveUpdateProcTranFromNewPred : Here the new successor will receive the updates
                                     and forwards to the chain


*/

func receiveUpdateProcTranFromNewPred(conn net.Conn,gn GlobalNotification){
  logMessage("Status","Connecting to the New Predecessor")
  var connSuccessor net.Conn

  if(indexInServerList == failedIndexNum && shouldIDie){
      fmt.Println("Wow Came inside this",myAddress)
      time.Sleep(time.Second*453634645)
  }
  var errSucc error
  if(successor != ""){
    connSuccessor,errSucc = net.Dial("tcp",successor)
    if(errSucc  != nil){
      fmt.Println("Error in connecting",errSucc)
    }
  }
  for key, value := range gn.TransHist.Trans {
    _, isExists := processedTrans[key];
    if(isExists){
      continue
    }
    processedTrans[key] = value
    logMessage("Status","Connecting to the Succesor :"+successor)
    me := whoAmI{"","predecessor"}
    accountInfoMap[value.AccountNumber] = accountInfo{"xx",value.Balance,"Savings",value.AccountNumber}
    logMessage("Send",accountInfoMap[value.AccountNumber])
    cliInfo := gn.TransHist.CliReqInfo[key]
    gn := GlobalNotification{me,changeRole{},Notification{},accountInfoMap[value.AccountNumber],cliInfo,ackToPred{},transferStruct{},false}
    gnMar,ErrMargn := json.Marshal(gn)
    if(ErrMargn != nil){
      fmt.Println("Error",ErrMargn)
    }
    if(successor!=""){
      connSuccessor.Write(gnMar)
    }else{
      replyToClient(gn)
      fmt.Println("In virtual Tail",gn)
    }
    logMessage("Send",cliInfo)
    //fmt.Println("client Info",cliInfo)
  }
  logMessage("Status","Successfully update the ProcessedTransMap")
  logMessage("ProcessedTrans Map",processedTrans)
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

/*
  Validating the Request in the Head

*/

func (ainfo *accountInfo) validateRequest(p ParamsUpdate) string{
val,keyExists := processedTrans[p.ReqId]
if(!keyExists){
  return "new"
}else if(keyExists){
  if(val.AccountNumber != p.AccountNumber){
    return "new"
  }else if (val.Operation == p.Operation){
    fmt.Println("Same Operation do nothing")
    return "Processed"  }
  }else {
    return "InconsistentWithHistory"
  }
return ""
}

/*
this will identify the requested node
*/
func identifyTheRequestorForTcp(conn net.Conn) whoAmI{
b := make([]byte,1000)
n, err := conn.Read(b)
if(err != nil){
  fmt.Println("Error in read Identity\n")
}
fromWhere := whoAmI{}
//fmt.Println(string(b[:n]))
err1 := json.Unmarshal(b[:n],&fromWhere)
if(err1!=nil){
  fmt.Println("Error in Identity IN tcp\n",myAddress)
}
return fromWhere
}

/*
  Generic unmarshalling method

*/

func unMarshalGlobalNF(conn net.Conn) GlobalNotification{
b := make([]byte,4096)
n, err := conn.Read(b)
if(err != nil){
  fmt.Println("Error in read Identity",myAddress,successor)
}
gn := GlobalNotification{}
err1 := json.Unmarshal(b[:n],&gn)
if(err1!=nil){
  fmt.Println("Error in Global Notification\n",myAddress)
}
return gn
}

func identifyTheRequestorForUdp(conn *net.UDPConn) whoAmI{
  b := make([]byte,1000)
  n,Addr,err := conn.ReadFromUDP(b)
  if(err != nil){
    fmt.Println("Error in read Identity\n",Addr)
  }
  fromWhere := whoAmI{}
  err1 := json.Unmarshal(b[:n],&fromWhere)
  if(err1!=nil){
    fmt.Println("Error in Marshal\n")
  }
  return fromWhere
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
sentReqList = append(sentReqList,p.ReqId)
return ainfo.Balance
}

func createNewAccount(accNumber string){
  accountInfoMap[accNumber] = accountInfo{"xx",0,"Saving",accNumber}
}

func (ainfo *accountInfo) processQuery() uint64{
  return ainfo.Balance
}

/*
   requestMasterToAdd : New Server will request the master to add in the chain
*/


func requestMasterToAdd(){
  var masErr error
  fmt.Println("came inside request to Add")
  masterConn,masErr = net.Dial("tcp","localhost:8051")
  if(masErr != nil){
    fmt.Println("Error in Requesting Master",masErr)
  }
  p := PingMsg{"CitiBank",strconv.Itoa(indexInServerList),true,true}
  reqMas,errReq := json.Marshal(&p)
  if(errReq!=nil){
    fmt.Println("Error in Requesting Master",errReq)
  }
  masterConn.Write(reqMas)
  fmt.Println("Sent the Request to Master Fingers Crossed:P")
}

/*
  sendAckToPredecessor : After the tail replies to the client it has to send the ack to
                          the predecessor to delete from theirs sentList

*/

func sendAckToPredecessor(reqId string){
  conn,err := net.Dial("tcp",predecessor)
  if(err != nil){
    fmt.Println("Error Connecting the predecesor",err)
  }
  me := whoAmI{myAddress,"Successor"}
  ack := ackToPred{reqId}
  gn := GlobalNotification{me,changeRole{},Notification{},accountInfo{},clientProcessInfo{},ack,transferStruct{},false}
  ackPred,errPred := json.Marshal(gn)
  if(errPred != nil){
    fmt.Println("Error in Marshal of the Acknowledgement",errPred)
  }
  conn.Write(ackPred)
  //fmt.Println("Ack Sent to the Predecessor")
}

/* --------------------------End Of Helper Functions ------------------ */

/* --------------------------Start of Extending the Chain Functionality -------------------*/

func startTransferToNewServer(gn GlobalNotification){
  notif := gn.Notif
  if(notif.IsExtendingTheChain){
    lifetime = 0
    /* Useful when failing the tail while the transfer */
    if(indexInServerList == failedIndexNum){
      time.Sleep(1242350*time.Second)
    }
    conn,err := net.Dial("tcp",notif.NewServerAddress)
    fmt.Println("Address",notif.NewServerAddress)
    if(err!=nil){
      fmt.Println("Error In connecting to the New Server")
    }
    tempClientInfo := make(map[string]clientProcessInfo)
    ts := transferStruct{processedTrans,accountInfoMap,tempClientInfo}
    me := whoAmI{myAddress,"TAIL"}
    globalNotif := GlobalNotification{me,changeRole{},Notification{},accountInfo{},clientProcessInfo{},ackToPred{},ts,false}

    transMar,errMar := json.Marshal(globalNotif)
    if(errMar!=nil){
        fmt.Println("Marshalling Error",errMar)
    }

    /*
        For Scenario failure of New Server when Extending the Chain
    */
    if(newNodeCame){
        time.Sleep(40*time.Second)
    }
    mutex.Lock()
    if(isNewServerFailDurExt){
        fmt.Println("I am still the Tail hence Aborting the Transaction")
    }else{
      conn.Write(transMar)
      logMessage("Send",globalNotif)
      serverType = INTERNAL
      fmt.Println("came inside internal")
      successor = serverConfigs["CitiBank"][indexInServerList+1].Host + ":" + serverConfigs["CitiBank"][indexInServerList+1].Port
    }
    mutex.Unlock()
    //conn.Close()
  }
}



/* ------- Handle Connections for different Servers : Head,Tail,Internal,New Server---- */

/*
  Ack is sent backwards in the chain so the successors will remove from theirs sentList
*/

func handleConnectionFromSucc(conn net.Conn,gn GlobalNotification){
   fmt.Println("Length before deletion",len(sentReqList),myAddress)
   logMessage("Length of the sent list before ",len(sentReqList))
   var temp int
   for i:=0;i<len(sentReqList);i++ {
     if(sentReqList[i] == gn.Ack.ReqId){
       temp = i
       break
     }
   }
   fmt.Println("Sent list",len(sentReqList),myAddress,temp)
     if(temp >= 0 && len(sentReqList) > 0){
       sentReqList = append(sentReqList[:temp],sentReqList[temp+1:]...)
     }
   logMessage("Length of the sent list After",len(sentReqList))
   fmt.Println("Length before deletion",len(sentReqList),myAddress)
}


func handleConnectionFromMaster(conn net.Conn,gn GlobalNotification){
  //fmt.Println("came inside master",gn)
  if(gn.RoleChange.NewRole == "HEAD"){
    fmt.Println("current Head\n")
    logMessage("Status","Became the current head")
    serverType = HEAD
    headUdpAddr  = serverConfigs["CitiBank"][indexInServerList].Host + ":" + serverConfigs["CitiBank"][indexInServerList].UDPPort
    go startHeadUdpServer()

    }else if (gn.RoleChange.NewRole == "TAIL"){
    fmt.Println("current Tail\n")
    logMessage("Status","Became the Current Tail")
    serverType = TAIL
    tailUdpAddress  = serverConfigs["CitiBank"][indexInServerList].Host + ":" + serverConfigs["CitiBank"][indexInServerList].UDPPort
    successor=""
    fmt.Println("came into failure\n")
    logMessage("Status","Changed to Tail")
    go startTailUdpServer()
    }else if (gn.RoleChange.NewRole == "ExtChain"){
    fmt.Println("extending the chain\n")
    logMessage("Status","Extending the chain")
    startTransferToNewServer(gn)

    }else if (gn.RoleChange.NewRole == "RemainAsTail"){
    fmt.Println("New Server Failed")
    mutex.Lock()
    isNewServerFailDurExt = true
    mutex.Unlock()
    fmt.Println("New Server Failed Value",isNewServerFailDurExt,myAddress)

    }else if (gn.RoleChange.NewRole == "NewSuccInternal"){
      fmt.Println("came into internal failure Succ")
      logMessage("status","Internal failure")
      mutex.Lock()
      haltTheSystem = true
      mutex.Unlock()
      sendSendSeq(conn,gn)

    }else if (gn.RoleChange.NewRole == "NewPredInternal"){
      fmt.Println("came into internal failure Pred")
      logMessage("status","Internal failure")
      mutex.Lock()
      haltTheSystem = true
      mutex.Unlock()
      receiveSeqNum(conn,gn)
    }else if(gn.Notif.FailureType == "TailFailed"){
      fmt.Println("resending the request\n")
      logMessage("Status","Resending the request")
      requestMasterToAdd()
    }
}

/*

  handleNewServerConnection : New server connection and this will be needed until
                              the extending the chain is in progress

*/


func handleNewServerConnection(gn GlobalNotification){
    //fmt.Println("Connection Established with Tail")
    processedTrans=gn.TransHist.Trans
    accountInfoMap = gn.TransHist.AccMap
    fmt.Printf("Assigned Lots of Tension : %+v",gn.TransHist)
    fmt.Println("Started the ping Server")
    logMessage("Status","Received the history from the Tail")
    logMessage("Received",gn.TransHist)
    serverType = TAIL
    fmt.Println("Started the ping Server")
    // inform master my new role
    chr := changeRole{"Tail"}
    newR,err := json.Marshal(chr)
    if(err!=nil){
      fmt.Println("Error in replying to the Master")
    }
    masterConn.Write(newR)
    tailUdpAddress = serverConfigs["CitiBank"][indexInServerList].Host + ":" + serverConfigs["CitiBank"][indexInServerList].UDPPort
    go startTailUdpServer()
}

/* ------------------- End Of Extending the Chain Functionality ---------------- */

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
    IdentityLength,readIdentityErr := conn.Read(IdentifyTheClient)
    fmt.Println("came Into Head")
    if(readIdentityErr!=nil){
      fmt.Println("Error In reading the identity\n",readIdentityErr)
    }
    fmt.Println("Head Connection\n")
    unMarshalClientIdentityErr := json.Unmarshal(IdentifyTheClient[:IdentityLength],&fromWhere)
    if(unMarshalClientIdentityErr!=nil){
      fmt.Println("unMarshalClientIdentity Error\n",unMarshalClientIdentityErr)
    }
    clientRequestBytes := make([]byte,1000)
    //fmt.Println("came")
    length,readErr := conn.Read(clientRequestBytes)
    if(readErr != nil){
      fmt.Println("Read Error from the client connection\n")
    }
    unmarshalClientRequestErr := json.Unmarshal(clientRequestBytes[:length], &p)
    logMessage("Status","Receiving the Client Request")
    logMessage("Received",p)
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
      fmt.Println("Balance In Head",balance,outcome)
      accountInfoMap[p.AccountNumber] = ainfo
      fmt.Println("ainfo in Head",ainfo)
      if(outcome == ""){
        outcome = "Processed"
      }
    }else if(transInfo == "Processed"){
      outcome = "Processed"
    }else {
      outcome = "Inconsistent With History"
    }
    logMessage("Updated the balance",accountInfoMap)
    logMessage("Status","Connecting to the succesor\n")
    sentReqList = append(sentReqList,p.ReqId)
    connSuccessor,errSucc := net.Dial("tcp",successor)
    if errSucc != nil{
      fmt.Println("Error connecting to the successor\n")
    }

    me := whoAmI{fromWhere.MySocketAddress,"predecessor"}
    cliProcessInfo := clientProcessInfo{fromWhere.MySocketAddress,p.ReqId,outcome,p.Operation}
    ch := changeRole{}
    notif := Notification{}
    globalNotif := GlobalNotification{me,ch,notif,ainfo,cliProcessInfo,ackToPred{},transferStruct{},false}
    //fmt.Println("In Valid",globalNotif)
    accCliInfo,ErrMar := json.Marshal(globalNotif)
    if(ErrMar != nil){
      fmt.Println("Error while marshalling",ErrMar)
    }
    connSuccessor.Write(accCliInfo)
    outcome = ""
    fmt.Println("Written to successor from Head\n")
    connSuccessor.Close()
    logMessage("Send",globalNotif)
}
}

func handleInternalConnection(p ParamsUpdate,conn net.Conn,accountInfoMap map[string]accountInfo,gn GlobalNotification){
  if(gn.ClientInfo.Outcome == "Processed"){
      updateProcessTrans(gn)
  }

  /*
      For Internal Server Failures
  */
  if(indexInServerList == failedIndexNum && mySecondRequest){
      fmt.Println("Wow Came inside this",myAddress)
      time.Sleep(time.Second*453634645)
  }


  //fmt.Println("Internal Connection",myAddress,indexInServerList,successor)
  connSuccessor,errSucc := net.Dial("tcp",successor)
  logMessage("Status","Connecting to the Succesor :"+successor)
  if errSucc != nil{
    fmt.Println("Error connecting to the successor",myAddress)
  }
  clientInfo[gn.ClientInfo.ReqId] = gn.ClientInfo
  accCliInfo,ErrMar := json.Marshal(gn)
  if(ErrMar != nil){
    fmt.Println("Error While Marshalling",ErrMar)
  }
  connSuccessor.Write(accCliInfo)
  logMessage("Status","Received in the Internal Server")
  logMessage("Send",accCliInfo)
  mySecondRequest = true
}

func updateProcessTrans(gn GlobalNotification){
  ainfo := gn.AccInfo
  accountInfoMap[ainfo.AccNumber] = ainfo
  processedTrans[gn.ClientInfo.ReqId] = Hist{ainfo.AccNumber,ainfo.Balance,gn.ClientInfo.Operation}
  sentReqList = append(sentReqList,gn.ClientInfo.ReqId)
  logMessage("Updated the history",processedTrans[gn.ClientInfo.ReqId])
  logMessage("Status","Inserted the New Request in the list")
}




/*
handleConnectionForUpdate : This function will handle the update requests from the predecessors.
                            and reply to the client
*/
func handleConnectionTailForUpdate(conn net.Conn,gn GlobalNotification){

  if(gn.ClientInfo.Outcome == "Processed"){
    updateProcessTrans(gn)
  }
  replyToClient(gn)
  /* Send Ack to the predecessors */
  sendAckToPredecessor(gn.ClientInfo.ReqId)
  fmt.Println("Sending the Ack to the Predecessor")
  logMessage("Status","Sending the Ack for the clients")
  logMessage("Send",gn.ClientInfo.ReqId)
  fmt.Println("Written to client Update\n")
}

/*--------------------- END OF Updates Processing In the chain ------------------ */

/*--------------------- Start OF Quering Proceesing Processing in the tail ------------------ */


func handleConnectionTailForQuery(p Params,conn *net.UDPConn,accountInfoMap map[string]accountInfo){
  set := make(map[string]bool)
  for{
  requestBytes := make([]byte,1000)
  length,Addr,errInReading := conn.ReadFromUDP(requestBytes)
  fmt.Println("status","Processing the Query by the Tail")
  if(errInReading != nil){
      fmt.Println("Error in reading the request from the client\n")
  }
  //fmt.Println("Bytes",string(requestBytes),Addr)
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
  fmt.Println("Balance in Tail",balance)
  logMessage("Processed the Query",accountInfoMap)
  //fmt.Println("P reqId",p.ReqId)
  rm := replyMsg{}
  rm.ReqId = p.ReqId
  rm.Outcome = "Processed"
  rm.Balance = ainfo.Balance
  rm.AccountNumber = ainfo.AccNumber
  ack,err2 := json.Marshal(rm)
  if(err2 !=nil){}
  ra, err := net.ResolveUDPAddr("udp", Addr.String())
  _, isExists := set[p.ReqId];
  if !isExists{
  set[p.ReqId] = true
  fmt.Println("ReqId",p.ReqId)
  _,err2 := conn.WriteToUDP(ack,ra)
  logMessage("Send",rm)
  logMessage("Status",conn.LocalAddr())
  //fmt.Printf("Written to client %d %+v\n",n,rm)
  if(err2!=nil || err!=nil){
    fmt.Println("Error in writing to the client",err,err2)
  }
  }else {
    fmt.Println("duplicate request")
  }
  }
}

func logMessage(msgType string,msg interface{}){
  temp := 0
  configData := serverConfigs["CitiBank"][indexInServerList]
  logFileName:= configData.ServerType + configData.BankName + configData.ServerId + ".log"
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

/* ----------------- Start Servers of the chain --------------------*/

func startHeadTcpServer(){
lnHead,errHead := net.Listen("tcp",myAddress)
if(errHead != nil){
  fmt.Println("Error in the Head Server\n")
  return;
}

for{
  conn, errAcceptHead := lnHead.Accept()
  if errAcceptHead!=nil {
    fmt.Println(conn)
  }
  globalNotif := unMarshalGlobalNF(conn)
  if (globalNotif.FromWhere.Identity == "Successor"){
      go handleConnectionFromSucc(conn,globalNotif)
  }
}
}


func startHeadUdpServer(){
  fmt.Println("Start Udp a Server  ",myAddress)
  sAddr, err4 := net.ResolveUDPAddr("udp", headUdpAddr)
  var p ParamsUpdate
  conn,errAcceptTail := net.ListenUDP("udp",sAddr)
  //fmt.Println("inside the connection\n")
  if(errAcceptTail!=nil || err4!=nil) {
      fmt.Println("Error in UDP Connection\n",errAcceptTail,err4)
  }
  go handleHeadUdpConnection(p,conn,accountInfoMap)
}

func startTcpServer(){
  fmt.Println("Start a Server  ",myAddress)
  lnTail,errTail := net.Listen("tcp",myAddress)
  //fmt.Println("My Address",myAddress)
  if(errTail != nil){
    fmt.Println("Error in connection",errTail,myAddress)
      return;
  }
  if(serverType == NEWNODE){
    fmt.Println("I am New Node")
    logMessage("Status","Started a New Server")
    go requestMasterToAdd()
  }
  for {
      conn, errAcceptTail := lnTail.Accept()
      if(errAcceptTail!=nil) {
        fmt.Println("Error in Connection\n")
      }
      var p ParamsUpdate
      globalNotif := unMarshalGlobalNF(conn)
      if(globalNotif.FromWhere.Identity == "Master"){
          go handleConnectionFromMaster(conn,globalNotif)
      }else if (globalNotif.FromWhere.Identity == "Successor"){
          go handleConnectionFromSucc(conn,globalNotif)
      }else if (globalNotif.FromWhere.Identity == "NewPredecessor"){
          go receiveUpdateProcTranFromNewPred(conn,globalNotif)
      }else if (serverType == TAIL){
          go handleConnectionTailForUpdate(conn,globalNotif)
      }else if(globalNotif.FromWhere.Identity == "predecessor"){
          go handleInternalConnection(p,conn,accountInfoMap,globalNotif)
      }else if(serverType == NEWNODE){
          fmt.Println("came into New Server")
          go handleNewServerConnection(globalNotif)
      }
    }
}

func startTailUdpServer(){
  fmt.Println("Start Tail Udp a Server ",myAddress)
  logMessage("Status","Started the Tail Udp Server")
  var p Params
  fmt.Println("tail Udp Address",tailUdpAddress)
  sAddr, err4 := net.ResolveUDPAddr("udp",tailUdpAddress)
  conn,errAcceptTail := net.ListenUDP("udp",sAddr)
  //fmt.Println("inside the connection\n")
  if(errAcceptTail!=nil || err4!=nil) {
      fmt.Println("Error in UDP Connection\n")
  }
  handleConnectionTailForQuery(p,conn,accountInfoMap)
}

/* ----------------- End of Start Servers of the chain --------------------*/


func assignDesignationToServer(){
  var err error
  indexInServerList,err = strconv.Atoi(os.Args[1])
  if(err!=nil){
    fmt.Println("str conv error",err)
  }
  myAddress=serverConfigs["CitiBank"][indexInServerList].Host + ":" + serverConfigs["CitiBank"][indexInServerList].Port
  if(serverConfigs["CitiBank"][indexInServerList].ServerType=="HEAD"){
    serverType=HEAD
    headServerId = serverConfigs["CitiBank"][indexInServerList].ServerId
    predecessor=""
    headUdpAddr = serverConfigs["CitiBank"][indexInServerList].Host + ":" + serverConfigs["CitiBank"][indexInServerList].UDPPort
    successor=serverConfigs["CitiBank"][indexInServerList+1].Host + ":" + serverConfigs["CitiBank"][indexInServerList+1].Port
  } else if (serverConfigs["CitiBank"][indexInServerList].ServerType=="INTERNAL"){
    serverType=INTERNAL
    //fmt.Println("index",indexInServerList)
    predecessor=serverConfigs["CitiBank"][indexInServerList-1].Host + ":" + serverConfigs["CitiBank"][indexInServerList-1].Port
    successor=serverConfigs["CitiBank"][indexInServerList+1].Host + ":" + serverConfigs["CitiBank"][indexInServerList+1].Port
  } else if (serverConfigs["CitiBank"][indexInServerList].ServerType== "TAIL") {
    serverType=TAIL
    tailUdpAddress = serverConfigs["CitiBank"][indexInServerList].Host + ":" + serverConfigs["CitiBank"][indexInServerList].UDPPort
    predecessor=serverConfigs["CitiBank"][indexInServerList-1].Host + ":" + serverConfigs["CitiBank"][indexInServerList-1].Port
  }else{
    serverType=NEWNODE
    predecessor=serverConfigs["CitiBank"][indexInServerList-1].Host + ":" + serverConfigs["CitiBank"][indexInServerList-1].Port
  }
}

func readClientConfig(){
  f, fileOpenError := os.OpenFile("../../config/clientConfig.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  err := json.NewDecoder(f).Decode(&clientConfigs)
  if(fileOpenError!=nil || err!=nil){
    fmt.Println("file open error",fileOpenError,err)
  }
}

// func readServerConfig(){
//   f, fileOpenError := os.OpenFile("../../config/Bank1Servers.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
//   err := json.NewDecoder(f).Decode(&serverConfigs)
//   if(fileOpenError!=nil || err!=nil){
//     fmt.Println("file open error",fileOpenError,err)
//   }
// }

func readServerConfig(){
  f, fileOpenError := os.OpenFile("../../config/Bank1Servers.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  err := json.NewDecoder(f).Decode(&serverConfigs)
  if(fileOpenError!=nil || err!=nil){
    fmt.Println("file open error",fileOpenError,err)
  }
}



// func readConfigFile(fileName string) interface{}{
//   var configStruct interface{}
//   f, fileOpenError := os.OpenFile("../../config/"+ fileName +".json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
//   err := json.NewDecoder(f).Decode(&configStruct)
//   fmt.Println("Config Struct",configStruct)
//   if(fileOpenError!=nil || err!=nil){
//     fmt.Println("file open error",fileOpenError,err)
//   }
//   return configStruct
// }

func readFailureConfigFile(){
  f, fileOpenError := os.OpenFile("../../config/FailureConfigFile.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  err := json.NewDecoder(f).Decode(&failureConfigsMap)
  //fmt.Println("Failure Map",failureConfigsMap)
  if(fileOpenError!=nil || err!=nil){
    fmt.Println("file open error",fileOpenError,err)
  }
}


func runTimer(){
  for ;; {
      time.Sleep(3 * time.Second)
      timeout <- true
  }
}

func pingMaster(){
  for ;; {

    if(len(failureConfigsMap) > 0){
      for i:=0;i<len(failureConfigsMap["CitiBank"]);i++ {
        indexNum,_  := strconv.Atoi(failureConfigsMap["CitiBank"][i].Index)
        seqNum , _ := strconv.Atoi(failureConfigsMap["CitiBank"][i].SendSeqNum)
        if(failureConfigsMap["CitiBank"][i].FailureType == "HEAD"){
          if(sendSeq > seqNum  && serverType == HEAD){
              return
          }
        }else if(failureConfigsMap["CitiBank"][i].FailureType == "TAIL" && indexInServerList == indexNum){
          if(sendSeq > seqNum  && serverType == TAIL){
              failedIndexNum=indexNum
              return
          }
        }else if(failureConfigsMap["CitiBank"][i].FailureType == "TailDelay" && indexInServerList == indexNum){
            newNodeCame = true
        }else if(failureConfigsMap["CitiBank"][i].FailureType == "INTERNAL"){
            if(sendSeq > seqNum && serverType == INTERNAL && indexInServerList == indexNum){
              failedIndexNum = indexNum
              shouldIDie = true
              return
            }
        }else if(failureConfigsMap["CitiBank"][i].FailureType == "NEWNODE"){
            if(sendSeq > seqNum && serverType == NEWNODE){
              shouldIDie = true
              return
          }
        }
      }
    }

  /* ---------------- For Failures have to read from the config file ------------ */

  // // if(sendSeq > 3 && serverType == TAIL){
  // //           break
  // // }
  // // if (serverType == INTERNAL && indexInServerList == 3 && sendSeq > 1 ){
  // //       break
  // // }
  // // if (serverType == INTERNAL && indexInServerList == 4  && sendSeq > 8 ){
  // //   break
  // // }
  // // if(serverType == TAIL){
  // //   fmt.Println("New Server",isNewSerGoingOn)
  // // }
  //
  //   if(sendSeq > 5 && serverType == TAIL && indexInServerList == 4){
  //             break
  //   }
  //
  //
  // // if(serverType == NEWNODE && sendSeq > 1){
  // //   break
  // // }
  /* ----------------------- End of the failure Configuration ---------------*/
  conn,Err := net.Dial("tcp","localhost:8051")
  if(Err != nil){
    fmt.Println("Error in Connecting to Master",Err)
  }
  p := PingMsg{"CitiBank",strconv.Itoa(indexInServerList),true,false}
  pingMars,errMars := json.Marshal(p)
  if(errMars != nil){
    fmt.Println("Error In Marshalling\n")
  }
  select{
    case <-timeout:
      conn.Write(pingMars)
      logMessage("Send","PingedMaster")
    }
    conn.Close()
  }
}

func startServers(){
  switch(serverType){
    case HEAD:
                  go startHeadUdpServer()
                  go startTcpServer()
    case INTERNAL:
                  go startTcpServer()
    case TAIL:
                  go startTailUdpServer()
                  go startTcpServer()
    case NEWNODE:
                  go startTcpServer()
  }
}

// func readConfigFiles(){
//   var x interface{}
//     var ok bool
//   x = readConfigFile("Bank1Servers")
//   serverConfigs, ok = x.(map[string][]service)
//   if(!ok){
//     fmt.Println("Error in Reading config file ")
//   }
//   x = readConfigFile("clientConfig")
//   clientConfigs ,ok = x.(map[string][]clientService)
//   if(!ok){
//     fmt.Println("Error in Reading config file ")
//   }
//   x = readConfigFile("FailureHeadConfigFile")
//   failureConfigsMap, ok = x.(map[string][]FailureConfigs)
//   fmt.Println("in read config file",serverConfigs)
//   if(!ok){
//     fmt.Println("Error in Reading config file ")
//   }
// }

func main(){
  readServerConfig()
  readClientConfig()
  //readConfigFiles()
  readFailureConfigFile()
  //readTailFailureConfigFile()
  assignDesignationToServer()
  initializeAccountDetails()
  if(serverType == NEWNODE){
    time.Sleep(10*time.Second)
  }
  go runTimer()
  go pingMaster()
  startServers()
  <-chn
}
