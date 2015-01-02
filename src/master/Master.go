/* Master Server */

package main

import(
"fmt"
"net"
"encoding/json"
"os"
"time"
)

type clientProcessInfo struct{
ClientSocketAddress string
ReqId string
Outcome string
Operation string
}

type whoAmI struct{
MySocketAddress string
Identity string
}

type accountInfo struct{
CustName string
Balance uint64
AccountType string
AccNumber string
}

var newUdpHead string
var newServerUdpRole string
var newUdpTail string
var isNewSerGoingOn bool


type Notification struct{
  FailureType string
  NewServerAddress string
  IsExtendingTheChain bool
  SendSeq int
}

type Hist struct{
  AccountNumber string
  Balance uint64
  Operation string
}

type GlobalNotification struct{
  FromWhere whoAmI
  RoleChange changeRole
  Notif Notification
  AccInfo accountInfo
  ClientInfo clientProcessInfo
  Ack ackToPred
  TransHist transferStruct
  IsNewSerGoingOn bool
}

type transferStruct struct{
  Trans map[string]Hist
  AccMap map[string]accountInfo
  CliReqInfo map[string]clientProcessInfo
}

type PingMsg struct{
  BankName string
  ServerId string
  IsAlive bool
  IsNewServer bool
}

var serverType int

type ackToPred struct{
  ReqId string
}

type changeRole struct{
    NewRole string
}

var newServerProcess bool
var newNodeAddr string

var myAddress = "localhost:8051"

type ServersStatus struct{
  ServerId string
  IsAlive bool
  PingCount int
}

var indexInServerList int

type service struct{
ServerId string
Host string
Port string
UDPPort string
ServerType string
BankName string
}

type clientService struct{
ClientId string
ClientHost string
ClientPort string
ClientSendPort string
BankName string
}

var newNodeUdpPort string

const (
     MASTER = 0
     HEAD = 1
     INTERNAL = 2
     TAIL = 3
  )

var serverStats = make(map[string][]ServersStatus, 0)
var serverConfigs = make(map[string][]service, 0)
var clientConfigs = make(map[string][]clientService, 0)
var serStats = make([]ServersStatus,0)

func intializeServerMap(BankName string){
  for i:=0;i<len(serverConfigs[BankName]);i++ {
    if(serverConfigs[BankName][i].ServerType != "NEWNODE"){
      serStats = append(serStats,ServersStatus{serverConfigs[BankName][i].ServerId,false,1})
    }
  }
  serverStats[BankName] = serStats
  fmt.Println("Length of the stats",len(serStats))
}


func processNewServerRequest(BankName string,masConn net.Conn){
  var addr string
  var index int
  var tailId int
  for i:=0;i<len(serverConfigs[BankName]);i++ {
    if(serverConfigs[BankName][i].ServerType == "TAIL"){
      addr = serverConfigs[BankName][i].Host + ":" + serverConfigs[BankName][i].Port
      tailId =i
    }else if (serverConfigs[BankName][i].ServerType == "NEWNODE"){
      newNodeAddr = serverConfigs[BankName][i].Host + ":" + serverConfigs[BankName][i].Port
      newNodeUdpPort = serverConfigs[BankName][i].Host + ":" + serverConfigs[BankName][i].UDPPort
      index =i
      if(len(serverStats[BankName]) != len(serverConfigs[BankName])){
        serverStats[BankName] = append(serverStats[BankName],ServersStatus{serverConfigs[BankName][i].ServerId,false,1})
      }
    }
  }
  conn,errTail := net.Dial("tcp",addr)
  if(errTail!=nil){
    fmt.Println("error In connecting to Tail",errTail)
  }
  fromWhere := whoAmI{"localhost:8051","Master"}
  ch := changeRole{"ExtChain"}
  notif := Notification{"Extending the Chain",newNodeAddr,true,0}
  gn := GlobalNotification{fromWhere,ch,notif,accountInfo{},clientProcessInfo{},ackToPred{},transferStruct{},false}
  marGn,ErrMarGn := json.Marshal(gn)
  if(ErrMarGn != nil){
    fmt.Println("Error While UnMarshalling",ErrMarGn)
  }
  conn.Write(marGn)
  var leng int
  var Err error
  fmt.Println("Written to Old Tail")
  newSerRep := make([]byte,1024)
  leng,Err = masConn.Read(newSerRep)
  fmt.Println("Writt Tail")
  if(Err!=nil){
      fmt.Println("Error in New Server Reply",Err)
  }
  if(leng > 0){
    newRole := "Extending The Chain"
    serverConfigs[BankName][tailId].ServerType = "INTERNAL"
    serverConfigs[BankName][index].ServerType = "TAIL"
    fmt.Println("came inside change of roles")
    informClients(newNodeUdpPort,newRole)
  }else{
    fmt.Println("Opps new server/Present Tail has failed")
  }
}

func handleConnection(conn net.Conn,p PingMsg){
  isNewSerGoingOn = false
  //fmt.Println("Cammmmme\n")
  msg := make([]byte,1000)
  n, err := conn.Read(msg)
  if(err != nil){
    fmt.Println("Error In reading Account Info",err)
  }
  pingError := json.Unmarshal(msg[:n], &p)
  if(pingError != nil){
    fmt.Println("Unmarshalling Error in receiving Ping from Server\n")
  }

  if(p.IsNewServer){
      newServerProcess = true
      processNewServerRequest(p.BankName,conn)
  }else{
    for i:=0;i<len(serverStats[p.BankName]);i++ {
    if(serverStats[p.BankName][i].ServerId == p.ServerId){
      serverStats[p.BankName][i].IsAlive = true
      fmt.Println("Server Id",p.ServerId)
      serverStats[p.BankName][i].PingCount = serverStats[p.BankName][i].PingCount + 1
    }
  }
  }
}

func startMasterServer(){
  fmt.Println("starting the Master Server")
  //fmt.Println("fsd",myAddress)
  lnMaster,errMaster := net.Listen("tcp",myAddress)
  if(errMaster != nil){
    fmt.Println("Error in the Head Server\n")
    return;
  }

  var pMsg = PingMsg{}

  for{
    conn, errAcceptMaster := lnMaster.Accept()
    if errAcceptMaster!=nil {
      fmt.Println(conn)
    }
    go handleConnection(conn,pMsg)
  }
}

func assignDesignationToServer(){
  serverType=MASTER
}

func readClientConfig(){
  f, fileOpenError := os.OpenFile("../../config/clientConfig.json", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  err := json.NewDecoder(f).Decode(&clientConfigs)
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


 func notifyMe(conn net.Conn){
//   fromWhere := whoAmI{"localhost:8051","Master"}
//   identity,errInSendingIdentity := json.Marshal(fromWhere)
//   if(errInSendingIdentity != nil){
//         fmt.Println("Error in Sending Identity",errInSendingIdentity)
//   }
//   // conn.Write(identity)
 }

var timeout = make(chan bool, 0)
var chn = make(chan int)
var failedServers []string


func handleFailureForHTN(BankName string, index int,newRole string){
  newServerRole := serverConfigs[BankName][index].Host + ":" + serverConfigs[BankName][index].Port
  newServerUdpRole = serverConfigs[BankName][index].Host + ":" + serverConfigs[BankName][index].UDPPort
  serverConfigs[BankName][index].ServerType = newRole
  conn,err := net.Dial("tcp",newServerRole)
  if(err != nil){
        fmt.Println("Error in Failure",err)
  }
  ch := changeRole{newRole}
  me := whoAmI{"localhost:8051","Master"}
  gn := GlobalNotification{me,ch,Notification{},accountInfo{},clientProcessInfo{},ackToPred{},transferStruct{},false}
  gnServer,errServer := json.Marshal(gn)
  if(errServer != nil){
    fmt.Println("Error in Marshal of the Acknowledgement",errServer)
  }
  conn.Write(gnServer)
  fmt.Println("New Server Process",newServerProcess)
  if(newRole == "TAIL" && newServerProcess){
      resendTheRequest()
  }
}

func resendTheRequest(){
  newNodeConnection,err := net.Dial("tcp",newNodeAddr)
  fmt.Println("Came inside resend",newNodeAddr)
  if(err!=nil){
    fmt.Println("New Server connection :",err)
  }
  notifNewNode := Notification{"TailFailed","",false,0}
  fromWhere := whoAmI{"localhost:8051","Master"}
  gn := GlobalNotification{fromWhere,changeRole{},notifNewNode,accountInfo{},clientProcessInfo{},ackToPred{},transferStruct{},false}
  gnMar, marErr := json.Marshal(gn)
  if(marErr != nil){
    fmt.Println("Error in Marshalling",marErr)
  }
  newNodeConnection.Write(gnMar)
  fmt.Println("written to new node")

}

func readNotification(masConn net.Conn) Notification{
  notif:= make([]byte,1024)
  leng,errNotif := masConn.Read(notif)
  if(errNotif != nil){
    fmt.Println("Error in reading the notification",errNotif)
  }
  var notifReqSeq Notification
  unMarNotfErr := json.Unmarshal(notif[:leng],&notifReqSeq)
  if(unMarNotfErr != nil){
    fmt.Println("Unmarshalling Error",unMarNotfErr)
  }
  return notifReqSeq
}

func handleFailureForInternal(BankName string, predIndex int,succIndex int,newRole string){
  newSucc := serverConfigs[BankName][succIndex].Host + ":" + serverConfigs[BankName][succIndex].Port
  newPred := serverConfigs[BankName][predIndex].Host + ":" + serverConfigs[BankName][predIndex].Port
  fmt.Println("Addresss",newSucc,newPred)
  succConn,errSucc := net.Dial("tcp",newSucc)
  if(errSucc != nil){
    fmt.Println("Error in Connecting to the Successor in Master",errSucc)
  }
  ch := changeRole{"NewSuccInternal"}
  fmt.Println("New Role",newRole,succIndex)
  fmt.Println("Sending to succ New Pred")
  notifSucc := Notification{"Internal",newPred,false,0}
  fromWhere := whoAmI{"localhost:8051","Master"}
  fn := GlobalNotification{fromWhere,ch,notifSucc,accountInfo{},clientProcessInfo{},ackToPred{},transferStruct{},false}
  fnMar,ErrMar := json.Marshal(fn)
  if(ErrMar != nil){
    fmt.Println("Marshalling Error\n",ErrMar)
  }
  succConn.Write(fnMar)
  fmt.Println("Written to Successor")
  notifFromSucc := readNotification(succConn)
  fmt.Println("fnMar came here")
  predConn,errPred := net.Dial("tcp",newPred)
  if(errPred!=nil){
    fmt.Println("Error Connecting to the Predecessor",errPred)
  }

  ch2 := changeRole{"NewPredInternal"}
  fmt.Println("New Role Pred",newRole,predIndex)
  notif := Notification{"Internal",newSucc,false,notifFromSucc.SendSeq}
  gn := GlobalNotification{fromWhere,ch2,notif,accountInfo{},clientProcessInfo{},ackToPred{},transferStruct{},false}
  gnMar,ErrMargn := json.Marshal(gn)
  if(ErrMargn != nil){
    fmt.Println("Error",ErrMargn)
  }
  predConn.Write(gnMar)
}


func handleFailure(BankName string,serverId string, k int){
  var predIndex,succIndex,index,tailId int
  var newRole string
  for i:=0;i<len(serverStats[BankName]);i++ {
    if(serverConfigs[BankName][i].ServerId == serverId){
      if(serverConfigs[BankName][i].ServerType == "HEAD"){
          index = i+1
          newRole = "HEAD"
          serverConfigs[BankName][i].ServerType = "DEAD"
          serverConfigs[BankName][i+1].ServerType = "HEAD"
          handleFailureForHTN(BankName,index,newRole)
        } else if(serverConfigs[BankName][i].ServerType == "TAIL"){
          tailId = i
          index = i-1
          newRole = "TAIL"
          serverConfigs[BankName][i].ServerType = "DEAD"
          serverConfigs[BankName][i-1].ServerType = "TAIL"
          handleFailureForHTN(BankName,index,newRole)
        } else if (serverConfigs[BankName][i].ServerType == "NEWNODE"){
          // hardcoded have to change
          if(tailId != 0){
            index = tailId-1
          }else {
            index = 4
          }
          newRole="RemainAsTail"
          go handleFailureForHTN(BankName,index,newRole)
        }else if (serverConfigs[BankName][i].ServerType == "INTERNAL"){
          serverConfigs[BankName][i].ServerType = "DEAD"
          if(serverStats[BankName][i-1].PingCount!=0){
              predIndex = i-1
          }else{
              predIndex = i-2
          }
          if(serverStats[BankName][i+1].PingCount!=0){
              succIndex = i+1
          }else{
              succIndex = i+2
          }
          newRole="Internal"
          fmt.Println("Index",predIndex,succIndex)
          go handleFailureForInternal(BankName,predIndex,succIndex,newRole)
        }
        serverStats[BankName][i].PingCount = 0;
        }
      }
    if(newRole!="RemainAsTail" && newRole != "Internal"){
      informClients(newServerUdpRole,newRole)
    }
  }


func informClients(newSerAdd string,newRole string){
  conn,err := net.Dial("tcp","localhost:10006")
  fmt.Println("came inside inform clients\n")
  if(err!=nil){
    fmt.Println("Error in Notifying",err)
  }
  var fn Notification
  fn.FailureType = newRole
  fmt.Println("New Server Address",newSerAdd)
  fn.NewServerAddress = newSerAdd
  fnClient,errInNotif := json.Marshal(&fn)
  if(errInNotif!=nil){
    fmt.Println("Error in Marshalling\n")
  }
  conn.Write(fnClient)
  fmt.Println("written to clients\n")
}


var readAll = make(chan bool,0)

func runTimer(){
  for ;; {
    time.Sleep(10 * time.Second)
    timeout <- true
  }
}

func checkChainsCondition(){
  for ;; {
    select{
      case <-timeout:
        readServerStatus("CitiBank")
  }
}
}

func readServerStatus(BankName string){
  i := 0
  for i=0;i<len(serverStats[BankName]);i++ {
    if(!(serverStats[BankName][i].PingCount == 0) && !serverStats[BankName][i].IsAlive){
      fmt.Println("here I shouldn't",i,serverStats[BankName][i].PingCount)
      handleFailure(BankName,serverStats[BankName][i].ServerId,i)
    }else if(!(serverStats[BankName][i].PingCount == 0)){
      serverStats[BankName][i].IsAlive = false
    }
}
}

func main(){
  readServerConfig()
  readClientConfig()
  assignDesignationToServer()
  //fmt.Println("ds",time.Second)
  intializeServerMap("CitiBank")
  go startMasterServer()
  go runTimer()
  go checkChainsCondition()
  <-chn
}
