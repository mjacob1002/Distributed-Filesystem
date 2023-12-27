package membership;

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"net"
	"math/rand"
	pb "github.com/mjacob1002/Distributed-Filesystem/pkg/gen_proto"
    "google.golang.org/protobuf/proto"
	"sync"
)

var ThisMachineId string;
var MyNodeNumber int64;
var MembershipPort string;
var ThisHostname string;
var MyIntroducer string;
var MembershipLock sync.Mutex;
var MembershipList map[string]pb.TableEntry = make(map[string]pb.TableEntry) // maps machineId to table entry
var addressCache map[string]*net.UDPAddr = make(map[string]*net.UDPAddr); // used in order to cache DNS translations
var deadMemberSet map[string]int = make(map[string]int)


// constants
var MAX_UDP_SIZE int64 = 64496;
var T_FAIL float64 = 5.0; // in seconds
var NUM_HEARTBEAT int64 = 4


func getUDPAddr(address string) (*net.UDPAddr, error){
	conn, ok := addressCache[address];
	if ok {
		return conn, nil;
	}
	// do the whole resolution thingy
	udpAddr, err := net.ResolveUDPAddr("udp", address);
	if err != nil {
		fmt.Printf("error resolving the udp address. err=%v\n", err);
		return nil, err;
	}
	addressCache[address] = udpAddr;
	return udpAddr, nil;
}
// assume the lock is already acquired
func IncrementHeartbeat(){
	myEntry, ok := MembershipList[ThisMachineId];
	if !ok {
		fmt.Printf("this is weird; I don't have my own table entry for myself...\n");
	}
	myEntry.HeartbeatCounter++;
	myEntry.LocalTime = time.Now().Unix()
	MembershipList[ThisMachineId] = myEntry;
}

// assume the lock is already acquired
func MakeHeartbeat()(pb.HeartbeatMessage){
	heartbeat := pb.HeartbeatMessage{};
	// increment my own heartbeat
	tableEntries := make([]*pb.TableEntry, 0);
	for _, entry := range(MembershipList){
		copiedEntry := entry;
		tableEntries = append(tableEntries, &copiedEntry);
	}
	heartbeat.Entries = tableEntries
	heartbeat.Hostname = ThisHostname
	heartbeat.Port = MembershipPort
	return heartbeat;
}

func SendHeartbeat(address string){
	IncrementHeartbeat();
	// get the heartbeat message
	myHeartbeat := MakeHeartbeat()
	// get the udp address
	udpAddr, err := getUDPAddr(address);
	if err != nil {
		fmt.Printf("Oopsie, error checking udp cache. %v\n", err);
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
    if err != nil {
        fmt.Printf(fmt.Errorf("net.DialUDP: %v\n", err).Error())
        return
    }
    defer conn.Close()
	// send the addy to everyone else
	serialHeartbeat, err := proto.Marshal(&myHeartbeat);
	if err != nil {
		fmt.Printf("proto.Marshal: %v\n", err);
	}
	_, err = conn.Write(serialHeartbeat);
	if err != nil {
		fmt.Printf("conn.Write to %s: %v\n", address, err);
	}
}

func listenForPackets(port string){
    udpAddress, err := net.ResolveUDPAddr("udp", ":" + port)
    if err != nil {
        fmt.Printf(fmt.Errorf("net.ResolveUDPAddr: %v\n", err).Error())
        os.Exit(1)
    }
	conn, err := net.ListenUDP("udp", udpAddress)
    if err != nil {
        fmt.Printf(fmt.Errorf("net.ListenUDP: %v\n", err).Error())
        os.Exit(1)
    }
    defer conn.Close()
	buffer := make([]byte, MAX_UDP_SIZE)
	for {
		bytesRead, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("net.ReadUDP: %v\n", err);
		}
		msg := &pb.HeartbeatMessage{}
		err = proto.Unmarshal(buffer[:bytesRead], msg)
		if err != nil {
			fmt.Printf("proto.Unmarshal: %v\n", err);
			continue;
		}
		// successfully unmarshalled
		processHeartbeat(*msg);
	}
}

func processHeartbeat(heartbeat pb.HeartbeatMessage){
	MembershipLock.Lock();
	defer MembershipLock.Unlock();
	for _, incomingEntry := range(heartbeat.Entries){
		// check if this entry is in the deadset
		if _, ok := deadMemberSet[incomingEntry.MachineId]; ok {
			fmt.Printf("%s is in the deaset, already marked dead...\n", incomingEntry.MachineId);
			continue;
		}

		// check if this entry is in the map
		if _, ok := MembershipList[incomingEntry.MachineId]; !ok {
			newEntry := *incomingEntry
			newEntry.LocalTime = time.Now().Unix()
			MembershipList[incomingEntry.MachineId] = newEntry;
			continue;
		}
		oldEntry, _ := MembershipList[incomingEntry.MachineId]
		if oldEntry.HeartbeatCounter >= incomingEntry.HeartbeatCounter{
			continue;
		}
		newEntry := *incomingEntry;
		newEntry.LocalTime = time.Now().Unix()
		MembershipList[incomingEntry.MachineId] = newEntry;
	}
}

func cleanMembershipList(){
	for {
		time.Sleep((5 * time.Second));
		MembershipLock.Lock()
		for id, entry := range(MembershipList){
			if id == ThisMachineId {
				continue;
			}
			if time.Now().Unix() - entry.LocalTime >= int64(T_FAIL) {
				// clean up this
				deadMemberSet[id] = 1;
				delete(MembershipList, id)
				fmt.Printf("deleted %s from the membership list...\n", id);
				fmt.Printf("New map: %v\n", MembershipList);
			}
		}
		MembershipLock.Unlock()
	}
}

func minInt(a int, b int) (int){
	if a <b {
		return a;
	} else {
		return b;
	}
}

func SendOutHeartbeat(){
	// go routine to send out heartbeats
	for {
		time.Sleep(1 * time.Second);
		MembershipLock.Lock()
		peers := make([]string, 0);
		for id, _:= range(MembershipList){
			if id == ThisMachineId{
				continue;
			}
			peers = append(peers, id);
		}
		// randomly order the array 
		for i := len(peers) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			peers[i], peers[j] = peers[j], peers[i]
		}
		num_pings := minInt(int(NUM_HEARTBEAT), len(peers))
		// ping those machines
		for i := 0; i < num_pings; i++ {
			peerid := peers[i]
			tableEntry := MembershipList[peerid]
			address := tableEntry.Hostname + ":" + tableEntry.Port
			SendHeartbeat(address)
		}
		MembershipLock.Unlock()
	}
}


func InitializeMembership(nodeNumber int64, membershipPort string){
	thisHostname, err := os.Hostname()
	if err != nil {
		fmt.Println(err);
	}
	MyNodeNumber = nodeNumber;
	MembershipPort = membershipPort;
	ThisHostname = thisHostname;
	// create our new machine id for the system
	utc_time := time.Now().Unix()
	ThisMachineId = strconv.FormatInt(MyNodeNumber, 10) + "_" + strconv.FormatInt(utc_time, 10)
	myEntry := pb.TableEntry{}
	myEntry.MachineId = ThisMachineId;
	myEntry.LocalTime = time.Now().Unix()
	myEntry.HeartbeatCounter = 0
	myEntry.Port = MembershipPort
	myEntry.Hostname = thisHostname
	MembershipLock.Lock()
	MembershipList[ThisMachineId] = myEntry
	MembershipLock.Unlock()
	fmt.Printf("ThisMachineId=%s, MyNodeNumber=%d, MembershipPort=%s, ThisHostname=%s\n", ThisMachineId, MyNodeNumber, MembershipPort, ThisHostname);
	go listenForPackets(MembershipPort);
	go cleanMembershipList();
	go SendOutHeartbeat();
	if MyIntroducer == ""{

	} else {
		SendHeartbeat(MyIntroducer);
	}
}

func GetMembers() ([]string) {
	MembershipLock.Lock()
	defer MembershipLock.Unlock()
	members := make([]string, 0)
	for member, _ := range(MembershipList) {
		members = append(members, member);
	}
	return members;
}

