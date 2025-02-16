package asg3

import (
	"fmt"
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// TODO: add more fields here (what does each node need to keep track of?)
	bros     int
	marks    map[string]int
	lastMark int
	active   map[int]*ActiveSnapshot
	buffered map[int]bool
	lock     sync.Mutex
}

type ActiveSnapshot struct {
	node         *Node
	currentBro   int
	finishedBros int
	nodeToken    int
	messages     []MsgSnapshot
}

func (active *ActiveSnapshot) addMarker() bool {
	active.finishedBros++
	if active.finishedBros == active.currentBro {
		return true
	} else {
		return false
	}
}

func (active *ActiveSnapshot) addTokenMsg(src string, msg Message) {
	active.messages = append(active.messages, MsgSnapshot{src, active.node.id, msg})
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),
		// TODO: You may need to modify this if you make modifications above
		bros:     0,
		marks:    make(map[string]int),
		lastMark: -1,
		active:   make(map[int]*ActiveSnapshot),
		buffered: make(map[int]bool),
	}
}
func (node *Node) CreateActiveSnapshot(id int, fromBro bool) bool {
	node.active[id] = &ActiveSnapshot{node, node.bros, 0, node.tokens, make([]MsgSnapshot, 0)}
	if !fromBro {
		return false
	} else {
		return true
	}
}

func (node *Node) StartSnapshotRecord(src string, msg Message) {
	nextActive, f := node.marks[src]
	if !f {
		nextActive = 0
	} else {
		nextActive++
	}
	for {
		if nextActive > node.lastMark {
			break
		}
		_, ok := node.active[nextActive]
		if !ok {
			continue
		}
		(*((*node).active[nextActive])).addTokenMsg(src, msg)
		nextActive++
	}

}

func free(node *Node, snapshotId int) {
	_, ok := (*node).buffered[snapshotId+1]
	if ok {
		(*node).StartSnapshot(snapshotId + 1)
	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

func (node *Node) HandlePacket(src string, message Message) {
	// TODO: Write this method

	node.lock.Lock()
	defer node.lock.Unlock()
	if message.isMarker {
		node.marks[src] = message.data
		_, ok := node.active[message.data]
		if !ok {
			(*node).StartSnapshot(message.data)
		} else {
			if (*node.active[message.data]).addMarker() {
				node.sim.NotifyCompletedSnapshot(node.id, message.data)
			}
		}
	} else {
		node.tokens += message.data
		(*node).StartSnapshotRecord(src, message)
	}
	// 	node.recording = true
	// 	node.sim.logger.RecordEvent(node, ReceivedMsgRecord{src, node.id, message})
	// 	snapshotId := message.data
	// 	node.HandlePacket(src, message)
	// 	node.lock.Unlock()
	// 	node.sim.NotifyCompletedSnapshot(node.id, snapshotId)
	// } else {
	// 	// if node.recording {
	// 	// 	node.lock.Lock()
	// 	// 	node.messageSnapshots = append(node.messageSnapshots, &MsgSnapshot{
	// 	// 		src:     src,
	// 	// 		dest:    node.id,
	// 	// 		message: message,
	// 	// 	})
	// 	// 	node.sim.logger.RecordEvent(node, ReceivedMsgRecord{src, node.id, message})
	// 	// 	node.HandlePacket(src, message)
	// 	// 	node.lock.Unlock()
	// 	// 	node.sim.NotifyCompletedSnapshot(node.id, node.sim.nextSnapshotId-1)
	// 	// } else {
	// 	node.tokens += message.data
	// }
	// // }
}

func (node *Node) StartSnapshot(snapshotId int) {

	// ToDo: Write this method
	node.lock.Lock()
	defer node.lock.Unlock()
	if snapshotId > node.lastMark+1 {
		node.buffered[snapshotId] = true
		return
	}
	if snapshotId != node.lastMark+1 {
		fmt.Println()
	}
	node.lastMark = snapshotId
	if (*node).CreateActiveSnapshot(snapshotId, true) {
		(*node.sim).NotifyCompletedSnapshot(node.id, snapshotId)
	}
	(*node).SendToNeighbors(Message{true, snapshotId})
	free(node, snapshotId)
	// for dest := range node.outboundLinks {
	// 	node.SendTokens(node.tokens, dest)
	// }
	// node.sim.logger.RecordEvent(node, StartSnapshotRecord{node.id, snapshotId})
	// node.recording = true
	// node.localSnapshots = make(map[string]int)
	// node.messageSnapshots = nil
}
