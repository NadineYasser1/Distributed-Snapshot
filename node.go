package asg3

import (
	"log"
	"strconv"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of Tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// TODO: add more fields here (what does each node need to keep track of?)
	mutex        sync.RWMutex
	pState       map[int]int
	recStatus    map[int]bool
	inChanMarked map[string]bool
	inChanData   map[string]string
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
		mutex:        sync.RWMutex{},
		pState:       make(map[int]int),
		recStatus:    make(map[int]bool),
		inChanMarked: make(map[string]bool),
		inChanData:   make(map[string]string),
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

// Send a number of Tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v Tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the Tokens
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
	node.sim.logger.RecordEvent(
		node,
		ReceivedMsgRecord{src, node.id, message})
	if message.isMarker {
		node.handleMarker(src, message)
	} else {
		node.handleToken(src, message)
	}

}

func (node *Node) handleMarker(src string, message Message) {
	snapshotID := message.data
	directions := node.inboundLinks[src].src + "->" + node.inboundLinks[src].dest
	data := strconv.Itoa(snapshotID) + "|" + directions

	node.mutex.Lock()
	node.inChanMarked[data] = true
	node.mutex.Unlock()

	node.mutex.RLock()
	_, exists := node.recStatus[snapshotID]
	node.mutex.RUnlock()

	if !exists {
		node.recStatus[snapshotID] = true
		node.StartSnapshot(snapshotID)
		node.mutex.Lock()
		node.inChanData[data] = ""
		node.mutex.Unlock()
	}
	count := 0
	for srcNode := range node.inboundLinks {
		dir := srcNode + "->" + node.id
		fMarker := strconv.Itoa(snapshotID) + "|" + dir
		node.mutex.RLock()
		_, visited := node.inChanMarked[fMarker]
		node.mutex.RUnlock()
		if visited {
			count++
		}
	}
	if count == len(node.inboundLinks) {
		node.recStatus[snapshotID] = false
		node.sim.NotifyCompletedSnapshot(node.id, snapshotID)
	}
}

func (node *Node) handleToken(src string, message Message) {

	node.tokens += message.data
	if len(node.recStatus) > 0 {
		for id, status := range node.recStatus {
			directions := node.inboundLinks[src].src + "->" + node.inboundLinks[src].dest
			data := strconv.Itoa(id) + "|" + directions
			node.mutex.RLock()
			_, visited := node.inChanMarked[data]
			node.mutex.RUnlock()
			if status && !visited {
				node.mutex.RLock()
				st, status := node.inChanData[data]
				node.mutex.RUnlock()
				if status {
					m := st + "|" + strconv.Itoa(message.data)
					node.mutex.Lock()
					node.inChanData[data] = m
					node.mutex.Unlock()
				} else {
					node.mutex.Lock()
					node.inChanData[data] = strconv.Itoa(message.data)
					node.mutex.Unlock()
				}

			}
		}
	}
}
func (node *Node) StartSnapshot(snapshotId int) {

	node.recStatus[snapshotId] = true
	node.mutex.Lock()
	node.pState[snapshotId] = node.tokens
	node.mutex.Unlock()
	node.SendToNeighbors(Message{true, snapshotId})
}
