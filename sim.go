package asg3

import (
	"log"
	"math/rand"
)

// Max random delay added to packet delivery
const maxDelay = 5

type ChandyLamportSim struct {
	time           int
	nextSnapshotId int
	nodes          map[string]*Node // key = node ID
	logger         *Logger
	// TODO: You can add more fields here
	snaps          map[int]*GlobalSnapshot
	completedSnaps map[int]int
	results        map[int]*(chan *GlobalSnapshot)
}

func NewSimulator() *ChandyLamportSim {
	return &ChandyLamportSim{
		time:           0,
		nextSnapshotId: 0,
		nodes:          make(map[string]*Node),
		logger:         NewLogger(),
		// ToDo: you may need to modify this if you modify the above struct
		snaps:          make(map[int]*GlobalSnapshot),
		completedSnaps: make(map[int]int),
		results:        make(map[int]*chan *GlobalSnapshot),
	}
}

// Add a node to this simulator with the specified number of starting tokens
func (sim *ChandyLamportSim) AddNode(id string, tokens int) {
	node := CreateNode(id, tokens, sim)
	sim.nodes[id] = node
}

// Add a unidirectional link between two nodes
func (sim *ChandyLamportSim) AddLink(src string, dest string) {
	node1, ok1 := sim.nodes[src]
	node2, ok2 := sim.nodes[dest]
	if !ok1 {
		log.Fatalf("Node %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Node %v does not exist\n", dest)
	}
	node1.AddOutboundLink(node2)
}

func (sim *ChandyLamportSim) ProcessEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.nodes[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.nodeId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step and deliver at most one packet per node
func (sim *ChandyLamportSim) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the nodes,
	// we must also iterate through the nodes and the links in a deterministic way
	for _, nodeId := range getSortedKeys(sim.nodes) {
		node := sim.nodes[nodeId]
		for _, dest := range getSortedKeys(node.outboundLinks) {
			link := node.outboundLinks[dest]
			// Deliver at most one packet per node at each time step to
			// establish total ordering of packet delivery to each node
			if !link.msgQueue.Empty() {
				e := link.msgQueue.Peek().(SendMsgEvent)
				if e.receiveTime <= sim.time {
					link.msgQueue.Pop()
					sim.logger.RecordEvent(
						sim.nodes[e.dest],
						ReceivedMsgRecord{e.src, e.dest, e.message})
					sim.nodes[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Return the receive time of a message after adding a random delay.
// Note: At each time step, only one message is delivered to a destination.
// This implies that the message may be received *after* the time step returned in this function.
// See the clarification in the document of the assignment
func (sim *ChandyLamportSim) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(maxDelay)
}

func (sim *ChandyLamportSim) StartSnapshot(nodeId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.nodes[nodeId], StartSnapshotRecord{nodeId, snapshotId})
	// TODO: Complete this method
	snap := GlobalSnapshot{snapshotId, make(map[string]int), make([]*MsgSnapshot, 0)}
	sim.snaps[snapshotId] = &snap
	sim.nodes[nodeId].StartSnapshot(snapshotId)
	f := make(chan *GlobalSnapshot)
	sim.results[snapshotId] = &f
	// node := sim.nodes[nodeId]
	// node.lock.Lock()
	// defer node.lock.Unlock()
	// node.recording = true
	// for destID := range node.outboundLinks {
	// 	node.SendTokens(0, destID)
	// }

}

func (sim *ChandyLamportSim) NotifyCompletedSnapshot(nodeId string, snapshotId int) {
	sim.logger.RecordEvent(sim.nodes[nodeId], EndSnapshotRecord{nodeId, snapshotId})

	// TODO: Complete this method
	// sim.CollectSnapshot(snapshotId)
	result := sim.nodes[nodeId].active[snapshotId]
	sim.snaps[snapshotId].tokenMap[nodeId] = result.nodeToken
	for i, _ := range result.messages {
		sim.snaps[snapshotId].messages = append(sim.snaps[snapshotId].messages, &result.messages[i])
	}
	sim.completedSnaps[snapshotId]++
	if sim.completedSnaps[snapshotId] == len(sim.nodes) {
		go func(sim *ChandyLamportSim, snapshotId int) {
			t := sim.results[snapshotId]
			*(t) <- sim.snaps[snapshotId]
		}(sim, snapshotId)
	}

}

func (sim *ChandyLamportSim) CollectSnapshot(snapshotId int) *GlobalSnapshot {

	// TODO: Complete this method
	snap := GlobalSnapshot{snapshotId, make(map[string]int), make([]*MsgSnapshot, 0)}
	for _, node := range sim.nodes {
		// Check if the node has recorded snapshot information for the given snapshot ID
		if result, ok := node.buffered[snapshotId]; ok {
			// Aggregate token information
			snap.tokenMap[node.id] = result
			// Aggregate message snapshots
			for i := range result.messages {
				snap.messages = append(snap.messages, &result.messages[i])
			}
		}
	}
	return &snap
}
