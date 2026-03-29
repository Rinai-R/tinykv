// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower's progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// 随机化选举超时
	randomizedElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftLog := newLog(c.Storage)

	hardState, confState, _ := c.Storage.InitialState()

	peers := c.peers
	if len(confState.Nodes) > 0 {
		peers = confState.Nodes
	}

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}

	for _, id := range peers {
		r.Prs[id] = &Progress{
			Next:  raftLog.LastIndex() + 1,
			Match: 0,
		}
	}

	r.resetRandomizedElectionTimeout()
	return r
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {

	pr := r.Prs[to]
	prevLogIndex := pr.Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		return false
	}
	var entries []*pb.Entry
	if len(r.RaftLog.entries) > 0 {
		first := r.RaftLog.entries[0].Index
		if pr.Next >= first {
			rawEnts := r.RaftLog.entries[pr.Next-first:]
			entries = make([]*pb.Entry, len(rawEnts))
			for i := range rawEnts {
				entries[i] = &rawEnts[i]
			}
		}
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	})
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {

	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {

	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {

	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {

	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Next:  lastIndex + 1,
			Match: 0,
		}
	}
	r.Prs[r.id].Match = lastIndex
	r.Prs[r.id].Next = lastIndex + 1

	// 追加一条 noop entry
	r.appendEntries([]*pb.Entry{{}}...)

	// 单节点直接提交
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	r.bcastAppend()
}

// 向本地日志追加 entries，并更新自身的 Progress
func (r *Raft) appendEntries(ents ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, ent := range ents {
		ent.Index = lastIndex + uint64(i) + 1
		ent.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *ent)
	}
	li := r.RaftLog.LastIndex()
	r.Prs[r.id].Match = li
	r.Prs[r.id].Next = li + 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// 如果收到更高 term 的消息，立刻降级为 follower
	if m.Term > r.Term {
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	}

	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgAppend:
		// 收到合法的 append 说明已有 leader
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	// 单节点直接成为 leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			Term:    r.Term,
			Index:   lastIndex,
			LogTerm: lastTerm,
		})
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true
	if m.Term < r.Term {
		// 旧 term，拒绝
	} else if r.Vote == None || r.Vote == m.From {
		// 检查日志是否足够新
		if r.isLogUpToDate(m.LogTerm, m.Index) {
			reject = false
			r.Vote = m.From
			r.electionElapsed = 0
			r.resetRandomizedElectionTimeout()
		}
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) isLogUpToDate(lastTerm, lastIndex uint64) bool {
	myLastIndex := r.RaftLog.LastIndex()
	myLastTerm, _ := r.RaftLog.Term(myLastIndex)
	return lastTerm > myLastTerm || (lastTerm == myLastTerm && lastIndex >= myLastIndex)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	granted := 0
	denied := 0
	for _, v := range r.votes {
		if v {
			granted++
		} else {
			denied++
		}
	}
	quorum := len(r.Prs)/2 + 1
	if granted >= quorum {
		r.becomeLeader()
	} else if denied >= quorum {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	r.appendEntries(m.Entries...)
	// 单节点直接提交
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	r.bcastAppend()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {

	if m.Term < r.Term {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	r.Lead = m.From
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()

	lastIndex := r.RaftLog.LastIndex()

	// 检查 prevLogIndex 位置的日志是否匹配
	if m.Index > lastIndex {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			Index:   lastIndex + 1,
		})
		return
	}

	if m.Index > 0 {
		prevTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || prevTerm != m.LogTerm {
			// 日志不匹配，返回冲突信息
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
				Index:   m.Index,
			})
			return
		}
	}

	// 找到第一个冲突点，截断并追加
	for i, ent := range m.Entries {
		if ent.Index <= r.RaftLog.LastIndex() {
			existTerm, _ := r.RaftLog.Term(ent.Index)
			if existTerm == ent.Term {
				continue
			}
			// 冲突：截断并追加剩余
			offset := ent.Index - r.RaftLog.entries[0].Index
			r.RaftLog.entries = r.RaftLog.entries[:offset]
			if r.RaftLog.stabled >= ent.Index {
				r.RaftLog.stabled = ent.Index - 1
			}
		}
		// 从冲突点（或超出现有日志的点）开始追加
		for _, e := range m.Entries[i:] {
			r.RaftLog.entries = append(r.RaftLog.entries, *e)
		}
		break
	}

	// 更新 committed
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
	})
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		next := m.Index
		if next < 1 {
			next = 1
		}
		r.Prs[m.From].Next = next
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
	}
	r.maybeCommit()
}

func (r *Raft) maybeCommit() {
	matches := make(uint64Slice, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matches = append(matches, pr.Match)
	}
	sort.Sort(sort.Reverse(matches))
	quorumMatch := matches[len(r.Prs)/2]
	if quorumMatch > r.RaftLog.committed {
		t, _ := r.RaftLog.Term(quorumMatch)
		if t == r.Term {
			r.RaftLog.committed = quorumMatch
			r.bcastAppend()
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {

	if m.Term < r.Term {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		Term:    r.Term,
	})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// 如果 follower 的日志落后，发送 append
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
