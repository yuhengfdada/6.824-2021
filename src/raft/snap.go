package raft

func (rf *Raft) sendApplySS(snapshot []byte, lastInstalledTerm int, lastInstalledIndex int) {
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  lastInstalledTerm,
		SnapshotIndex: lastInstalledIndex,
	}
	rf.applyCh <- applyMsg
}

func (rf *Raft) sendInstall(index int) {
	ssArgs := InstallSnapshotArgs{
		LTerm:              rf.currentTerm,
		LID:                rf.me,
		LastInstalledIndex: rf.lastInstalledIndex,
		LastInstalledTerm:  rf.lastInstalledTerm,
		Snapshot:           rf.snapshot,
	}
	ssReply := InstallSnapshotReply{}
	rf.unlock("installSnapshot lock")
	ok := rf.sendInstallSnapshot(index, &ssArgs, &ssReply)
	if !ok {
		return
	}
	if ssReply.Term > ssArgs.LTerm {
		rf.lock("installSS_change_term")
		if rf.currentTerm < ssReply.Term {
			rf.receivedLargerTerm(ssReply.Term)
			rf.unlock("installSS_change_term")
			return
		}
		rf.unlock("installSS_change_term")
	}
	// panic("installSnapshot not implemented")
	//rf.unlock("installSnapshot RPC sent")
}
