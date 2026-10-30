"""内存版 Raft 选举：共享状态在锁内修改，出站网络请求在工作线程执行。"""

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
import logging
import random
import threading
import time
from typing import Callable

from .config import Config
from .events import emit
from .protocol import AppendEntries, RequestVote, log_is_up_to_date, parse_response
from .transport import HTTPTransport


class NodeUnavailable(RuntimeError):
    pass


class RaftNode:
    def __init__(self, node_id: str, config: Config, *, transport=None,
                 clock: Callable[[], float] = time.monotonic, rng=None):
        self.node_id = node_id
        self.config = config
        # 每次创建节点都从初始状态开始，不读取或写入数据库、文件。
        self.current_term = 0
        self.voted_for: str | None = None
        self.role = "follower"
        self.leader_id: str | None = None
        self.peers = {key: url for key, url in config.nodes.items() if key != node_id}
        self.majority = len(config.nodes) // 2 + 1
        self.clock = clock
        self.rng = rng if rng is not None else random.Random()
        self.transport = transport or HTTPTransport(config.rpc_timeout_ms / 1000)
        # HTTP 请求线程、计时线程、RPC 回调线程共同使用这一把状态锁。
        # Condition 还用于在角色或超时时间变化后唤醒计时线程。
        self._condition = threading.Condition(threading.RLock())
        self._executor = ThreadPoolExecutor(max_workers=max(10, len(self.peers)),
                                             thread_name_prefix="raft-rpc")
        self._busy: set[str] = set()
        self._votes: set[str] = set()
        self._running = False
        self._closed = False
        self._deadline = 0.0
        self._next_heartbeat = 0.0
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        with self._condition:
            if self._closed or self._running:
                raise RuntimeError("Node cannot be started twice")
            self._running = True
            self._reset_deadline()
            self._thread = threading.Thread(target=self._run, name="raft-timer")
            self._thread.start()

    def stop(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._running = False
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join()
        self._executor.shutdown(wait=True, cancel_futures=True)
        with self._condition:
            self._closed = True

    def _require_running(self) -> None:
        if not self._running:
            raise NodeUnavailable("Node is stopping or has not started")

    def _reset_deadline(self) -> None:
        self._deadline = self.clock() + self.rng.uniform(
            self.config.election_timeout_min_ms,
            self.config.election_timeout_max_ms) / 1000
        self._condition.notify_all()

    def _set_term_vote(self, term: int, vote: str | None) -> None:
        """调用者必须持有状态锁；任期和投票作为一次状态变更共同更新。"""
        self.current_term, self.voted_for = term, vote

    def _follow(self, term: int) -> None:
        previous_role = self.role
        previous_term = self.current_term
        if term > self.current_term:
            self._set_term_vote(term, None)
        self.role = "follower"
        self.leader_id = None
        self._votes.clear()
        self._condition.notify_all()
        if previous_role != self.role or previous_term != self.current_term:
            emit("became_follower", nodeId=self.node_id, currentTerm=self.current_term,
                 previousRole=previous_role, previousTerm=previous_term)

    def last_log(self) -> tuple[int, int]:
        """(last term, last index); extension point for a future replicated log."""
        return 0, 0

    def status(self) -> dict:
        with self._condition:
            # 在同一临界区内复制所有字段，避免读到角色与任期不一致的快照。
            self._require_running()
            log_term, log_index = self.last_log()
            return {
                "nodeId": self.node_id,
                "role": self.role,
                "currentTerm": self.current_term,
                "votedFor": self.voted_for,
                "leaderId": self.leader_id,
                "leaderUrl": self.config.nodes.get(self.leader_id),
                "peers": dict(self.peers),
                "lastLogIndex": log_index,
                "lastLogTerm": log_term,
                "commitIndex": 0,
                "lastApplied": 0,
            }

    def request_vote(self, request: RequestVote) -> dict:
        with self._condition:
            self._require_running()
            if request.term > self.current_term:
                self._follow(request.term)
            granted = False
            if request.term == self.current_term:
                local_term, local_index = self.last_log()
                can_vote = self.voted_for in (None, request.candidateId)
                if can_vote and log_is_up_to_date(
                        request.lastLogTerm, request.lastLogIndex, local_term, local_index):
                    if self.voted_for != request.candidateId:
                        self._set_term_vote(self.current_term, request.candidateId)
                    self._reset_deadline()
                    granted = True
                    emit("vote_granted", nodeId=self.node_id,
                         currentTerm=self.current_term, candidateId=request.candidateId)
            return {"term": self.current_term, "voteGranted": granted}

    def append_entries(self, request: AppendEntries) -> dict:
        with self._condition:
            self._require_running()
            if request.term < self.current_term:
                return {"term": self.current_term, "success": False}
            if request.term > self.current_term or self.role != "follower":
                self._follow(request.term)
            self.leader_id = request.leaderId
            # Even a log mismatch proves a current-term leader is communicating.
            self._reset_deadline()
            success = request.prevLogIndex == 0 and request.prevLogTerm == 0
            return {"term": self.current_term, "success": success}

    def _start_election(self) -> None:
        self._set_term_vote(self.current_term + 1, self.node_id)
        self.role = "candidate"
        self.leader_id = None
        self._votes = {self.node_id}
        self._reset_deadline()
        emit("election_started", nodeId=self.node_id, currentTerm=self.current_term)
        if len(self._votes) >= self.majority:
            self._become_leader()
            return
        last_term, last_index = self.last_log()
        request = RequestVote(self.current_term, self.node_id, last_index, last_term)
        self._broadcast("/raft/request-vote", asdict(request))

    def _become_leader(self) -> None:
        self.role = "leader"
        self.leader_id = self.node_id
        self._next_heartbeat = self.clock()
        self._condition.notify_all()
        emit("became_leader", nodeId=self.node_id, currentTerm=self.current_term,
             votes=len(self._votes), majority=self.majority)

    def _broadcast(self, path: str, body: dict) -> None:
        # One in-flight request per peer bounds both workers and the work queue.
        for peer_id, address in self.peers.items():
            if peer_id in self._busy:
                continue
            self._busy.add(peer_id)
            future = self._executor.submit(self.transport.call, address, path, body)
            future.add_done_callback(
                lambda result, peer=peer_id, rpc=path, term=body["term"]:
                    self._receive_response(peer, rpc, term, result))

    def _receive_response(self, peer_id: str, path: str, sent_term: int,
                          future: Future) -> None:
        flag = "voteGranted" if path == "/raft/request-vote" else "success"
        try:
            response_term, accepted = parse_response(future.result(), flag)
        except Exception as exc:
            response_term, accepted = None, False
            emit("rpc_failed", level=logging.DEBUG, nodeId=self.node_id,
                 peerId=peer_id, rpc=path, error=str(exc))
        with self._condition:
            self._busy.discard(peer_id)
            if not self._running or response_term is None:
                return
            # 锁只能防止同时修改；任期检查还要防止网络迟到的旧消息改变新状态。
            if response_term > self.current_term:
                self._follow(response_term)
            if (flag == "voteGranted" and accepted and self.role == "candidate"
                    and sent_term == self.current_term == response_term):
                self._votes.add(peer_id)
                if len(self._votes) >= self.majority:
                    self._become_leader()

    def _run(self) -> None:
        with self._condition:
            while self._running:
                now = self.clock()
                if self.role == "leader":
                    if now >= self._next_heartbeat:
                        request = AppendEntries(self.current_term, self.node_id,
                                                0, 0, [], 0)
                        self._broadcast("/raft/append-entries", asdict(request))
                        self._next_heartbeat = now + self.config.heartbeat_interval_ms / 1000
                    wake_at = self._next_heartbeat
                else:
                    if now >= self._deadline:
                        self._start_election()
                        continue
                    wake_at = self._deadline
                # wait() 等待期间释放锁；唤醒后重新获取锁、重新检查状态。
                self._condition.wait(timeout=max(0, wake_at - self.clock()))
