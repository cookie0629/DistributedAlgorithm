"""HTTP wire models keep the field names used in the Raft paper."""

from dataclasses import dataclass


MAX_BODY_BYTES = 64 * 1024


def integer(body: dict, name: str) -> int:
    value = body.get(name)
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    # 保持协议整数范围固定，即使 Python 内存中的整数可以继续增长。
    if value > 2**63 - 1:
        raise ValueError(f"{name} exceeds the supported integer range")
    return value


def member(body: dict, name: str, members: dict[str, str]) -> str:
    value = body.get(name)
    if not isinstance(value, str) or value not in members:
        raise ValueError(f"{name} must identify a configured member")
    return value


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidateId: str
    lastLogIndex: int
    lastLogTerm: int

    @classmethod
    def parse(cls, body: dict, members: dict[str, str]) -> "RequestVote":
        return cls(integer(body, "term"), member(body, "candidateId", members),
                   integer(body, "lastLogIndex"), integer(body, "lastLogTerm"))


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leaderId: str
    prevLogIndex: int
    prevLogTerm: int
    entries: list
    leaderCommit: int

    @classmethod
    def parse(cls, body: dict, members: dict[str, str]) -> "AppendEntries":
        entries = body.get("entries")
        if not isinstance(entries, list) or entries:
            raise ValueError("Only empty entries (heartbeats) are supported")
        return cls(integer(body, "term"), member(body, "leaderId", members),
                   integer(body, "prevLogIndex"), integer(body, "prevLogTerm"),
                   [], integer(body, "leaderCommit"))


def parse_response(body: dict, flag: str) -> tuple[int, bool]:
    term = integer(body, "term")
    if type(body.get(flag)) is not bool:
        raise ValueError(f"{flag} must be a boolean")
    return term, body[flag]


def log_is_up_to_date(candidate_term: int, candidate_index: int,
                      local_term: int, local_index: int) -> bool:
    return (candidate_term, candidate_index) >= (local_term, local_index)
