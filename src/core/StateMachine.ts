import { NodeId } from "./Config";
import { RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse } from "../rpc/RPCTypes";

export enum RaftState {
    Follower = "Follower",
    Candidate = "Candidate",
    Leader = "Leader"
}

export interface StateMachineInterface {
    start(): Promise<void>;
    stop(): Promise<void>;
    getCurrentState(): RaftState;
    getCurrentLeader(): NodeId | null;
    isLeader(): boolean;
    becomeFollower(term: number, leaderId: NodeId | null): Promise<void>;
    becomeCandidate(): Promise<void>;
    becomeLeader(): Promise<void>;
    handleRequestVote(from: NodeId, request: RequestVoteRequest): Promise<RequestVoteResponse>;
    handleAppendEntries(from: NodeId, request: AppendEntriesRequest): Promise<AppendEntriesResponse>;
}