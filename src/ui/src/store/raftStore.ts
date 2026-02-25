import type { MessageArrow, NodeUIState, RaftEvent } from "../types/raftTypes";
import { create } from 'zustand'

interface RaftStore {
    nodeIds: string[];
    events: RaftEvent[];
    nodes: Record<string, NodeUIState>;
    arrows: MessageArrow[];
    selectedNodeId?: string | null;
    setNodeIds: (ids: string[]) => void;
    pushEvent: (event: RaftEvent) => void;
    processEvent: (event: RaftEvent) => void;
    selectNode: (nodeId: string | null) => void;
    reset: () => void;
}

const makeNode = (nodeId: string): NodeUIState => ({
    nodeId,
    role: "Follower",
    term: 0,
    commitIndex: 0,
    votedFor: null,
    crashed: false,
    logEntries: [],
})

export const useRaftStore = create<RaftStore>((set) => ({
    nodeIds: [],
    events: [],
    nodes: {},
    arrows: [],
    selectedNodeId: null,
    setNodeIds: (ids) => { 
        const nodes: Record<string, NodeUIState> = {};
        for (const id of ids) {
            nodes[id] = makeNode(id);
        }
        set({ nodeIds: ids, nodes });
    },
    pushEvent: (event) => set(
        (state) => ({ events: [event, ...state.events]
            .slice(0, 100) })),
    processEvent: (event) => {
        switch (event.type) {
            case "NodeStateChanged": {
                set(state => ({
                    nodes: {
                        ...state.nodes,
                        [event.nodeId]: {
                            ...state.nodes[event.nodeId],
                            role: event.newState,
                            term: event.term,
                        },
                    },
                }));
                break;
            }
            case "MessageSent": {

                const isHeartbeat: boolean = event.messageType === "AppendEntries" && (event.payload as { entries: unknown[] })?.entries?.length === 0;

                const arrow: MessageArrow = {
                    id: event.messageId,
                    fromNodeId: event.fromNodeId,
                    toNodeId: event.toNodeId,
                    messageType: event.messageType,
                    status: "inFlight",
                    createdAt: Date.now(),
                    isHeartbeat: isHeartbeat,
                };
                set(state => ({ arrows: [...state.arrows, arrow] }));
                break;
            }

            case "MessageReceived": {
                if (event.messageType === "AppendEntriesResponse") {
                    const returnId = event.messageId + "-response";

                    const originalArrow = useRaftStore.getState().arrows.find(a => a.id === event.messageId);

                    set(s => ({ arrows: s.arrows.filter(a => a.id !== event.messageId) }));
                    set(s => ({ arrows: [...s.arrows, {
                        id: returnId,
                        fromNodeId: event.fromNodeId,
                        toNodeId: event.toNodeId,
                        messageType: event.messageType,
                        status: "inFlight" as const,
                        createdAt: Date.now(),
                        isHeartbeat: originalArrow?.isHeartbeat ?? true,
                    }]}));
                    setTimeout(() => {
                        set(s => ({ arrows: s.arrows.filter(a => a.id !== returnId) }));
                    }, 300);
                } else if (event.messageType === "RequestVoteResponse") {

                    const returnId = event.messageId + "-response";
                    setTimeout(() => {
                        set(s => ({ arrows: s.arrows.filter(a => a.id !== event.messageId) }));
                    }, 1000);
                    setTimeout(() => {
                        set(s => ({ arrows: [...s.arrows, {
                            id: returnId,
                            fromNodeId: event.fromNodeId,
                            toNodeId: event.toNodeId,
                            messageType: event.messageType,
                            status: "inFlight" as const,
                            createdAt: Date.now(),
                            isHeartbeat: false,
                        }]}));
                    }, 1500);
                    setTimeout(() => {
                        set(s => ({ arrows: s.arrows.filter(a => a.id !== returnId) }));
                    }, 2500);
                }
                break;
            }

            case "MessageDropped": {
                set(state => ({
                    arrows: state.arrows.map(a => a.id === event.messageId ? { ...a, status: "dropped" } : a)
                }));
                break;
            }

            case "TermChanged": {
                set(state => ({
                    nodes: {
                        ...state.nodes,
                        [event.nodeId]: {
                            ...state.nodes[event.nodeId],
                            term: event.newTerm,
                        },
                    },
                }));
                break;
            }

            case "CommitIndexAdvanced": {
                set(state => ({
                    nodes: {
                        ...state.nodes,
                        [event.nodeId]: {
                            ...state.nodes[event.nodeId],
                            commitIndex: event.newCommitIndex,
                        },
                    },
                }));
                break;
            }

            case "VoteGranted": {
                set(state => ({
                    nodes: {
                        ...state.nodes,
                        [event.nodeId]: {
                            ...state.nodes[event.nodeId],
                            votedFor: event.candidateId,
                        },
                    },
                }));
                break;
            }
        }
    },
    selectNode: (nodeId) => set({ selectedNodeId: nodeId }),
    reset: () => set({ nodeIds: [], events: [], nodes: {}, arrows: [], selectedNodeId: null }),
    })
)
