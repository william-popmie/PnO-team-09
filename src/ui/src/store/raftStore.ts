import type { NodeUIState, RaftEvent } from "../types/raftTypes";
import { create } from 'zustand'

interface RaftStore {
    nodeIds: string[];
    events: RaftEvent[];
    nodes: Record<string, NodeUIState>;
    setNodeIds: (ids: string[]) => void;
    pushEvent: (event: RaftEvent) => void;
    processEvent: (event: RaftEvent) => void;
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
            default:
                break;
        }
    },
    reset: () => set({ nodeIds: [], events: [], nodes: {} }),
    })
)
