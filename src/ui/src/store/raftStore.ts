import type { RaftEvent } from "../types/raftTypes";
import { create } from 'zustand'

interface RaftStore {
    nodeIds: string[];
    events: RaftEvent[];
    setNodeIds: (ids: string[]) => void;
    pushEvent: (event: RaftEvent) => void;
}

export const useRaftStore = create<RaftStore>((set) => ({
    nodeIds: [],
    events: [],
    setNodeIds: (ids) => set({ nodeIds: ids }),
    pushEvent: (event) => set(
        (state) => ({ events: [event, ...state.events]
            .slice(0, 100) })),
    })
)
