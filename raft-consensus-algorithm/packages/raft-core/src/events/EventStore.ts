import { RaftEvent, RaftEventBus } from "./RaftEvents";

export interface EventStoreOptions {
    maxEvents: number;
}

export class EventStore {
    private events: RaftEvent[] = [];
    private liveSubscribers: Set<(event: RaftEvent) => void> = new Set();
    
    constructor(bus: RaftEventBus, private options: EventStoreOptions) {
        bus.subscribe((event) => this.append(event));
    }

    private append(event: RaftEvent): void {
        this.events.push(event);

        if (this.events.length > this.options.maxEvents) {
            this.events.shift();
        }

        for (const subscriber of this.liveSubscribers) {
            subscriber(event);
        }
    }

    getAllEvents(): RaftEvent[] {
        return [...this.events];
    }

    onLiveEvent(subscriber: (event: RaftEvent) => void): () => void {
        this.liveSubscribers.add(subscriber);
        return () => this.liveSubscribers.delete(subscriber);
    }

    getSize(): number {
        return this.events.length;
    }
}