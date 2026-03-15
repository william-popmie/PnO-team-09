import { RaftEvent, RaftEventBus } from "./RaftEvents";

export class LocalEventBus implements RaftEventBus {
    private handlers: Array<(event: RaftEvent) => void> = [];

    emit(event: RaftEvent): void {
        for (const handler of this.handlers) {
            try {
                handler(event);
            } catch {
                // pass
            }
        }
    }

    subscribe(handler: (event: RaftEvent) => void): () => void {
        this.handlers.push(handler);
        return () => {
            this.handlers = this.handlers.filter(h => h !== handler);
        };
    }
}

export class NoOpEventBus implements RaftEventBus {
    emit(_event: RaftEvent): void {
        // no-op
    }
    subscribe(handler: (event: RaftEvent) => void): () => void {
        return () => {
            // no-op
        };
    }
}