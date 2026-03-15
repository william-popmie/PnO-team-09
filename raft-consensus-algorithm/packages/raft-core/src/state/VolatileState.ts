import { VolatileStateError } from "../util/Error";

export interface VolatileStateSnapshot {
    commitIndex: number;
    lastApplied: number;
}

export interface VolatileStateInterface {
    getCommitIndex(): number;
    setCommitIndex(index: number): void;
    // advanceCommitIndex(): void;
    getLastApplied(): number;
    // setLastApplied(index: number): void;
    advanceLastApplied(): number;
    needsApplication(): boolean;
    getNextIndexToApply(): number | null;
    getPendingApplicationsCount(): number;
    reset(): void;
    snapshot(): VolatileStateSnapshot;
}

export class VolatileState implements VolatileStateInterface {
    private commitIndex: number = 0
    private lastApplied: number = 0;

    getCommitIndex(): number {
        return this.commitIndex;
    }

    setCommitIndex(index: number): void {

        this.validateIndex(index, 'Commit index');

        if (index < this.commitIndex) {
            throw new VolatileStateError(`Commit index cannot be decreased (current: ${this.commitIndex}, attempted: ${index})`);
        }

        this.commitIndex = index;
    }

    advanceCommitIndex(): void {
        this.validateIndex(this.commitIndex + 1, 'Commit index');
        this.commitIndex++;
    }

    getLastApplied(): number {
        return this.lastApplied;
    }

    setLastApplied(index: number): void {
        this.validateIndex(index, 'Last applied index');

        if (index < this.lastApplied) {
            throw new VolatileStateError(`Last applied index cannot be decreased (current: ${this.lastApplied}, attempted: ${index})`);
        }

        if (index > this.commitIndex) {
            throw new VolatileStateError(`Last applied index cannot be greater than commit index (commitIndex: ${this.commitIndex}, attempted: ${index})`);
        }

        this.lastApplied = index;
    }

    advanceLastApplied(): number {
        if (this.lastApplied >= this.commitIndex) {
            throw new VolatileStateError(`Cannot advance last applied index beyond commit index (commitIndex: ${this.commitIndex}, attempted: ${this.lastApplied + 1})`);
        }

        this.lastApplied++;

        return this.lastApplied;
    }

    needsApplication(): boolean {
        return this.lastApplied < this.commitIndex;
    }

    getNextIndexToApply(): number | null {
        if (!this.needsApplication()) {
            return null;
        }

        return this.lastApplied + 1;
    }

    getPendingApplicationsCount(): number {

        return this.commitIndex - this.lastApplied;
    }

    reset(): void {
        this.commitIndex = 0;
        this.lastApplied = 0;
    }

    snapshot(): VolatileStateSnapshot {
        return {
            commitIndex: this.commitIndex,
            lastApplied: this.lastApplied,
        };
    }

    restoreFromSnapshot(snapshot: VolatileStateSnapshot): void {
        this.validateIndex(snapshot.commitIndex, 'Commit index');
        this.validateIndex(snapshot.lastApplied, 'Last applied index');

        if (snapshot.commitIndex < this.commitIndex) {
            throw new VolatileStateError(`Cannot restore from snapshot with lower commit index (current: ${this.commitIndex}, snapshot: ${snapshot.commitIndex})`);
        }

        if (snapshot.lastApplied > snapshot.commitIndex) {
            throw new VolatileStateError(`Snapshot last applied index cannot be greater than commit index (commitIndex: ${snapshot.commitIndex}, lastApplied: ${snapshot.lastApplied})`);
        }

        this.commitIndex = snapshot.commitIndex;
        this.lastApplied = snapshot.lastApplied;
    }

    private validateIndex(value: number, name: string): void {
        if (!Number.isInteger(value) || value < 0) {
            throw new VolatileStateError(`${name} must be a non-negative integer (got ${value})`);
        }
    }
}
