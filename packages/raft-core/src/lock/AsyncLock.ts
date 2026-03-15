export class AsyncLock {
    private locked: boolean = false
    private queue: Array<() => void> = []

    async acquire(): Promise<void> {

        if (!this.locked) {
            this.locked = true;
            return;
        }

        await new Promise<void>(resolve => this.queue.push(resolve));
    }

    release(): void {
        if (!this.locked) {
            throw new Error('Cannot release an unlocked lock');
        }

        if (this.queue.length > 0) {
            const nextResolve = this.queue.shift()!;
            nextResolve();
        } else {
            this.locked = false;
        }
    }

    async runExclusive<T>(callback: () => Promise<T>): Promise<T> {
        await this.acquire();
        try {
            return await callback();
        } finally {
            this.release();
        }
    }

    isLocked(): boolean {
        return this.locked;
    }

    getQueueLength(): number {
        return this.queue.length;
    }
}

