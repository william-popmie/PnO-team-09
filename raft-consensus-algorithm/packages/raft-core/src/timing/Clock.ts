export type TimerHandle = number | NodeJS.Timeout;

export interface Clock {
    now(): number;
    setTimeout(callback: () => void, delayMs: number): TimerHandle;
    clearTimeout(handle: TimerHandle): void;
}

export interface MockTimer {
    id: number;
    fireTime: number;
    callback: () => void;
}

export class MockClock implements Clock {
    private currentTime: number = 0;
    private timers: Map<number, MockTimer> = new Map();
    private nextTimerId: number = 1;

    now(): number {
        return this.currentTime;
    }

    setTimeout(callback: () => void, delayMs: number): number {
        const id = this.nextTimerId++;
        const fireTime = this.currentTime + delayMs;
        this.timers.set(id, { id, fireTime, callback });
        return id;
    }

    clearTimeout(handle: number): void {
        if (typeof handle !== 'number') {
            throw new Error("Invalid timer handle type");
        }

        this.timers.delete(handle);
    }

    advanceMs(ms: number): void {
        if (ms < 0) {
            throw new Error("Cannot advance time by a negative amount");
        }

        this.currentTime += ms;
        this.fireTimers();
    }

    async advanceAsyncMs(ms: number): Promise<void> {
        this.advanceMs(ms);
        await this.tick();
    }

    advanceToNextTimer(): number {
        const nextTimer = this.getNextTimer();
        if (!nextTimer) {
            return 0;
        }

        const timeToAdvance = Math.max(0, nextTimer.fireTime - this.currentTime);

        if (timeToAdvance > 0) {
            this.advanceMs(timeToAdvance);
        }

        return timeToAdvance;
    }

    async advanceToNextTimerAsync(): Promise<number> {
        const advanced = this.advanceToNextTimer();
        await this.tick();
        return advanced;
    }

    advanceToEnd(): void {
        let iterations = 0;
        const maxIterations = 1000;

        while (this.timers.size > 0 && iterations < maxIterations) {
            this.advanceToNextTimer();
            iterations++;
        }

        if (iterations === maxIterations) {
            throw new Error("Too many timers, possible infinite loop");
        }
    }

    async advanceToEndAsync(): Promise<void> {
        let iterations = 0;
        const maxIterations = 1000;

        while (this.timers.size > 0 && iterations < maxIterations) {
            await this.advanceToNextTimerAsync();
            await this.tick();
            iterations++;
        }

        if (iterations === maxIterations) {
            throw new Error("Too many timers, possible infinite loop");
        }
    }

    async tick(): Promise<void> {
        await new Promise(resolve => setImmediate(resolve));
    }

    async tickMultiple(times: number): Promise<void> {
        for (let i = 0; i < times; i++) {
            await this.tick();
        }
    }

    async runUntilIdle(maxIterations: number = 100): Promise<void> {
        let iterations = 0;

        while (iterations < maxIterations) {
            const hadTimers = this.timers.size > 0;

            this.fireTimers();

            await this.tick();

            if (!hadTimers && this.timers.size === 0) {
                break;
            }
            iterations++;
        }

        if (iterations === maxIterations) {
            throw new Error("Too many timers, possible infinite loop");
        }
    }

    private fireTimers(): void {
        const timersToFire = Array.from(this.timers.values())
            .filter(timer => timer.fireTime <= this.currentTime)
            .sort((a, b) => a.fireTime - b.fireTime);

        for (const timer of timersToFire) {
            this.timers.delete(timer.id);
            timer.callback();
        }    
    }

    private getNextTimer(): MockTimer | null {
        if (this.timers.size === 0) {
            return null;
        }

        let nextTimer: MockTimer | null = null;
        for (const timer of this.timers.values()) {
            if (!nextTimer || timer.fireTime < nextTimer.fireTime) {
                nextTimer = timer;
            }
        }
        return nextTimer;
    }

    getPendingTimersCount(): number {
        return this.timers.size;
    }

    getPendingTimers(): MockTimer[] {
        return Array.from(this.timers.values())
            .sort((a, b) => a.fireTime - b.fireTime);
    }

    reset(): void {
        this.currentTime = 0;
        this.timers.clear();
        this.nextTimerId = 1;
    }
}

export class SystemClock implements Clock {
    now(): number {
        return Date.now();
    }

    setTimeout(callback: () => void, delayMs: number): TimerHandle {
        return global.setTimeout(callback, delayMs);
    }

    clearTimeout(handle: TimerHandle): void {
        if (typeof handle === 'number') {
            throw new Error("Invalid timer handle type");
        }
        global.clearTimeout(handle);
    }
}
