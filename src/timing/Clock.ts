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