export interface TimerConfig{
    electionTimeoutMin: number;
    electionTimeoutMax: number;
    heartbeatInterval: number;
}

export interface TimerManagerIInterface {
    startElectionTimer(callback: () => void): void;
    resetElectionTimer(): void;
    stopElectionTimer(): void;
    isElectionTimerActive(): boolean;
    startHeartbeatTimer(callback: () => void): void;
    stopHeartbeatTimer(): void;
    isHeartbeatTimerActive(): boolean;
    stopAllTimers(): void;
    getElectionTimeoutRange(): { min: number; max: number };
    getHeartbeatInterval(): number;
}