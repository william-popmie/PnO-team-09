import { Clock, TimerHandle } from "./Clock";
import { Random } from "../util/Random";
import { TimerManagerError } from "../util/Error";
import { Logger } from "../util/Logger";

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

export class TimerManager implements TimerManagerIInterface {
    private electionTimer: TimerHandle | null = null;
    private heartbeatTimer: TimerHandle | null = null;
    private electionCallback: (() => void) | null = null;
    private heartbeatCallback: (() => void) | null = null;
    private static readonly minRatio = 2;
    private static readonly maxHeartBeats = Number.MAX_SAFE_INTEGER;
    private heartbeatCount = 0;

    constructor(private clock: Clock, private random: Random, private logger: Logger, private config: TimerConfig) {
        this.validateConfig(config);
    }

    startElectionTimer(callback: () => void): void {
        this.stopElectionTimer();
        this.electionCallback = callback;

        const timeoutMs = this.getRandomElectionTimeout();

        this.electionTimer = this.clock.setTimeout(() => {
            this.electionTimer = null;
            try { 
                this.electionCallback?.();
            } catch (error) {
                this.logger.error(`Error in election timer callback`, error as Error);
            }
        }, timeoutMs);
    }

    resetElectionTimer(): void {
        if (!this.electionCallback) {
            return;
        }

        this.startElectionTimer(this.electionCallback);
    }

    stopElectionTimer(): void {
        if (this.electionTimer !== null) {
            this.clock.clearTimeout(this.electionTimer);
            this.electionTimer = null;
        }
    }

    isElectionTimerActive(): boolean {
        return this.electionTimer !== null;
    }

    startHeartbeatTimer(callback: () => void): void {
        this.stopHeartbeatTimer();
        this.heartbeatCallback = callback;
        this.heartbeatCount = 0;
        this.scheduleNextHeartbeat();
    }

    stopHeartbeatTimer(): void {
        if (this.heartbeatTimer !== null) {
            this.clock.clearTimeout(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        this.heartbeatCallback = null;
    }

    isHeartbeatTimerActive(): boolean {
        return this.heartbeatTimer !== null;
    }

    stopAllTimers(): void {
        this.stopElectionTimer();
        this.stopHeartbeatTimer();
        this.electionCallback = null;
        this.heartbeatCallback = null;
        this.heartbeatCount = 0;
    }

    getElectionTimeoutRange(): { min: number; max: number } {
        return {
            min: this.config.electionTimeoutMin,
            max: this.config.electionTimeoutMax
        };
    }

    getHeartbeatInterval(): number {
        return this.config.heartbeatInterval;
    }

    private validateConfig(config: TimerConfig) {
        if (!Number.isInteger(config.electionTimeoutMin) || config.electionTimeoutMin <= 0) {
            throw new TimerManagerError(`Invalid electionTimeoutMin: ${config.electionTimeoutMin}. Must be > 0.`);
        }

        if (!Number.isInteger(config.electionTimeoutMax) || config.electionTimeoutMax < config.electionTimeoutMin) {
            throw new TimerManagerError(`Invalid election timeout range: max (${config.electionTimeoutMax}) must be >= min (${config.electionTimeoutMin}).`);
        }

        if (!Number.isInteger(config.heartbeatInterval) || config.heartbeatInterval <= 0) {
            throw new TimerManagerError(`Invalid heartbeatInterval: ${config.heartbeatInterval}. Must be > 0.`);
        }

        if (config.electionTimeoutMin < TimerManager.minRatio * config.heartbeatInterval) {
            throw new TimerManagerError(`Election timeout min (${config.electionTimeoutMin}) must be at least ${TimerManager.minRatio} times the heartbeat interval (${config.heartbeatInterval}).`);
        }

        /* can never happen
        if (config.electionTimeoutMax < TimerManager.minRatio * config.heartbeatInterval) {
            throw new TimerManagerError(`Election timeout max (${config.electionTimeoutMax}) must be at least ${TimerManager.minRatio} times the heartbeat interval (${config.heartbeatInterval}).`);
        }
        */
    }

    private getRandomElectionTimeout(): number {
        const { min, max } = this.getElectionTimeoutRange();
        return this.random.nextInt(min, max);
    }

    private scheduleNextHeartbeat(): void {
        /* can never happen
        if (!this.heartbeatCallback) {
            return;
        }
        */

        /*
        if (this.heartbeatCount >= TimerManager.maxHeartBeats) {
            this.logger.warn(`Reached maximum heartbeat count (${TimerManager.maxHeartBeats}). Stopping heartbeat timer to prevent potential issues.`);
            this.stopHeartbeatTimer();
            return;
        }

        this.heartbeatCount++;
        */

        this.heartbeatTimer = this.clock.setTimeout(() => {
            try {
                this.heartbeatCallback?.();
            } catch (error) {
                this.logger.error(`Error in heartbeat timer callback`, error as Error);
            } finally {
                this.scheduleNextHeartbeat();
            }
        }, this.getHeartbeatInterval());
    }
}