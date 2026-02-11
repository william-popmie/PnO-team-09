import { describe, it, expect } from "vitest";
import { validateConfig, createConfig } from "./Config";

describe('Config.ts, validateConfig', () => {

    const validConfig = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig1 = {
        nodeId: "",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig2 = {
        nodeId: 123 as any,
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig3 = {
        nodeId: "node1",
        peerIds: "not an array" as any,
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig4 = {
        nodeId: "node1",
        peerIds: ["node2", 123 as any],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig5 = {
        nodeId: "node1",
        peerIds: ["node1", "node2"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig6 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: "not an integer" as any,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig7 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: -150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig8 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: "not an integer" as any,
        heartbeatIntervalMs: 50
    };

    const invalidConfig9 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: -300,
        heartbeatIntervalMs: 50
    };

    const invalidConfig10 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 300,
        electionTimeoutMaxMs: 150,
        heartbeatIntervalMs: 50
    };

    const invalidConfig11 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: "not an integer" as any
    };

    const invalidConfig12 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: -50
    };

    const invalidConfig13 = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 100,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    it('should validate a correct config', () => {
        expect(() => validateConfig(validConfig)).not.toThrow();
    });

    it('should throw error for empty nodeId', () => {
        expect(() => validateConfig(invalidConfig1)).toThrow("Invalid nodeId: . nodeId must be a non-empty string.");
    });

    it('should throw for a node id that is not a string', () => {
        expect(() => validateConfig(invalidConfig2)).toThrow("Invalid nodeId: 123. nodeId must be a non-empty string.");
    });

    it('should throw error for non array peerIds', () => {
        expect(() => validateConfig(invalidConfig3)).toThrow("Invalid peerIds: not an array. peerIds must be an array of strings.");
    });

    it ('should throw error if peerIds includes a non string', () => {
        expect(() => validateConfig(invalidConfig4)).toThrow("Invalid peerIds: node2,123. peerIds must be an array of strings.");
    });

    it('should throw error if peerIds includes nodeId', () => {
        expect(() => validateConfig(invalidConfig5)).toThrow("Invalid peerIds: node1,node2. peerIds cannot include the nodeId.");
    });

    it('should throw error for non integer electionTimeoutMinMs', () => {
        expect(() => validateConfig(invalidConfig6)).toThrow("Invalid electionTimeoutMinMs: not an integer. electionTimeoutMinMs must be a positive integer.");
    });

    it('should throw error for non positive electionTimeoutMinMs', () => {
        expect(() => validateConfig(invalidConfig7)).toThrow("Invalid electionTimeoutMinMs: -150. electionTimeoutMinMs must be a positive integer.");
    });

    it('should throw error for non integer electionTimeoutMaxMs', () => {
        expect(() => validateConfig(invalidConfig8)).toThrow("Invalid electionTimeoutMaxMs: not an integer. electionTimeoutMaxMs must be a positive integer.");
    });

    it('should throw error for non positive electionTimeoutMaxMs', () => {
        expect(() => validateConfig(invalidConfig9)).toThrow("Invalid electionTimeoutMaxMs: -300. electionTimeoutMaxMs must be a positive integer.");
    });

    it('should throw error if electionTimeoutMinMs is greater than electionTimeoutMaxMs', () => {
        expect(() => validateConfig(invalidConfig10)).toThrow("Invalid election timeout range: min 300 ms must be less than max 150 ms.");
    });

    it('should throw error for non integer heartbeatIntervalMs', () => {
        expect(() => validateConfig(invalidConfig11)).toThrow("Invalid heartbeatIntervalMs: not an integer. heartbeatIntervalMs must be a positive integer.");
    });

    it('should throw error for non positive heartbeatIntervalMs', () => {
        expect(() => validateConfig(invalidConfig12)).toThrow("Invalid heartbeatIntervalMs: -50. heartbeatIntervalMs must be a positive integer.");
    });

    it('should throw error if electionTimeoutMinMs is less than three times heartbeatIntervalMs', () => {
        expect(() => validateConfig(invalidConfig13)).toThrow("Invalid electionTimeoutMinMs: 100. electionTimeoutMinMs must be at least three times the heartbeatIntervalMs: 50.");
    });
});

describe('Config.ts, createConfig', () => {
    const validConfigParams = {
        nodeId: "node1",
        peerIds: ["node2", "node3"],
        electionTimeoutMinMs: 150,
        electionTimeoutMaxMs: 300,
        heartbeatIntervalMs: 50
    };

    it('should create a valid config', () => {
        const config = createConfig(
            validConfigParams.nodeId,
            validConfigParams.peerIds,
            validConfigParams.electionTimeoutMinMs,
            validConfigParams.electionTimeoutMaxMs,
            validConfigParams.heartbeatIntervalMs
        );
        expect(config).toEqual(validConfigParams);
    });

    it('should throw error for invalid config parameters', () => {
        expect(() => createConfig(
            "",
            validConfigParams.peerIds,
            validConfigParams.electionTimeoutMinMs,
            validConfigParams.electionTimeoutMaxMs,
            validConfigParams.heartbeatIntervalMs
        )).toThrow("Invalid nodeId: . nodeId must be a non-empty string.");
    });
});
