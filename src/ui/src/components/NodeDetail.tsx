import { useRaftStore } from "../store/raftStore";

export function NodeDetail() {
    const selectedNodeId = useRaftStore((state) => state.selectedNodeId);
    const selectNode = useRaftStore((state) => state.selectNode);
    const nodes = useRaftStore((state) => state.nodes);
    const sendCommand = useRaftStore((state) => state.sendCommand);

    const node = selectedNodeId ? nodes[selectedNodeId] : null;
    if (!node) return null;

    return (
        <div style={{
            position: 'absolute',
            top: 0, right: 0, bottom: 0,
            width: 280,
            background: '#161b22',
            borderLeft: '1px solid #30363d',
            padding: '16px',
            display: 'flex',
            flexDirection: 'column',
            gap: 12,
            zIndex: 10,
        }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontFamily: 'monospace', fontWeight: 700, fontSize: 16 }}>
                    {node.nodeId}
                </span>
                <button onClick={() => selectNode(null)}
                    style={{ background: 'none', color: '#8b949e', fontSize: 18, cursor: 'pointer' }}>
                    ×
                </button>
            </div>

            <Row label="Role"         value={node.role} />
            <Row label="Term"         value={String(node.term)} />
            <Row label="Commit index" value={String(node.commitIndex)} />
            <Row label="Voted for"    value={node.votedFor ?? '—'} />
            <Row label="Crashed"      value={node.crashed ? 'yes' : 'no'} />

            <div style={{ borderTop: '1px solid #30363d', paddingTop: 12, marginTop: 4 }}>
                {node.crashed ? (
                    <button
                        onClick={() => sendCommand({ type: 'RecoverNode', nodeId: node.nodeId })}
                        style={{
                            width: '100%',
                            padding: '8px',
                            background: 'transparent',
                            border: '1px solid #2ea043',
                            color: '#2ea043',
                            borderRadius: 6,
                            fontFamily: 'monospace',
                            fontSize: 12,
                            cursor: 'pointer',
                        }}
                    >
                        recover
                    </button>
                ) : (
                    <button
                        onClick={() => sendCommand({ type: 'CrashNode', nodeId: node.nodeId })}
                        style={{
                            width: '100%',
                            padding: '8px',
                            background: 'transparent',
                            border: '1px solid #d73a49',
                            color: '#d73a49',
                            borderRadius: 6,
                            fontFamily: 'monospace',
                            fontSize: 12,
                            cursor: 'pointer',
                        }}
                    >
                        crash
                    </button>
                )}
            </div>
        </div>
    );
}

function Row({ label, value }: { label: string; value: string }) {
    return (
        <div style={{ display: 'flex', justifyContent: 'space-between', fontFamily: 'monospace', fontSize: 13 }}>
            <span style={{ color: '#8b949e' }}>{label}</span>
            <span style={{ color: '#e6edf3' }}>{value}</span>
        </div>
    );
}
