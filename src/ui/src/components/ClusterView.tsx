import { useRaftStore } from "../store/raftStore";

const roleColors: Record<string, string> = {
    "Leader": "#2ea043",
    "Follower": "#0366d6",
    "Candidate": "#d73a49",
};

function computePositionsCircle(nodeIds: string[], radius: number, centerX: number, centerY: number) {
    const positions: Record<string, { x: number; y: number }> = {};
    nodeIds.forEach((id, index) => {
        const angle = (2 * Math.PI * index) / nodeIds.length - Math.PI / 2;
        positions[id] = {
            x: centerX + radius * Math.cos(angle),
            y: centerY + radius * Math.sin(angle),
        };
    });
    return positions;
}

const width = 800;
const height = 600;
const nodeRadius = 30;

export function ClusterView() {
    const nodeIds = useRaftStore((state) => state.nodeIds);
    const nodes = useRaftStore((state) => state.nodes);
    const positions = computePositionsCircle(nodeIds, 200, width / 2, height / 2);
    return (
        <svg width={width} height={height} style={{ background: '#0d1117', display: 'block' }}>
            {nodeIds.map(id => {
                const { x, y } = positions[id];
                const node = nodes[id];
                const color = node ? roleColors[node.role] : "#161b22";
                return (
                    <g key={id}>
                        <circle cx={x} cy={y} r={nodeRadius} fill="#161b22" stroke={color} strokeWidth={2} />
                        <text x={x} y={y - 10} textAnchor="middle" dominantBaseline="middle" fill="#e6edf3" fontSize={12} fontFamily="monospace">
                            {id}
                        </text>
                        <text x={x} y={y + 5} textAnchor="middle" dominantBaseline="middle" fill={color} fontSize={9} fontFamily="monospace">
                            {node?.role ?? '—'}
                        </text>
                    </g>
                );
            })}
        </svg>
    );
}