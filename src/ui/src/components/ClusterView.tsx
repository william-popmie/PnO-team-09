import { useEffect, useRef, useState } from "react";
import { useRaftStore } from "../store/raftStore";
import { MessageLayer } from "./MessageLayer";

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

const nodeRadius = 30;

export function ClusterView() {
    const nodeIds = useRaftStore((state) => state.nodeIds);
    const nodes = useRaftStore((state) => state.nodes);

    const selectNode = useRaftStore((state) => state.selectNode);
    const selectedNodeId = useRaftStore((state) => state.selectedNodeId);

    const containerRef = useRef<HTMLDivElement>(null);
    const [ size, setSize ] = useState({ width: 800, height: 600 });

    useEffect(() => {
        const element = containerRef.current;
        if (!element) return;
        const ro = new ResizeObserver(entries => {
            const { width, height } = entries[0].contentRect;
            setSize({ width, height });
        });
        ro.observe(element);
        return () => ro.disconnect();
    }, []);

    const cx = size.width / 2;
    const cy = size.height / 2;
    const radius = Math.min(cx, cy) * 0.6;
    const positions = computePositionsCircle(nodeIds, radius, cx, cy);

    return (
        <div ref={containerRef} style={{ position: 'relative', width: '100%', height: '100%' }}>
            <svg width={size.width} height={size.height} style={{ background: '#0d1117', display: 'block' }}>
                {nodeIds.map(id => {
                    const { x, y } = positions[id];
                    const node = nodes[id];
                    const color = node ? roleColors[node.role] : "#161b22";
                    return (
                        <g key={id} onClick={() => selectNode(selectedNodeId === id ? null : id)} style={{ cursor: 'pointer' }}>
                            <circle cx={x} cy={y} r={nodeRadius} fill="#161b22" stroke={color} strokeWidth={2} />
                            <text x={x} y={y - 10} textAnchor="middle" dominantBaseline="middle" fill="#e6edf3" fontSize={12} fontFamily="monospace">
                                {id}
                            </text>
                            <text x={x} y={y + 5} textAnchor="middle" dominantBaseline="middle" fill={color} fontSize={9} fontFamily="monospace">
                                {node?.role ?? '—'}
                            </text>

                            {selectedNodeId === id && (
                                <circle cx={x} cy={y} r={nodeRadius + 6} fill="none" stroke="#e6edf3" strokeWidth={2} />
                            )}
                        </g>
                    );
                })}
            </svg>
            <MessageLayer positions={positions} nodeRadius={nodeRadius} width={size.width} height={size.height} />
            <svg style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }} width={size.width} height={size.height}>
                {nodeIds.flatMap((a, i) =>
                    nodeIds.slice(i + 1).map(b => {
                        const pa = positions[a];
                        const pb = positions[b];
                        return (
                            <line key={`${a}-${b}`}
                                x1={pa.x} y1={pa.y}
                                x2={pb.x} y2={pb.y}
                                stroke="#ffffff0a"
                                strokeWidth={1}
                            />
                        );
                    })
                )}
            </svg>
        </div>
    );
}