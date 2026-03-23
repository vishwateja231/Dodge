import React, { useState } from 'react';

export default function TableView({ data }) {
    const [visibleCount, setVisibleCount] = useState(5);

    if (!data || data.length === 0) {
        return <div style={{ marginTop: '10px', fontSize: '13px', color: 'var(--text-muted)' }}>No data available.</div>;
    }

    const cols = Object.keys(data[0]);
    const visibleData = data.slice(0, visibleCount);
    const hasMore = visibleCount < data.length;

    return (
        <div style={{
            marginTop: '12px',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            overflow: 'hidden',
            background: 'var(--bg)'
        }}>
            <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                <table style={{
                    width: '100%',
                    textAlign: 'left',
                    fontSize: '13px',
                    color: 'var(--text)',
                    borderCollapse: 'collapse'
                }}>
                    <thead style={{
                        background: 'var(--card)',
                        position: 'sticky',
                        top: 0,
                        zIndex: 1
                    }}>
                        <tr>
                            {cols.map(c => (
                                <th key={c} style={{
                                    padding: '10px 14px',
                                    borderBottom: '1px solid var(--border)',
                                    fontWeight: '600',
                                    whiteSpace: 'nowrap'
                                }}>
                                    {c.replace(/_/g, ' ')}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {visibleData.map((row, i) => (
                            <tr key={i} className="hover-row" style={{
                                borderBottom: '1px solid var(--border)',
                                transition: 'background 0.2s'
                            }}>
                                {cols.map(c => (
                                    <td key={c + i} style={{ padding: '10px 14px' }}>
                                        {row[c]}
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            {hasMore && (
                <button
                    className="load-more-btn"
                    onClick={() => setVisibleCount(prev => prev + 5)}
                    style={{
                        width: '100%',
                        background: 'transparent',
                        border: 'none',
                        padding: '10px',
                        color: 'var(--accent)',
                        cursor: 'pointer',
                        fontWeight: '500',
                        fontSize: '13px',
                        borderTop: '1px solid var(--border)',
                        transition: 'background 0.2s'
                    }}
                >
                    Load More ({data.length - visibleCount} remaining)
                </button>
            )}

            <style>{`
                .hover-row:hover {
                    background: var(--surface-hover) !important;
                }
                .load-more-btn:hover {
                    background: var(--card) !important;
                }
            `}</style>
        </div>
    );
}
