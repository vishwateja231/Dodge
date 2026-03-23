import React from 'react';
import { X } from 'lucide-react';

export default function NodeDetailsCard({ nodeData, onClose }) {
    if (!nodeData) return null;

    return (
        <div className="node-details-card">
            <div className="details-header">
                <div>
                    <div className="details-subtitle">{nodeData.type.toUpperCase()} ID</div>
                    <div className="details-title">{nodeData.id}</div>
                </div>
                <button className="close-btn" onClick={onClose} aria-label="Close details">
                    <X size={18} />
                </button>
            </div>

            <div className="details-grid">
                {Object.entries(nodeData.metadata || {}).map(([key, val]) => (
                    (val !== null && val !== undefined && val !== '') && (
                        <div className="details-row" key={key}>
                            <span className="details-lbl">{key.replace(/_/g, ' ')}</span>
                            <span className="details-val">{String(val)}</span>
                        </div>
                    )
                ))}
            </div>
        </div>
    );
}
