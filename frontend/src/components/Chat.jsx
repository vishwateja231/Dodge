import React, { useState, useRef, useEffect } from 'react';
import { sendQuery, sendQueryStream } from '../services/api';
import { Loader2, Send } from 'lucide-react';
import TableView from './TableView';

// Sub-component for rendering AI list messages with internal toggle state
const ListMessage = ({ p, msgId, onEntityDetect }) => {
    // Default table to open so user sees data immediately
    const [showTable, setShowTable] = useState(true);

    const handleShowInGraph = () => {
        if (onEntityDetect) {
            if (p.entities && p.entities.order_id) {
                onEntityDetect(p.entities);
            } else if (p.full_data && p.full_data.length > 0) {
                // Extract first-column values as IDs for graph
                const ids = p.full_data.map(r => Object.values(r)[0]).filter(Boolean);
                onEntityDetect(ids);
            }
        }
    };

    return (
        <div style={{ width: '100%', display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {/* Header: title + count */}
            <div style={{ display: 'flex', alignItems: 'baseline', gap: '8px' }}>
                <span style={{ fontWeight: '700', fontSize: '15px', color: 'var(--text)' }}>{p.title}</span>
                <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>{p.total} result{p.total !== 1 ? 's' : ''} found</span>
            </div>

            {/* Data table — shown by default */}
            {p.full_data && p.full_data.length > 0 && (
                <TableView data={p.full_data} />
            )}

            {/* Action buttons */}
            <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                <button
                    className="modern-btn"
                    onClick={() => setShowTable(!showTable)}
                >
                    {showTable ? 'Collapse Table' : 'Expand Table'}
                </button>
                <button
                    className="modern-btn primary"
                    onClick={handleShowInGraph}
                >
                    Show in Graph
                </button>
            </div>
        </div>
    );
};

export default function Chat({ onEntityDetect }) {
    const [messages, setMessages] = useState([
        {
            id: 'init',
            role: 'ai',
            type: 'text',
            content: 'Hi there. I am Dodge AI. Ask me anything about your orders, customers, or deliveries.',
        },
    ]);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const [loadingText, setLoadingText] = useState('Processing...');

    const scrollRef = useRef(null);
    const inputRef = useRef(null);

    const scrollToBottom = () => {
        if (scrollRef.current) {
            scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
        }
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages, loading]);

    const send = async (e) => {
        e?.preventDefault();
        const text = input.trim();
        if (!text || loading) return;

        setInput('');
        if (inputRef.current) inputRef.current.focus();

        const userMsg = { id: Date.now().toString(), role: 'user', type: 'text', content: text };
        setMessages((prev) => [...prev, userMsg]);
        setLoading(true);
        setLoadingText('Fetching data...');

        try {
            let data;
            try {
                data = await sendQueryStream(text, (status) => {
                    if (status) setLoadingText(status);
                });
            } catch {
                data = await sendQuery(text);
            }

            if (data.type === 'error' || data.error) {
                const errorMsg = data.error || data.answer || "Internal error";
                if (errorMsg.includes("LLM limit exceeded")) {
                    alert("LLM limit reached. Please try again later.");
                }
                setMessages((prev) => [...prev, { id: Date.now().toString(), role: 'ai', type: 'text', content: errorMsg }]);
            }
            else if (data.type === 'list' || data.type === 'graph') {
                setMessages((prev) => [...prev, {
                    id: Date.now().toString(),
                    role: 'ai',
                    type: 'list',
                    content: data
                }]);
            }
            else {
                setMessages((prev) => [...prev, { id: Date.now().toString(), role: 'ai', type: 'text', content: data.answer || "No matching data found." }]);
            }

            if ((data.type === 'list' || data.type === 'graph') && onEntityDetect) {
                if (data.graph && (data.graph.nodes?.length > 0)) {
                    onEntityDetect(data.graph);
                } else if (data.intent === 'trace_order' || data.entities?.order_id) {
                    onEntityDetect(data.entities);
                }
            }

        } catch (err) {
            setMessages((prev) => [...prev, { id: Date.now().toString(), role: 'ai', type: 'text', content: "Internal connection error. Please try again." }]);
        } finally {
            setLoading(false);
            setLoadingText('Processing...');
            if (inputRef.current) inputRef.current.focus();
        }
    };

    const handleInputKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            send();
        }
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: 'var(--bg)' }}>
            {/* Messages Scroll Area */}
            <div
                ref={scrollRef}
                style={{
                    flex: 1,
                    overflowY: 'auto',
                    padding: '24px',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '24px'
                }}
            >
                {messages.map((msg) => {
                    const isUser = msg.role === 'user';
                    return (
                        <div key={msg.id} style={{
                            display: 'flex',
                            width: '100%',
                            justifyContent: isUser ? 'flex-end' : 'flex-start'
                        }}>
                            {!isUser && (
                                <div style={{
                                    width: '32px', height: '32px', borderRadius: '8px',
                                    background: 'var(--card)', color: 'var(--text)', display: 'flex',
                                    alignItems: 'center', justifyContent: 'center',
                                    fontSize: '14px', fontWeight: 'bold', marginRight: '12px', flexShrink: 0,
                                    border: '1px solid var(--border)'
                                }}>
                                    D
                                </div>
                            )}

                            <div style={{
                                background: isUser ? 'var(--accent)' : 'var(--card)',
                                color: isUser ? '#ffffff' : 'var(--text)',
                                borderRadius: '12px',
                                padding: '14px 16px',
                                maxWidth: isUser ? '75%' : '85%',
                                fontSize: '14.5px',
                                lineHeight: '1.6',
                                boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                                borderTopRightRadius: isUser ? '4px' : '12px',
                                borderTopLeftRadius: !isUser ? '4px' : '12px',
                                border: !isUser ? '1px solid var(--border)' : 'none'
                            }}>
                                {msg.type === 'text' && <div>{msg.content}</div>}
                                {msg.type === 'list' && (
                                    <ListMessage p={msg.content} msgId={msg.id} onEntityDetect={onEntityDetect} />
                                )}
                            </div>
                        </div>
                    );
                })}

                {loading && (
                    <div style={{ display: 'flex', width: '100%', justifyContent: 'flex-start' }}>
                        <div style={{
                            width: '32px', height: '32px', borderRadius: '8px',
                            background: 'var(--card)', color: 'var(--text)', display: 'flex',
                            alignItems: 'center', justifyContent: 'center',
                            fontSize: '14px', fontWeight: 'bold', marginRight: '12px', flexShrink: 0,
                            border: '1px solid var(--border)'
                        }}>D</div>
                        <div style={{
                            background: 'var(--card)', color: 'var(--text)', borderRadius: '12px', padding: '14px 16px',
                            borderTopLeftRadius: '4px', border: '1px solid var(--border)', display: 'flex', alignItems: 'center', gap: '8px'
                        }}>
                            <Loader2 size={16} className="animate-spin text-slate-400" />
                            <span style={{ color: 'var(--text-muted)', fontSize: '14px' }}>{loadingText}</span>
                        </div>
                    </div>
                )}
            </div>

            {/* Input Fixed at Bottom */}
            <div style={{
                padding: '20px 24px',
                background: 'var(--bg)',
                borderTop: '1px solid var(--border)'
            }}>
                <form
                    onSubmit={send}
                    style={{
                        display: 'flex',
                        background: 'var(--card)',
                        border: '1px solid var(--border)',
                        borderRadius: '24px',
                        padding: '6px 6px 6px 20px',
                        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.2)',
                        alignItems: 'center'
                    }}
                >
                    <textarea
                        ref={inputRef}
                        placeholder="Message Dodge AI..."
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyDown={handleInputKeyDown}
                        disabled={loading}
                        rows={1}
                        style={{
                            flex: 1,
                            background: 'transparent',
                            border: 'none',
                            color: 'var(--text)',
                            fontSize: '15px',
                            outline: 'none',
                            resize: 'none',
                            minHeight: '24px',
                            maxHeight: '120px',
                            lineHeight: '1.4',
                            paddingTop: '6px',
                            paddingBottom: '6px'
                        }}
                    />
                    <button
                        type="submit"
                        disabled={!input.trim() || loading}
                        style={{
                            width: '36px',
                            height: '36px',
                            borderRadius: '50%',
                            background: (!input.trim() || loading) ? 'var(--border)' : 'var(--accent)',
                            color: '#fff',
                            border: 'none',
                            cursor: (!input.trim() || loading) ? 'not-allowed' : 'pointer',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            transition: 'background 0.2s'
                        }}
                    >
                        <Send size={16} style={{ marginLeft: '-2px' }} />
                    </button>
                </form>
            </div>

            <style>{`
                .modern-btn {
                    background: var(--border);
                    color: var(--text);
                    border: none;
                    padding: 6px 12px;
                    border-radius: 6px;
                    font-size: 13px;
                    font-weight: 500;
                    cursor: pointer;
                    transition: background 0.2s;
                }
                .modern-btn:hover {
                    background: var(--surface-hover);
                }
                .modern-btn.primary {
                    background: var(--accent);
                    color: #ffffff;
                }
                .modern-btn.primary:hover {
                    background: var(--brand); /* use brand for hover brightness */
                }
                
                /* Override scrollbar */
                ::-webkit-scrollbar { width: 8px; height: 8px; }
                ::-webkit-scrollbar-track { background: transparent; }
                ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
                ::-webkit-scrollbar-thumb:hover { background: var(--surface-hover); }

                /* Prevent override on pure white text inside the indigo bubble */
                .text-white { color: #ffffff !important; }
            `}</style>
        </div>
    );
}
