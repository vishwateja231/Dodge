import React, { useState, useEffect } from 'react';
import GraphView from './components/GraphView';
import Chat from './components/Chat';
import { Moon, Sun } from 'lucide-react';
import './App.css';

export default function App() {
  const [graphHighlight, setGraphHighlight] = useState(null);

  // Initialize theme from localStorage or default to 'dark'
  const [theme, setTheme] = useState(() => localStorage.getItem('theme') || 'dark');

  useEffect(() => {
    // Apply theme strictly to body element
    document.body.className = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  const toggleTheme = () => {
    setTheme(prev => prev === 'dark' ? 'light' : 'dark');
  };

  const handleEntityDetect = (entities) => {
    if (Array.isArray(entities)) {
      setGraphHighlight(entities);
    } else if (entities && entities.order_id) {
      setGraphHighlight([entities.order_id]);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="brand">
          <div className="brand-icon">D</div>
          <div>
            <div className="brand-text">Dodge AI</div>
            <div className="brand-tagline">Navigate your data. Instantly.</div>
          </div>
        </div>

        {/* Theme Toggle Button */}
        <button
          onClick={toggleTheme}
          style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
            color: 'var(--text)',
            borderRadius: '50%',
            width: '40px',
            height: '40px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            cursor: 'pointer',
            transition: 'all 0.2s ease'
          }}
          title={`Switch to ${theme === 'dark' ? 'Light' : 'Dark'} Mode`}
        >
          {theme === 'dark' ? <Sun size={20} /> : <Moon size={20} />}
        </button>
      </header>

      <main className="app-body">
        <div className="pane pane-graph">
          <GraphView externalOrderQuery={graphHighlight} onClearExternal={() => setGraphHighlight(null)} />
        </div>
        <div className="pane pane-chat">
          <Chat onEntityDetect={handleEntityDetect} />
        </div>
      </main>
    </div>
  );
}
