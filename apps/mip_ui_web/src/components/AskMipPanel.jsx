import { useState, useRef, useEffect, useCallback } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { sectionForRoute } from '../guide/index'
import { API_BASE } from '../App'
import './AskMipPanel.css'

/**
 * Slide-over panel for "Ask MIP" — shows contextual guide content
 * and provides a chat interface powered by Cortex COMPLETE.
 */
export default function AskMipPanel({ open, onClose, pathname }) {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [guideExpanded, setGuideExpanded] = useState(false)
  const chatEndRef = useRef(null)
  const inputRef = useRef(null)

  // Resolve the guide section for the current page
  const guideSection = sectionForRoute(pathname)

  // Auto-scroll to latest message
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // Focus input when panel opens
  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 300)
    }
  }, [open])

  const sendMessage = useCallback(async () => {
    const question = input.trim()
    if (!question || loading) return

    const userMsg = { role: 'user', content: question }
    setMessages((prev) => [...prev, userMsg])
    setInput('')
    setLoading(true)

    try {
      // Build history (last 10 messages for context window management)
      const history = [...messages, userMsg].slice(-10).map(({ role, content }) => ({ role, content }))

      const res = await fetch(`${API_BASE}/ask`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question,
          route: pathname || null,
          history,
        }),
      })

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: 'Request failed' }))
        throw new Error(err.detail || `HTTP ${res.status}`)
      }

      const data = await res.json()
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: data.answer || 'No response received.' },
      ])
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: `Sorry, something went wrong: ${err.message}`, error: true },
      ])
    } finally {
      setLoading(false)
    }
  }, [input, loading, messages, pathname])

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const clearChat = () => {
    setMessages([])
    setInput('')
  }

  return (
    <div className={`ask-mip-panel ${open ? 'ask-mip-panel--open' : ''}`} role="dialog" aria-label="Ask MIP">
      {/* ── Header ── */}
      <div className="ask-mip-panel-header">
        <h2 className="ask-mip-panel-title">Ask MIP</h2>
        <div className="ask-mip-panel-header-actions">
          {messages.length > 0 && (
            <button type="button" className="ask-mip-clear-btn" onClick={clearChat} title="Clear conversation">
              Clear
            </button>
          )}
          <button type="button" className="ask-mip-close-btn" onClick={onClose} aria-label="Close panel">
            &times;
          </button>
        </div>
      </div>

      {/* ── Guide Context Card ── */}
      {guideSection && (
        <div className="ask-mip-guide-card">
          <button
            type="button"
            className="ask-mip-guide-toggle"
            onClick={() => setGuideExpanded((v) => !v)}
            aria-expanded={guideExpanded}
          >
            <span className="ask-mip-guide-toggle-icon">{guideExpanded ? '\u25BC' : '\u25B6'}</span>
            <span className="ask-mip-guide-toggle-label">
              Guide: {guideSection.title}
            </span>
          </button>
          {guideExpanded && (
            <div className="ask-mip-guide-content">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {guideSection.markdown}
              </ReactMarkdown>
            </div>
          )}
        </div>
      )}

      {/* ── Chat Messages ── */}
      <div className="ask-mip-chat">
        {messages.length === 0 && !loading && (
          <div className="ask-mip-empty">
            <p className="ask-mip-empty-title">Ask me anything about MIP</p>
            <p className="ask-mip-empty-hint">
              How is z-score used? What is watch mode? Explain the training flow.
              What does the Cockpit show? How are trade proposals generated?
            </p>
          </div>
        )}
        {messages.map((msg, i) => (
          <div
            key={i}
            className={`ask-mip-msg ask-mip-msg--${msg.role} ${msg.error ? 'ask-mip-msg--error' : ''}`}
          >
            <div className="ask-mip-msg-label">{msg.role === 'user' ? 'You' : 'MIP'}</div>
            <div className="ask-mip-msg-body">
              {msg.role === 'assistant' ? (
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
              ) : (
                <p>{msg.content}</p>
              )}
            </div>
          </div>
        ))}
        {loading && (
          <div className="ask-mip-msg ask-mip-msg--assistant ask-mip-msg--loading">
            <div className="ask-mip-msg-label">MIP</div>
            <div className="ask-mip-msg-body">
              <span className="ask-mip-typing">
                <span /><span /><span />
              </span>
            </div>
          </div>
        )}
        <div ref={chatEndRef} />
      </div>

      {/* ── Input Area ── */}
      <div className="ask-mip-input-area">
        <textarea
          ref={inputRef}
          className="ask-mip-input"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask a question about MIP..."
          rows={2}
          disabled={loading}
        />
        <button
          type="button"
          className="ask-mip-send-btn"
          onClick={sendMessage}
          disabled={loading || !input.trim()}
          aria-label="Send message"
        >
          Send
        </button>
      </div>
    </div>
  )
}
