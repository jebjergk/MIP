import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ExplainModeProvider } from './context/ExplainModeContext'
import { ExplainCenterProvider } from './context/ExplainCenterContext'
import App from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <BrowserRouter>
      <ExplainModeProvider defaultOn={true}>
        <ExplainCenterProvider>
          <App />
        </ExplainCenterProvider>
      </ExplainModeProvider>
    </BrowserRouter>
  </React.StrictMode>,
)
