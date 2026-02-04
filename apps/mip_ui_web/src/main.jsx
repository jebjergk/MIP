import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ExplainModeProvider } from './context/ExplainModeContext'
import { ExplainCenterProvider } from './context/ExplainCenterContext'
import { PortfolioProvider } from './context/PortfolioContext'
import { FreshnessProvider } from './context/FreshnessContext'
import App from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <BrowserRouter>
      <FreshnessProvider>
        <ExplainModeProvider defaultOn={true}>
          <ExplainCenterProvider>
            <PortfolioProvider>
              <App />
            </PortfolioProvider>
          </ExplainCenterProvider>
        </ExplainModeProvider>
      </FreshnessProvider>
    </BrowserRouter>
  </React.StrictMode>,
)
