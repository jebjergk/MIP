import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { PortfolioProvider } from './context/PortfolioContext'
import { SymbolMetaProvider } from './context/SymbolMetaContext'
import App from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <BrowserRouter>
      <PortfolioProvider>
        <SymbolMetaProvider>
          <App />
        </SymbolMetaProvider>
      </PortfolioProvider>
    </BrowserRouter>
  </React.StrictMode>,
)
