import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // /api/* → http://127.0.0.1:8000/* (strip /api prefix for FastAPI)
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
        // Bootstrap / Snowflake can be slow; API reload drops sockets — avoid premature proxy timeouts
        timeout: 300000,
        proxyTimeout: 300000,
      },
    },
  },
})
