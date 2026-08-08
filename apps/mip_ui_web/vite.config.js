import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    // Bind IPv4 loopback — default Vite on Windows can listen only on [::1], breaking http://127.0.0.1:5173
    host: '127.0.0.1',
    port: 5173,
    strictPort: true,
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
