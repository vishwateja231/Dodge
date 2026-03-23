import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Proxy all /api/* requests to FastAPI at localhost:8000
// This also eliminates all CORS issues since requests go same-origin
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/orders': { target: 'http://localhost:8000', changeOrigin: true },
      '/customers': { target: 'http://localhost:8000', changeOrigin: true },
      '/products': { target: 'http://localhost:8000', changeOrigin: true },
      '/analytics': { target: 'http://localhost:8000', changeOrigin: true },
      '/query': { target: 'http://localhost:8000', changeOrigin: true },
    },
  },
})
