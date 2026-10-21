import { useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import { Tooltip } from '@base-ui/react/tooltip'
import { byRoute } from './specs'
import { Map } from './Map'
import { SpecPage } from './SpecPage'
import './index.css'

// No client router: every link is a full page load, and the path picks the page.
const path = location.pathname.replace(/\/$/, '') || '/'
const spec = byRoute.get(path)
const kind = path === '/' ? 'map' : spec ? 'spec' : 'not-found'

const root = document.getElementById('root')!

function App() {
  // Set after commit so a reader of data-page (the e2e link walk) sees the
  // rendered page, not an empty root.
  useEffect(() => {
    root.dataset.page = kind
    document.title = spec ? `${spec.title} · ethp2p` : 'ethp2p'
  }, [])
  if (kind === 'map') return <Map />
  if (spec) return <SpecPage spec={spec} />
  return (
    <main className="px-6 py-10 md:px-12">
      <a href="/" className="font-display text-wordmark italic">ethp2p</a>
      <p className="mt-8 text-leaf">No page at {path}.</p>
    </main>
  )
}

createRoot(root).render(
  <Tooltip.Provider>
    <App />
  </Tooltip.Provider>,
)
