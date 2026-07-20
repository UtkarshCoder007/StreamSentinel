import { useState, useEffect } from 'react'
import StatusBar from './components/StatusBar'
import ScoreChart from './components/ScoreChart'
import LatencyPanel from './components/LatencyPanel'
import MetricsPanel from './components/MetricsPanel'
import DriftTimeline from './components/DriftTimeline'
import './App.css'

const STREAM_ID = 'realAWSCloudwatch/ec2_cpu_utilization_5f5533'
const API_BASE = 'http://localhost:8000'
const POLL_INTERVAL = 2000

export default function App() {
  const [latest, setLatest] = useState<any>(null)
  const [history, setHistory] = useState<any[]>([])
  const [driftEvents, setDriftEvents] = useState<any[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchAll = async () => {
      try {
        const base = `${API_BASE}/stream/${STREAM_ID}`
        const [latestRes, historyRes, driftRes] = await Promise.all([
          fetch(`${base}/latest`),
          fetch(`${base}/history`),
          fetch(`${base}/drift`),
        ])
        if (!latestRes.ok) throw new Error(`/latest returned ${latestRes.status}`)
        const [latestData, historyData, driftData] = await Promise.all([
          latestRes.json(),
          historyRes.json(),
          driftRes.json(),
        ])
        setLatest(latestData)
        setHistory(historyData.results ?? [])
        setDriftEvents(driftData.events ?? [])
        setError(null)
      } catch (e: any) {
        setError(e.message)
      }
    }

    fetchAll()
    const id = setInterval(fetchAll, POLL_INTERVAL)
    return () => clearInterval(id)
  }, [])

  return (
    <div className="app">
      <StatusBar latest={latest} driftEvents={driftEvents} />
      {error && (
        <div className="error-banner mono">
          ⚠ {error} — Is the backend running? (uvicorn app.main:app --reload)
        </div>
      )}
      <div className="dashboard">
        <div className="dashboard__main">
          <ScoreChart history={history} driftEvents={driftEvents} />
        </div>
        <div className="dashboard__side">
          <MetricsPanel latest={latest} history={history} />
          <LatencyPanel history={history} />
          <DriftTimeline driftEvents={driftEvents} />
        </div>
      </div>
    </div>
  )
}