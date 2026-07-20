interface Props {
  latest: any
  driftEvents: any[]
}

export default function StatusBar({ latest, driftEvents }: Props) {
  if (!latest) return (
    <div className="status-bar status-bar--loading">
      <span className="mono">Waiting for data...</span>
    </div>
  )

  const isAnomaly = latest.is_anomaly
  const totalDrifts = driftEvents.length

  return (
    <div className="status-bar">
      <div className="status-bar__left">
        <span className="status-bar__title mono">STREAMSENTINEL</span>
        <span className="status-bar__stream mono">{latest.stream_id}</span>
      </div>
      <div className="status-bar__right">
        <div className={`status-indicator ${isAnomaly ? 'status-indicator--anomaly' : 'status-indicator--normal'}`}>
          <span className="status-indicator__dot" />
          <span className="mono">{isAnomaly ? 'ANOMALY' : 'NORMAL'}</span>
        </div>
        <div className="status-stat">
          <span className="status-stat__label">MODEL GEN</span>
          <span className="status-stat__value mono">{latest.model_generation}</span>
        </div>
        <div className="status-stat">
          <span className="status-stat__label">DRIFTS</span>
          <span className="status-stat__value mono">{totalDrifts}</span>
        </div>
        <div className="status-stat">
          <span className="status-stat__label">E2E LATENCY</span>
          <span className="status-stat__value mono">{latest.e2e_latency_ms.toFixed(1)}ms</span>
        </div>
      </div>
    </div>
  )
}