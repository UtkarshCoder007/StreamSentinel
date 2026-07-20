interface Props {
  latest: any
  history: any[]
}

export default function MetricsPanel({ latest, history }: Props) {
  const anomalyCount = history.filter(h => h.is_anomaly).length
  const anomalyRate = history.length
    ? ((anomalyCount / history.length) * 100).toFixed(1)
    : null

  return (
    <div className="panel">
      <div className="panel__header">
        <span className="panel__title">METRICS</span>
      </div>
      <div className="stat-grid">
        <div className="stat-cell">
          <span className="stat-cell__label">ROCAUC</span>
          <span className="stat-cell__value mono">
            {latest?.running_rocauc != null ? latest.running_rocauc.toFixed(4) : 'N/A'}
          </span>
        </div>
        <div className="stat-cell">
          <span className="stat-cell__label">ANOMALIES</span>
          <span className="stat-cell__value mono" style={{ color: 'var(--red)' }}>
            {anomalyCount}
          </span>
        </div>
        <div className="stat-cell">
          <span className="stat-cell__label">ANOMALY RATE</span>
          <span className="stat-cell__value mono">
            {anomalyRate != null ? `${anomalyRate}%` : '—'}
          </span>
        </div>
        <div className="stat-cell">
          <span className="stat-cell__label">LAST SCORE</span>
          <span className="stat-cell__value mono" style={{ color: 'var(--orange)' }}>
            {latest?.anomaly_score != null ? latest.anomaly_score.toFixed(4) : '—'}
          </span>
        </div>
      </div>
    </div>
  )
}