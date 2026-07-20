interface Props {
  history: any[]
}

function computePercentile(arr: number[], p: number): number | null {
  if (!arr.length) return null
  const sorted = [...arr].sort((a, b) => a - b)
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[idx]
}

export default function LatencyPanel({ history }: Props) {
  const e2eValues = history.map(h => h.e2e_latency_ms).filter((v): v is number => v != null)
  const procValues = history.map(h => h.processing_latency_ms).filter((v): v is number => v != null)

  const e2eP50 = computePercentile(e2eValues, 50)
  const e2eP95 = computePercentile(e2eValues, 95)
  const procP50 = computePercentile(procValues, 50)
  const procP95 = computePercentile(procValues, 95)

  const stats = [
    { label: 'E2E p50',  value: e2eP50  },
    { label: 'E2E p95',  value: e2eP95  },
    { label: 'PROC p50', value: procP50 },
    { label: 'PROC p95', value: procP95 },
  ]

  return (
    <div className="panel">
      <div className="panel__header">
        <span className="panel__title">LATENCY</span>
        <span className="panel__subtitle mono">last {history.length} msgs</span>
      </div>
      <div className="stat-grid">
        {stats.map(s => (
          <div key={s.label} className="stat-cell">
            <span className="stat-cell__label">{s.label}</span>
            <span className="stat-cell__value mono">
              {s.value != null ? `${s.value.toFixed(1)}ms` : '—'}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}