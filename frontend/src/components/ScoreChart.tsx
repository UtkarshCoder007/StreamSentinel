import {
  ComposedChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ReferenceLine, ResponsiveContainer
} from 'recharts'

interface Props {
  history: any[]
  driftEvents: any[]
}

function formatTime(isoStr: string): string {
  if (!isoStr) return ''
  const d = new Date(isoStr)
  return d.toLocaleTimeString('en-GB', { hour12: false })
}

function AnomalyDot(props: any) {
  const { cx, cy, payload } = props
  if (!payload.is_anomaly) return null
  return <circle cx={cx} cy={cy} r={4} fill="#ef4444" stroke="#ef4444" />
}

function CustomTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div className="chart-tooltip mono">
      <div className="chart-tooltip__time">{formatTime(d.event_time)}</div>
      <div>Value: <span style={{ color: 'var(--blue)' }}>{d.value?.toFixed(3)}</span></div>
      <div>Score: <span style={{ color: 'var(--orange)' }}>{d.anomaly_score?.toFixed(4)}</span></div>
      <div>Anomaly: <span style={{ color: d.is_anomaly ? 'var(--red)' : 'var(--green)' }}>{d.is_anomaly ? 'YES' : 'NO'}</span></div>
      <div>E2E: <span>{d.e2e_latency_ms?.toFixed(1)}ms</span></div>
      <div>Gen: <span>{d.model_generation}</span></div>
    </div>
  )
}

export default function ScoreChart({ history, driftEvents }: Props) {
  if (!history.length) return (
    <div className="panel panel--chart">
      <span className="mono muted">No history yet — waiting for consumer...</span>
    </div>
  )

  return (
    <div className="panel panel--chart">
      <div className="panel__header">
        <span className="panel__title">ANOMALY SCORE / VALUE STREAM</span>
      </div>
      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={history} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="event_time"
            tickFormatter={formatTime}
            tick={{ fontFamily: 'JetBrains Mono', fontSize: 11, fill: '#64748b' }}
            interval="preserveStartEnd"
          />
          <YAxis
            yAxisId="left"
            domain={[0, 100]}
            tick={{ fontFamily: 'JetBrains Mono', fontSize: 11, fill: '#64748b' }}
            label={{ value: 'CPU %', angle: -90, position: 'insideLeft', fill: '#64748b', fontSize: 11 }}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            domain={[0, 1]}
            tick={{ fontFamily: 'JetBrains Mono', fontSize: 11, fill: '#64748b' }}
            label={{ value: 'Score', angle: 90, position: 'insideRight', fill: '#64748b', fontSize: 11 }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontFamily: 'JetBrains Mono', fontSize: 12, color: '#64748b' }} />
          {driftEvents.map((evt, i) => (
            <ReferenceLine
              key={i}
              yAxisId="left"
              x={evt.detected_at}
              stroke="#a855f7"
              strokeDasharray="4 2"
              label={{
                value: `Gen ${evt.model_generation_killed}→${evt.model_generation_killed + 1}`,
                fill: '#a855f7',
                fontSize: 10,
                fontFamily: 'JetBrains Mono'
              }}
            />
          ))}
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="value"
            stroke="#3b82f6"
            dot={false}
            strokeWidth={1.5}
            name="CPU value"
            isAnimationActive={false}
          />
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="anomaly_score"
            stroke="#f97316"
            strokeWidth={1.5}
            dot={<AnomalyDot />}
            activeDot={{ r: 5 }}
            name="anomaly score"
            isAnimationActive={false}
          />
          <Line
            yAxisId="right"
            type="monotone"
            dataKey={(d: any) => d.is_anomaly ? d.anomaly_score : null}
            stroke="transparent"
            dot={{ fill: '#ef4444', r: 4, strokeWidth: 0 }}
            activeDot={false}
            legendType="none"
            name="anomaly marker"
            connectNulls={false}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}