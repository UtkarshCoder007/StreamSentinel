interface Props {
  driftEvents: any[]
}

function formatTime(isoStr: string): string {
  if (!isoStr) return ''
  return new Date(isoStr).toLocaleTimeString('en-GB', { hour12: false })
}

export default function DriftTimeline({ driftEvents }: Props) {
  return (
    <div className="panel">
      <div className="panel__header">
        <span className="panel__title">DRIFT EVENTS</span>
        <span className="panel__subtitle mono">{driftEvents.length} total</span>
      </div>
      {driftEvents.length === 0 ? (
        <p className="mono muted">No drift detected yet.</p>
      ) : (
        <div className="drift-list">
          {[...driftEvents].reverse().map((evt, i) => (
            <div key={i} className="drift-item">
              <div className="drift-item__header">
                <span className="drift-item__gen mono">
                  Gen {evt.model_generation_killed} → {evt.model_generation_killed + 1}
                </span>
                <span className="drift-item__time mono">{formatTime(evt.detected_at)}</span>
              </div>
              <div className="drift-item__meta mono">
                trigger score: {evt.trigger_score.toFixed(4)} · window: {evt.adwin_window_size} · msg #{evt.message_count}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}